import os
import socket as sockmod
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kuttidb_client import KuttiDBClient


def wait_for_port(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sockmod.create_connection(("127.0.0.1", port), 0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def start_server(wal, budget_mb=None):
    args = ["./kuttidb", "7396", wal, "50"]
    if budget_mb:
        args.append("-")
        args.append(str(budget_mb))
    p = subprocess.Popen(args, stderr=subprocess.DEVNULL, start_new_session=True)
    wait_for_port(7396)
    return p


tmp = tempfile.mkdtemp(prefix="ttltest-")
wal = os.path.join(tmp, "kuttidb.wal")
p = None
try:
    # ---- TTL basics ----
    p = start_server(wal)
    with KuttiDBClient(port=7396, timeout=10) as c:
        c.put("ephemeral", b"soon gone", ttl=1.0)
        c.put("durable", b"stays")
        assert c.get("ephemeral") == b"soon gone"
        time.sleep(1.4)
        assert c.get("ephemeral") is None, "TTL key should expire"
        assert c.get("durable") == b"stays"
        st = c.stats()
        assert st["expired"] >= 1, st

        # ttl across restart (wal)
        c.put("ttl-persist", b"persisted-ttl", ttl=30)
    p.terminate(); p.wait()

    p = start_server(wal)
    with KuttiDBClient(port=7396, timeout=10) as c:
        assert c.get("ttl-persist") == b"persisted-ttl", "ttl key lost in restart"
        assert c.get("durable") == b"stays"

        # batched with per-item ttl
        c.put_many([(f"b{i}", b"x" * 10) for i in range(100)])
        c.put_many([(f"bt{i}", b"y" * 10, 30.0) for i in range(100)])
        assert c.get("bt7") == b"y" * 10

        # eviction: budget 4 MB, push ~20 MB of records
        c.put_many((f"evict-fill-{i}", os.urandom(200)) for i in range(100000))
    p.terminate(); p.wait()

    # snapshot made on clean shutdown; reload and verify TTL keys survive
    p = start_server(wal)
    with KuttiDBClient(port=7396, timeout=30) as c:
        assert c.get("ttl-persist") == b"persisted-ttl", "ttl lost in snapshot"
        assert c.get("bt7") == b"y" * 10
        st = c.stats()
        # expired entries from old snapshot must not resurrect
    p.terminate(); p.wait()

    # eviction under budget (fresh db, budget 4MB)
    p = start_server(os.path.join(tmp, "evict.wal"), budget_mb=4)
    with KuttiDBClient(port=7396, timeout=60) as c:
        c.put_many((f"fill-{i}", os.urandom(200)) for i in range(50000))
        st = c.stats()
        assert st["mem_bytes"] < 8 * 1024 * 1024, f"budget ignored: {st['mem_bytes']}"
        assert st["evicted"] > 0, "nothing evicted"
        print(f"eviction: {st['count']} records, {st['mem_bytes']/1e6:.1f} MB, "
              f"{st['evicted']} evicted, budget 4MB")
    p.terminate(); p.wait()

    print("TTL + EVICTION TESTS PASSED")
finally:
    if p:
        try:
            p.kill()
        except Exception:
            pass
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
