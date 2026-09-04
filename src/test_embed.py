import os
import shutil
import socket as sockmod
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "clients"))
from kuttidb_client import KuttiDBClient
from kuttidb_embed import KuttiEmbed


def wait_for_port(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sockmod.create_connection(("127.0.0.1", port), 0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


tmp = tempfile.mkdtemp(prefix="embedtest-")
embed = os.path.join(tmp, "db.embed")
wal = os.path.join(tmp, "kuttidb.wal")
p = None
try:
    # server creates the region and serves network clients from it
    p = subprocess.Popen(
        ["./kuttidb", "7397", wal, "50", "", "0", embed],
        stderr=subprocess.DEVNULL, start_new_session=True)
    wait_for_port(7397)

    db = KuttiEmbed(embed)

    # embed write -> network read
    db.put("from-embed", b"via shared memory")
    with KuttiDBClient(port=7397) as c:
        assert c.get("from-embed") == b"via shared memory"

        # network write -> embed read
        c.put("from-net", b"via tcp")
    assert db.get("from-net") == b"via tcp"

    # overwrite from embed, read from network
    db.put("from-embed", b"updated")
    with KuttiDBClient(port=7397) as c:
        assert c.get("from-embed") == b"updated"

    # ttl via embed
    db.put("brief", b"gone soon", ttl=1.0)
    assert db.get("brief") == b"gone soon"
    time.sleep(1.3)
    assert db.get("brief") is None, "embed ttl should expire"

    # bulk through the embed path (memory speed)
    t = time.perf_counter()
    v = b"z" * 100
    for i in range(100000):
        db.put(f"bulk{i}", v)
    dt = time.perf_counter() - t
    ops = 100000 / dt
    print(f"embed single puts: {ops:.0f} ops/s (ctypes overhead included)")

    t = time.perf_counter()
    for i in range(100000):
        assert db.get(f"bulk{i}") == v
    dt = time.perf_counter() - t
    print(f"embed single gets: {100000/dt:.0f} ops/s")

    # delete via embed visible on network
    assert db.delete("bulk0")
    with KuttiDBClient(port=7397) as c:
        assert c.get("bulk0") is None
    print(f"count={db.count()} mem={db.memusage()}")

    # kill -9 the server: the REGION and its data survive in the file
    os.kill(p.pid, 9)
    p.wait()

    # embed client can still read (server crash must not corrupt shared state)
    assert db.get("from-embed") == b"updated"
    db.close()

    # restart: server re-attaches the region; data is right there
    p = subprocess.Popen(
        ["./kuttidb", "7397", wal, "50", "", "0", embed],
        stderr=subprocess.DEVNULL, start_new_session=True)
    wait_for_port(7397)
    with KuttiDBClient(port=7397) as c:
        assert c.get("from-embed") == b"updated", "region data lost across restart"
        assert c.get("bulk5") == v

    # WAL recovery: destroy region, server replays WAL (embed writes were logged)
    p.terminate(); p.wait()
    os.remove(embed)
    p = subprocess.Popen(
        ["./kuttidb", "7397", wal, "50", "", "0", embed],
        stderr=subprocess.DEVNULL, start_new_session=True)
    wait_for_port(7397)
    with KuttiDBClient(port=7397) as c:
        assert c.get("from-embed") == b"updated", "embed WAL write not recovered"
        assert c.get("from-net") == b"via tcp"

    print("EMBEDDED MODE TESTS PASSED")
finally:
    if p:
        try:
            p.kill()
        except Exception:
            pass
    shutil.rmtree(tmp, ignore_errors=True)
