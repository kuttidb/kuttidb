import os
import shutil
import signal
import socket as sockmod
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kuttidb_client import KuttiDBClient

PORT = 7393
tmp = tempfile.mkdtemp(prefix="cachetest-")
wal = os.path.join(tmp, "kuttidb.wal")


def wait_for_port(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sockmod.create_connection(("127.0.0.1", port), 0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def start_server():
    p = subprocess.Popen(
        ["./kuttidb", str(PORT), wal, "50"],
        stderr=subprocess.PIPE, start_new_session=True)
    wait_for_port(PORT)
    return p


try:
    # phase 1: write data, crash hard
    p = start_server()
    with KuttiDBClient(port=PORT, timeout=10) as c:
        c.put_many((f"key{i}", os.urandom(200)) for i in range(5000))
        c.put("to-delete", b"gone")
        c.delete("to-delete")
    os.kill(p.pid, signal.SIGKILL)
    p.wait()

    # phase 2: recover from wal
    p = start_server()
    with KuttiDBClient(port=PORT, timeout=30) as c:
        vals = c.get_many([f"key{i}" for i in range(5000)])
        assert all(v is not None for v in vals), "lost keys after crash"
        assert all(len(v) == 200 for v in vals)
        assert c.get("to-delete") is None, "delete not persisted"
        st = c.stats()
        assert st["count"] == 5000

        # phase 3: snapshot + wal compaction, restart, verify again
        c.put("after", b"snapshot-test")
    p.send_signal(signal.SIGUSR1) if False else None
    # force a snapshot by writing enough wal? trigger via kill -TERM then check
    p.send_signal(signal.SIGTERM)
    p.wait(timeout=10)
    assert os.path.exists(wal + ".snap"), "snapshot not created"

    p = start_server()
    with KuttiDBClient(port=PORT, timeout=30) as c:
        vals = c.get_many([f"key{i}" for i in range(5000)])
        assert all(v is not None for v in vals), "lost keys after snapshot"
        assert c.get("after") == b"snapshot-test"
        st = c.stats()
        assert st["count"] == 5001

    per_rec = st["mem_bytes"] / st["count"]
    print(f"recovery verified: 5001 records, ~{per_rec:.0f} bytes/record in memory")
    print("PERSISTENCE TESTS PASSED")
finally:
    shutil.rmtree(tmp, ignore_errors=True)
    try:
        p.kill()
    except Exception:
        pass