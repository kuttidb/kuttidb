import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "clients"))
from local_client import LocalKuttiDB


def wait_port(port):
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), 0.1).close()
            return
        except OSError:
            time.sleep(0.03)
    raise RuntimeError("server did not start")


tmp = tempfile.mkdtemp(prefix="kuttidb-local-")
embed = os.path.join(tmp, "db.embed")
unix_path = os.path.join(tmp, "db.sock")
proc = None
try:
    proc = subprocess.Popen([os.path.join(ROOT, "kuttidb"), "7403", "-", "100",
                             unix_path, "0", embed], stderr=subprocess.DEVNULL,
                            start_new_session=True)
    wait_port(7403)
    with LocalKuttiDB(embed_path=os.path.join(tmp, "missing.embed"), port=7403,
                    unix_path=unix_path) as db:
        assert db.transport == "unix_socket"
        db.put("fallback", b"network")
        assert db.get("fallback") == b"network"
        assert db.stats()["transport"] == "unix_socket"
    with LocalKuttiDB(embed_path=embed, port=7403) as db:
        assert db.transport == "shared_memory"
        assert db.get("fallback") == b"network"
        db.put("direct", b"memory")
        assert db.get("direct") == b"memory"
        assert db.stats()["transport"] == "shared_memory"
    print("LOCAL TRANSPORT TESTS PASSED")
finally:
    if proc:
        proc.kill()
        proc.wait()
    shutil.rmtree(tmp, ignore_errors=True)
