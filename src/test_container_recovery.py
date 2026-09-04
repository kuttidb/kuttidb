"""Container persistent-volume recovery check.

Requires Docker. Builds nothing: run against an already-built runtime image
(default kuttidb:ci). The flow mirrors the Kubernetes StatefulSet contract:

  1. start a container with a fresh volume, write durable cache records and
     durable queue messages through the exposed port;
  2. SIGKILL the container (no graceful shutdown);
  3. start a new container on the same volume;
  4. verify the acknowledged durable state is fully present.

Exit 0 = pass."""

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient

IMAGE = os.environ.get("KUTTIDB_TEST_IMAGE", "kuttidb:local-test")
NAMES = {"vol": f"kuttidb-recovery-{uuid.uuid4().hex[:12]}",
         "c1": f"kuttidb-recovery-a-{uuid.uuid4().hex[:8]}",
         "c2": f"kuttidb-recovery-b-{uuid.uuid4().hex[:8]}"}
PORT = 7495
TOKEN = b"container-recovery-token"


def sh(*args, check=True):
    return subprocess.run(args, capture_output=True, text=True, timeout=120,
                          check=check)


def wait_port(name=None):
    until = time.time() + 15
    while time.time() < until:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.1).close()
            return
        except OSError:
            time.sleep(0.05)
    if name:
        diag = subprocess.run(["docker", "logs", name], capture_output=True,
                              text=True, timeout=30)
        raise RuntimeError(
            f"container server did not start; logs: {diag.stdout} {diag.stderr}")
    raise RuntimeError("container server did not start")


def start(name, token_path, delete_old=True):
    if delete_old:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True,
                       timeout=60)
    return subprocess.run(
        ["docker", "run", "-d", "--name", name, "-p",
         f"127.0.0.1:{PORT}:7379", "-v", f"{NAMES['vol']}:/var/lib/kuttidb",
         "-v", f"{token_path}:/etc/kuttidb-auth/token:ro",
         "-e", "KUTTIDB_AUTH_SOURCE=/etc/kuttidb-auth/token",
         "-e", "KUTTIDB_METRICS_TOKEN_SOURCE=/etc/kuttidb-auth/token",
         IMAGE, "7379", "/var/lib/kuttidb/kuttidb.wal", "100", "-", "64",
         "--bind", "0.0.0.0", "--auth-file", "/var/lib/kuttidb/auth.token",
         "--durability", "always", "--queue-wal",
         "/var/lib/kuttidb/queue.wal", "--stream-wal",
         "/var/lib/kuttidb/stream.wal", "--metrics-bind", "0.0.0.0:9099",
         "--metrics-token-file", "/var/lib/kuttidb/metrics.token"],
        capture_output=True, text=True, timeout=60, check=True)


def main():
    logs = []
    tmp = tempfile.mkdtemp(prefix="kuttidb-pv-")
    token_path = os.path.join(tmp, "token")
    with open(token_path, "wb") as f:
        f.write(TOKEN)
    os.chmod(token_path, 0o644)  # bind-mounted; the entrypoint makes a private copy
    try:
        sh("docker", "volume", "create", NAMES["vol"])
        sh("docker", "rm", "-f", NAMES["c1"], check=False)
        start(NAMES["c1"], token_path, delete_old=False)
        wait_port(NAMES["c1"])

        with KuttiDBClient(port=PORT, auth_token=TOKEN) as c:
            for i in range(500):
                c.put(f"pv:{i}", f"value-{i}".encode())
            c.queue_declare("pv.jobs", durable=True)
            ids = [c.queue_publish("pv.jobs", f"job-{i}".encode())
                   for i in range(50)]
            c.stream_declare("pv.events", partitions=2)
            for i in range(20):
                c.stream_append("pv.events", f"event-{i}".encode(), partition=i % 2)
            stats = c.stats()
        logs.append(f"seeded: cache=500 queue={len(ids)} "
                    f"stream_offsets={stats['stream_partitions']}")

        # Hard kill: no graceful flush, straight to the recovery path.
        subprocess.run(["docker", "kill", "-s", "SIGKILL", NAMES["c1"]],
                       capture_output=True, timeout=30, check=True)
        subprocess.run(["docker", "rm", "-f", NAMES["c1"]], capture_output=True,
                       timeout=30, check=True)
        time.sleep(0.3)

        start(NAMES["c2"], token_path, delete_old=False)
        wait_port(NAMES["c2"])
        with KuttiDBClient(port=PORT, auth_token=TOKEN) as c:
            assert c.health() is True, "container not ready after recovery"
            values = c.get_many([f"pv:{i}" for i in range(500)])
            assert values == [f"value-{i}".encode() for i in range(500)], \
                "durable cache records lost across container SIGKILL"
            got = []
            while True:
                msg = c.queue_consume("pv.jobs", visibility=5.0)
                if not msg:
                    break
                got.append(msg["id"])
                c.queue_ack("pv.jobs", msg["id"])
            assert len(got) == 50, f"expected 50 durable messages, got {len(got)}"
            records = [x for p in (0, 1)
                       for x in c.stream_fetch("pv.events", partition=p)]
            assert len(records) == 20, "stream records lost across container SIGKILL"
            assert all(b"event-" in r["value"] for r in records)
        logs.append("recovery verified: cache, queue, and stream state survived "
                    "a container SIGKILL restart on the same volume")
        print("\n".join(logs))
        print("CONTAINER PV RECOVERY TESTS PASSED")
    finally:
        for name in (NAMES["c1"], NAMES["c2"]):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True,
                           timeout=30)
        subprocess.run(["docker", "volume", "rm", NAMES["vol"]],
                       capture_output=True, timeout=30)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
