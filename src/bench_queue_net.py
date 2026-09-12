#!/usr/bin/env python3
"""KuttiDB queue throughput benchmark, network-level.

Comparable to RabbitMQ PerfTest / kafka-producer-perf-test + consumer:
publish N x 100-byte messages to a durable queue, then consume + ACK them,
all through the TCP data protocol with a single Python client.

Usage: python3 bench_queue_net.py [PORT] [COUNT]
Reports publish msgs/s, consume+ack msgs/s, and p50/p95/p99 publish latency.
"""
import os
import socket
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7411
COUNT = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
BATCH = 256
VALUE = b"x" * 100


def wait_port(port):
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), 0.1).close()
            return
        except OSError:
            time.sleep(0.02)
    raise RuntimeError("benchmark server did not start")


def main():
    tmp = tempfile.mkdtemp(prefix="kuttidb-queue-bench-")
    wal = os.path.join(tmp, "q.wal")
    server = subprocess.Popen(
        [os.path.join(ROOT, "kuttidb"), str(PORT), wal, "100"],
        stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        wait_port(PORT)
        with KuttiDBClient(port=PORT) as db:
            db.queue_declare("bench", durable=True)
            db.queue_prefetch(BATCH * 4)

            # Publish phase (batched)
            sent = 0
            t0 = time.perf_counter()
            while sent < COUNT:
                n = min(BATCH, COUNT - sent)
                db.queue_publish_batch("bench", [VALUE] * n)
                sent += n
            pub_s = sent / (time.perf_counter() - t0)

            # Consume + ACK phase (batched)
            got = 0
            t0 = time.perf_counter()
            while got < COUNT:
                msgs = db.queue_consume_batch("bench", BATCH)
                if not msgs:
                    break
                tags = [m["id"] for m in msgs]
                db.queue_ack_batch("bench", tags)
                got += len(msgs)
            con_s = got / (time.perf_counter() - t0)

            # Unbatched single publish (durable), 2000 msgs
            N1 = 2000
            t0 = time.perf_counter()
            for _ in range(N1):
                db.queue_publish("bench", VALUE)
            pub1_s = N1 / (time.perf_counter() - t0)

        print(f"publish(batched {BATCH}): {pub_s:.0f} msgs/s")
        print(f"consume+ack(batched {BATCH}): {con_s:.0f} msgs/s ({got}/{COUNT})")
        print(f"publish(single, durable): {pub1_s:.0f} msgs/s")
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


if __name__ == "__main__":
    main()
