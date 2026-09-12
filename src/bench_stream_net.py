#!/usr/bin/env python3
"""KuttiDB stream throughput benchmark, network-level.

Comparable to NATS JetStream pub/consume and Kafka producer/consumer perf
tests: append N x 100-byte records to a durable partitioned stream, then
fetch them back, all through the TCP data protocol with one Python client.

Usage: python3 bench_stream_net.py [PORT] [COUNT] [PARTITIONS]
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

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7412
COUNT = int(sys.argv[2]) if len(sys.argv) > 2 else 40000
PARTS = int(sys.argv[3]) if len(sys.argv) > 3 else 8
BATCH = 256
VALUE = b"y" * 100


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
    tmp = tempfile.mkdtemp(prefix="kuttidb-stream-bench-")
    wal = os.path.join(tmp, "s.wal")
    server = subprocess.Popen(
        [os.path.join(ROOT, "kuttidb"), str(PORT), wal, "100"],
        stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        wait_port(PORT)
        with KuttiDBClient(port=PORT) as db:
            db.stream_declare("bench", partitions=PARTS)
            # Append phase (batched, records spread round-robin via key hash)
            items = [(str(i).encode(), VALUE) for i in range(BATCH)]
            sent = 0
            t0 = time.perf_counter()
            while sent < COUNT:
                n = min(BATCH, COUNT - sent)
                db.stream_append_many("bench", items[:n])
                sent += n
            app_s = sent / (time.perf_counter() - t0)

            # Fetch phase: drain every partition from offset 0
            got = 0
            t0 = time.perf_counter()
            for p in range(PARTS):
                offset = 0
                while True:
                    records = db.stream_fetch("bench", partition=p,
                                              offset=offset, max_records=BATCH)
                    if not records:
                        break
                    got += len(records)
                    offset += len(records)
            fetch_s = got / (time.perf_counter() - t0) if got else 0

            # Single appends (durable), 2000 records
            N1 = 2000
            t0 = time.perf_counter()
            for i in range(N1):
                db.stream_append("bench", VALUE, key=str(i).encode())
            app1_s = N1 / (time.perf_counter() - t0)

        print(f"append(batched {BATCH}, {PARTS} partitions): {app_s:.0f} recs/s")
        print(f"fetch(batched {BATCH}): {fetch_s:.0f} recs/s ({got}/{COUNT})")
        print(f"append(single, durable): {app1_s:.0f} recs/s")
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


if __name__ == "__main__":
    main()
