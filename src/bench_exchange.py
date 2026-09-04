"""Exchange routing benchmark.

Measures end-to-end publish throughput through the exchange layer versus a
plain queue publish, plus topic-match cost at several binding counts, using
the native Python client. Durability is `--durability always`-equivalent per
queue publish (each durable copy is fsynced before confirmation), so these
numbers reflect the durable path.

Usage: python3 src/bench_exchange.py [PORT] [OPS]
"""

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7406
OPS = int(sys.argv[2]) if len(sys.argv) > 2 else 5000


def wait_port():
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.1).close()
            return
        except OSError:
            time.sleep(0.03)
    raise RuntimeError("benchmark server did not start")


def rate(label, fn, ops):
    start = time.perf_counter()
    for i in range(ops):
        fn(i)
    elapsed = time.perf_counter() - start
    print(f"{label:<44} {ops / elapsed:>12,.0f} ops/s")
    return ops / elapsed


tmp = tempfile.mkdtemp(prefix="kuttidb-exchange-bench-")
proc = None
try:
    server = os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb"))
    proc = subprocess.Popen([server, str(PORT), os.path.join(tmp, "kuttidb.wal"),
                             "100", "--queue-wal", os.path.join(tmp, "q.wal")],
                            stderr=subprocess.DEVNULL, start_new_session=True)
    wait_port()
    with KuttiDBClient(port=PORT) as db:
        db.queue_declare("plain", durable=True)
        rate("queue publish (durable, baseline)",
             lambda i: db.queue_publish("plain", b"x" * 100), OPS)

        db.exchange_declare("direct", type="direct", durable=True)
        db.queue_declare("dq", durable=True)
        db.exchange_bind("direct", "dq", "key")
        rate("direct exchange, 1 durable binding",
             lambda i: db.exchange_publish("direct", "key", b"x" * 100), OPS)

        db.exchange_declare("fan", type="fanout", durable=True)
        for n in range(8):
            db.queue_declare(f"fan{n}", durable=True)
            db.exchange_bind("fan", f"fan{n}", "")
        rate("fanout exchange, 8 durable bindings",
             lambda i: db.exchange_publish("fan", "", b"x" * 100), max(OPS // 8, 200))

        db.exchange_declare("topic", type="topic", durable=True)
        db.queue_declare("tq", durable=True)
        for n in range(100):
            db.exchange_bind("topic", "tq", f"a.{n:03d}.*")
        rate("topic exchange, 100 bindings, 1 match",
             lambda i: db.exchange_publish("topic", "a.042.match", b"x" * 100),
             max(OPS // 4, 500))

        rate("topic exchange, 100 bindings, no match (unroutable)",
             lambda i: db.exchange_publish("topic", "zz.nomatch", b"x" * 100),
             max(OPS // 4, 500))
finally:
    if proc:
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass
    shutil.rmtree(tmp, ignore_errors=True)
