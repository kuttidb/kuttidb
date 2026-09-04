import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient

PORT = 7405


def wait_port():
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.1).close()
            return
        except OSError:
            time.sleep(0.03)
    raise RuntimeError("exchange server did not start")


def start(queue_wal):
    server = os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb"))
    proc = subprocess.Popen([server, str(PORT), "-", "100",
                             "--queue-wal", queue_wal], stderr=subprocess.DEVNULL,
                            start_new_session=True)
    wait_port()
    return proc


def raw_request(client, op, name, value):
    """Send one framed request on a fresh connection and return the status."""
    key = name.encode()
    payload = struct.pack("<BHI", op, len(key), len(value)) + key + value
    with socket.create_connection(("127.0.0.1", PORT), 2) as s:
        s.settimeout(2)
        s.sendall(payload)
        header = b""
        while len(header) < 5:
            chunk = s.recv(5 - len(header))
            if not chunk:
                raise AssertionError("server closed connection")
            header += chunk
        status, vlen = struct.unpack("<BI", header)
        if vlen:
            s.recv(vlen)
        return status


tmp = tempfile.mkdtemp(prefix="kuttidb-exchange-proto-")
wal = os.path.join(tmp, "queues.wal")
proc = None
try:
    proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        # -- default exchange ------------------------------------------------
        client.queue_declare("jobs", durable=True, max_depth=10)
        assert client.exchange_publish("", "jobs", b"direct-hit") == 1
        delivery = client.queue_consume("jobs")
        assert delivery["value"] == b"direct-hit"
        assert client.queue_ack("jobs", delivery["id"])
        assert client.exchange_publish("", "missing-queue", b"x") == 0

        # -- direct exchange -------------------------------------------------
        client.queue_declare("blue.q", durable=True)
        client.queue_declare("red.q", durable=True)
        client.exchange_declare("paint", type="direct", durable=True)
        client.exchange_bind("paint", "blue.q", "blue")
        client.exchange_bind("paint", "red.q", "red")
        assert client.exchange_publish("paint", "blue", b"b1") == 1
        assert client.exchange_publish("paint", "red", b"r1") == 1
        assert client.exchange_publish("paint", "green", b"nope") == 0
        delivery = client.queue_consume("blue.q")
        assert delivery["value"] == b"b1"
        assert client.queue_ack("blue.q", delivery["id"])
        delivery = client.queue_consume("red.q")
        assert delivery["value"] == b"r1"
        assert client.queue_ack("red.q", delivery["id"])
        # redeclare with different parameters is refused
        refused = False
        try:
            client.exchange_declare("paint", type="topic", durable=True)
        except Exception:
            refused = True
        assert refused, "exchange redeclare mismatch was accepted"
        client.exchange_declare("paint", type="direct", durable=True)
        # bindings must reference declared queues
        refused = False
        try:
            client.exchange_bind("paint", "ghost.q", "blue")
        except Exception:
            refused = True
        assert refused, "binding to a missing queue was accepted"
        # unbind removes exactly one route
        assert client.exchange_unbind("paint", "red.q", "red")
        assert client.exchange_publish("paint", "red", b"r2") == 0
        assert not client.exchange_unbind("paint", "red.q", "red")
        client.exchange_bind("paint", "red.q", "red")

        # -- fanout exchange -------------------------------------------------
        client.queue_declare("f1", durable=True)
        client.queue_declare("f2", durable=True)
        client.exchange_declare("broadcast", type="fanout", durable=True)
        client.exchange_bind("broadcast", "f1", "ignored")
        client.exchange_bind("broadcast", "f2", "ignored")
        client.exchange_bind("broadcast", "f1", "also-ignored")
        assert client.exchange_publish("broadcast", "anything", b"fan") == 2
        for queue in ("f1", "f2"):
            delivery = client.queue_consume(queue)
            assert delivery["value"] == b"fan"
            assert client.queue_ack(queue, delivery["id"])
            assert client.queue_consume(queue) is None  # deduped per queue

        # -- topic exchange --------------------------------------------------
        client.queue_declare("orders.all", durable=True)
        client.queue_declare("orders.eu", durable=True)
        client.exchange_declare("events", type="topic", durable=True)
        client.exchange_bind("events", "orders.all", "orders.#")
        client.exchange_bind("events", "orders.eu", "orders.eu.*")
        assert client.exchange_publish("events", "orders.eu.paid", b"e1") == 2
        assert client.exchange_publish("events", "orders.us.paid", b"e2") == 1
        assert client.exchange_publish("events", "payments.eu", b"e3") == 0
        delivery = client.queue_consume("orders.all")
        assert delivery["value"] == b"e1"
        assert client.queue_ack("orders.all", delivery["id"])
        delivery = client.queue_consume("orders.eu")
        assert delivery["value"] == b"e1"
        assert client.queue_ack("orders.eu", delivery["id"])
        delivery = client.queue_consume("orders.all")
        assert delivery["value"] == b"e2"
        assert client.queue_ack("orders.all", delivery["id"])
        assert client.queue_consume("orders.eu") is None

        # -- alternate exchange ----------------------------------------------
        client.queue_declare("catch", durable=True)
        client.exchange_declare("catch.x", type="fanout", durable=True)
        client.exchange_bind("catch.x", "catch", "")
        client.exchange_declare("strict", type="direct", durable=True,
                                alternate_exchange="catch.x")
        assert client.exchange_publish("strict", "unroutable", b"saved") == 1
        delivery = client.queue_consume("catch")
        assert delivery["value"] == b"saved"
        assert client.queue_ack("catch", delivery["id"])

        # -- durability bookkeeping -------------------------------------------
        stats = client.stats()
        assert stats["exchanges"] == 5          # paint, broadcast, events,
        assert stats["exchange_bindings"] == 8  # catch.x and strict
        assert stats["exchange_unroutable"] == 4

        # -- raw protocol validation ------------------------------------------
        assert raw_request(client, 0x30, "bad", b"\x01\x07") == 2          # short declare
        assert raw_request(client, 0x30, "bad", b"\x01\x09") == 2          # bad type
        assert raw_request(client, 0x30, "bad", b"\x01\x00") == 0          # ok, no ext
        assert raw_request(client, 0x31, "bad", b"\x00\x02") == 2          # short bind
        assert raw_request(client, 0x31, "bad",
                           struct.pack("<H", 3) + b"jobs") == 2            # missing key
        assert raw_request(client, 0x33, "nosuch",
                           struct.pack("<HQ", 1, 0) + b"k") == 2           # unknown exchange
        assert raw_request(client, 0x33, "paint",
                           struct.pack("<HQ", 1, 0) + b"k") == 1           # unroutable
        assert raw_request(client, 0x33, "",
                           struct.pack("<HQ", 300, 0) + b"k" * 300) == 2   # oversize key

        # -- restart recovery ---------------------------------------------------
        client.queue_publish("blue.q", b"pre-restart")
        pending = client.queue_consume("blue.q")
        assert pending["value"] == b"pre-restart"
    proc.kill()
    proc.wait()
    proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        # durable exchanges, bindings, and the delivered message survive
        assert client.exchange_publish("events", "orders.eu.paid",
                                       b"post-restart") == 2
        delivery = client.queue_consume("orders.all")
        assert delivery["value"] == b"post-restart"
        assert client.queue_ack("orders.all", delivery["id"])
        delivery = client.queue_consume("orders.eu")
        assert delivery["value"] == b"post-restart"
        assert client.queue_ack("orders.eu", delivery["id"])
        # the unacked pre-restart delivery is redelivered (at-least-once)
        delivery = client.queue_consume("blue.q")
        assert delivery["value"] == b"pre-restart" and delivery["redelivered"]
        assert client.queue_ack("blue.q", delivery["id"])
        # durable routing state survived the restart
        assert client.exchange_publish("paint", "blue", b"still-routed") == 1
        delivery = client.queue_consume("blue.q")
        assert delivery["value"] == b"still-routed"
        assert client.queue_ack("blue.q", delivery["id"])
        stats = client.stats()
        assert stats["exchanges"] == 6
        assert stats["exchange_bindings"] == 8
    print("EXCHANGE PROTOCOL + RECOVERY TESTS PASSED")
finally:
    if proc:
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass
    shutil.rmtree(tmp, ignore_errors=True)
