import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient

PORT = 7404
QUEUE_NACK_OP = 0x24


def wait_port():
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.1).close()
            return
        except OSError:
            time.sleep(0.03)
    raise RuntimeError("queue server did not start")


def raw_request(sock, op, key, value):
    """Send one framed request and read the [status:1][vlen:4][payload]
    response without the SDK, so malformed shapes stay reachable."""
    frame = (bytes([op]) + struct.pack("<H", len(key)) +
             struct.pack("<I", len(value)) + key + value)
    sock.sendall(frame)
    head = b""
    while len(head) < 5:
        chunk = sock.recv(5 - len(head))
        if not chunk:
            raise RuntimeError("connection closed")
        head += chunk
    payload = b""
    need = struct.unpack("<I", head[1:5])[0]
    while len(payload) < need:
        chunk = sock.recv(need - len(payload))
        if not chunk:
            raise RuntimeError("connection closed")
        payload += chunk
    return head[0], payload


def start(queue_wal):
    server = os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb"))
    proc = subprocess.Popen([server, str(PORT), "-", "100",
                             "--queue-wal", queue_wal], stderr=subprocess.DEVNULL,
                            start_new_session=True)
    wait_port()
    return proc


tmp = tempfile.mkdtemp(prefix="kuttidb-queue-proto-")
wal = os.path.join(tmp, "queues.wal")
proc = None
try:
    proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        client.queue_declare("jobs", durable=True, max_depth=2)
        client.queue_prefetch(1)
        assert client.queue_stats("missing") is None
        assert client.queue_stats("jobs") == {"depth": 0, "inflight": 0}
        assert client.queue_list() == [{"name": "jobs", "depth": 0, "inflight": 0}]
        first = client.queue_publish("jobs", b"first")
        client.queue_publish("jobs", b"second")
        assert client.queue_stats("jobs") == {"depth": 2, "inflight": 0}
        rejected = False
        try:
            client.queue_publish("jobs", b"overflow")
        except Exception:
            rejected = True
        assert rejected, "queue max_depth did not backpressure"
        delivery = client.queue_consume("jobs")
        assert (delivery["message_id"] == first and not delivery["redelivered"] and
                delivery["value"] == b"first")
        assert client.queue_stats("jobs") == {"depth": 2, "inflight": 1}
        assert client.queue_list() == [{"name": "jobs", "depth": 2, "inflight": 1}]
        assert client.queue_consume("jobs") is None
        assert client.queue_nack("jobs", delivery["id"], requeue=True)
        client.queue_prefetch(0)
        delivery = client.queue_consume("jobs")
        assert delivery["message_id"] == first and delivery["redelivered"]
        assert client.queue_ack("jobs", delivery["id"])
        stranded = KuttiDBClient(port=PORT)
        pending = stranded.queue_consume("jobs", visibility=3600)
        assert pending["value"] == b"second" and not pending["redelivered"]
        stranded.queue_cancel()
        stranded.close()
        deadline = time.time() + 2
        while True:
            pending = client.queue_consume("jobs")
            if pending is not None:
                break
            if time.time() >= deadline:
                raise AssertionError("consumer cancellation did not requeue delivery")
            time.sleep(0.01)
        assert pending["message_id"] != first and pending["redelivered"]
        assert client.queue_ack("jobs", pending["id"])
        client.queue_declare("expiry", durable=True, max_depth=1)
        client.queue_publish("expiry", b"old", ttl=0.01)
        time.sleep(0.03)
        client.queue_publish("expiry", b"new")
        pending = client.queue_consume("expiry")
        assert pending["value"] == b"new"
        assert client.queue_ack("expiry", pending["id"])
        client.queue_declare("retry", durable=True)
        client.queue_publish("retry", b"later")
        pending = client.queue_consume("retry")
        assert pending["delivery_count"] == 1
        assert client.queue_nack("retry", pending["id"], delay=0.05)
        assert client.queue_consume("retry") is None
        time.sleep(0.07)
        pending = client.queue_consume("retry")
        assert pending["value"] == b"later" and pending["redelivered"]
        assert pending["delivery_count"] == 2
        assert client.queue_ack("retry", pending["id"])
        # -- dead-letter queues ------------------------------------------------
        client.queue_declare("dlqsrc", durable=True,
                             dead_letter_queue="dlq", max_deliveries=2)
        client.queue_publish("dlqsrc", b"poison")
        delivery = client.queue_consume("dlqsrc")
        assert delivery["delivery_count"] == 1
        assert client.queue_nack("dlqsrc", delivery["id"], requeue=True)
        delivery = client.queue_consume("dlqsrc")
        assert delivery["delivery_count"] == 2
        assert client.queue_nack("dlqsrc", delivery["id"], requeue=True)
        # The delivery limit is exhausted: the next consume dead-letters it.
        assert client.queue_consume("dlqsrc") is None
        assert client.queue_stats("dlqsrc") == {"depth": 0, "inflight": 0}
        assert client.queue_stats("dlq") == {"depth": 1, "inflight": 0}
        # Rejected messages are routed too.
        client.queue_publish("dlqsrc", b"reject")
        delivery = client.queue_consume("dlqsrc")
        assert delivery["value"] == b"reject"
        assert client.queue_nack("dlqsrc", delivery["id"], requeue=False)
        assert client.queue_stats("dlq") == {"depth": 2, "inflight": 0}
        # Expired messages are routed and lose their expiry in the DLQ.
        client.queue_publish("dlqsrc", b"stale", ttl=0.01)
        time.sleep(0.03)
        assert client.queue_consume("dlqsrc") is None
        assert client.queue_stats("dlq") == {"depth": 3, "inflight": 0}
        dead = client.queue_consume("dlq")
        assert dead["value"] == b"poison" and not dead["redelivered"]
        assert dead["delivery_count"] == 1
        assert client.queue_ack("dlq", dead["id"])
        # A full DLQ must fail closed: the rejection is refused.
        client.queue_declare("tiny", durable=True, max_depth=1)
        client.queue_publish("tiny", b"fill")
        client.queue_declare("fullsrc", durable=True, dead_letter_queue="tiny")
        client.queue_publish("fullsrc", b"keepme")
        delivery = client.queue_consume("fullsrc")
        rejected = False
        try:
            client.queue_nack("fullsrc", delivery["id"], requeue=False)
        except Exception:
            rejected = True
        assert rejected, "full DLQ did not fail closed"
        assert client.queue_stats("fullsrc") == {"depth": 1, "inflight": 1}
        assert client.queue_ack("fullsrc", delivery["id"])
        stats = client.stats()
        assert stats.get("queue_deadlettered") == 3
        client.queue_publish("jobs", b"restart")
        pending = client.queue_consume("jobs")
        assert pending["value"] == b"restart"
    proc.kill()
    proc.wait()
    proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        recovered = client.queue_consume("jobs")
        assert recovered["value"] == b"restart" and recovered["redelivered"]
        assert client.queue_ack("jobs", recovered["id"])
        assert client.queue_consume("jobs") is None
        # Dead-letter routing state survives the restart.
        assert client.queue_stats("dlq") == {"depth": 2, "inflight": 0}
        assert client.queue_stats("dlqsrc") == {"depth": 0, "inflight": 0}
        dead = client.queue_consume("dlq")
        assert dead["value"] == b"reject"
        assert client.queue_ack("dlq", dead["id"])

    # --- durable named consumers ----------------------------------------
    with KuttiDBClient(port=PORT) as client:
        client.queue_declare("consumers", durable=True)
        for i in range(2):
            client.queue_publish("consumers", f"item-{i}".encode())
        owner = client.queue_consumer_register("worker-a")
        assert owner > 0
        # Re-registering (heartbeat) keeps the same owner token.
        assert client.queue_consumer_register("worker-a") == owner
        delivery = client.queue_consume_as("consumers", "worker-a", visibility=2.0)
        assert delivery["value"] == b"item-0" and delivery["delivery_count"] == 1
        # Prefetch still bounds in-flight deliveries for the consumer owner.
        client.queue_prefetch(1)
        assert client.queue_consume_as("consumers", "worker-a", visibility=2.0) is None
        client.queue_prefetch(0)
        assert client.stats()["queue_consumers"] == 1
    # Dropping the connection must not requeue the delivery immediately: it
    # follows its visibility deadline, then returns redelivered.
    with KuttiDBClient(port=PORT) as client:
        held = client.queue_consume("consumers")
        assert held["value"] == b"item-1"  # the other message is ready
        assert client.queue_ack("consumers", held["id"])
        assert client.queue_consume("consumers") is None
    time.sleep(2.2)
    with KuttiDBClient(port=PORT) as client:
        redelivered = client.queue_consume("consumers")
        assert redelivered["value"] == b"item-0" and redelivered["redelivered"] \
            and redelivered["delivery_count"] == 2
        assert client.queue_ack("consumers", redelivered["id"])
        # Graceful unregister requeues any held delivery immediately.
        client.queue_publish("consumers", b"late")
        client.queue_consumer_register("worker-b")
        got = client.queue_consume_as("consumers", "worker-b", visibility=60.0)
        assert got["value"] == b"late"
        client.queue_consumer_unregister("worker-b")
        back = client.queue_consume("consumers")
        assert back["message_id"] == got["message_id"] and back["redelivered"]
        assert client.queue_ack("consumers", back["id"])
        unknown = False
        try:
            client.queue_consumer_unregister("ghost")
        except Exception:
            unknown = True
        assert unknown, "unregistering an unknown consumer was accepted"

    # Registration survives a crash restart; consume-as works for the name.
    proc.kill(); proc.wait(); proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        client.queue_publish("consumers", b"after-restart")
        assert client.queue_consumer_register("worker-a") == owner
        d = client.queue_consume_as("consumers", "worker-a", visibility=60.0)
        assert d["value"] == b"after-restart"
        assert client.queue_ack("consumers", d["id"])
        assert client.queue_stats("consumers")["inflight"] == 0

    # --- delayed NACK ownership ------------------------------------------
    # The 17-byte delayed branch must accept a delivery owned by the
    # connection's named consumer, mirroring ACK and immediate NACK. The
    # server restarts first and the consumer-owner space is padded: owner
    # tokens and connection ids are independent counters, and without the
    # pad a numeric coincidence between them can hide the dispatch defect.
    proc.kill(); proc.wait(); proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        for i in range(32):
            client.queue_consumer_register("delay-pad-%d" % i)
        client.queue_declare("delaynamed", durable=True)
        client.queue_consumer_register("delay-worker")
        client.queue_publish("delaynamed", b"delayed-payload")
        got = client.queue_consume_as("delaynamed", "delay-worker", visibility=30.0)
        assert got["value"] == b"delayed-payload" and got["delivery_count"] == 1
        assert client.queue_stats("delaynamed") == {"depth": 1, "inflight": 1}
        nack_at = time.time()
        assert client.queue_nack("delaynamed", got["id"], requeue=True, delay=1.0)
        # Delayed requeue keeps the message live (depth includes it) but not
        # in flight and not consumable before the delay.
        assert client.queue_stats("delaynamed") == {"depth": 1, "inflight": 0}
        if time.time() - nack_at < 0.9:
            assert client.queue_consume("delaynamed") is None, \
                "delayed message was consumable before its delay"
        deadline = time.time() + 10
        again = None
        while time.time() < deadline:
            again = client.queue_consume_as("delaynamed", "delay-worker",
                                            visibility=30.0)
            if again is not None:
                break
            time.sleep(0.05)
        assert again is not None, "delayed named-consumer retry never became due"
        assert again["message_id"] == got["message_id"] and again["redelivered"]
        assert again["delivery_count"] == 2
        assert again["id"] != got["id"], "retry must carry a fresh delivery tag"
        assert client.queue_ack("delaynamed", again["id"])
        assert client.queue_stats("delaynamed") == {"depth": 0, "inflight": 0}

        # Immediate named-consumer NACK and ACK stay on the same rules.
        client.queue_publish("delaynamed", b"immediate")
        held = client.queue_consume_as("delaynamed", "delay-worker", visibility=30.0)
        assert client.queue_nack("delaynamed", held["id"], requeue=True)
        back = client.queue_consume_as("delaynamed", "delay-worker", visibility=30.0)
        assert back["message_id"] == held["message_id"] and back["redelivered"]
        assert client.queue_ack("delaynamed", back["id"])

        # A delivery owned by the named consumer is invisible to a foreign
        # connection (different owner token): NACK and ACK both miss.
        client.queue_publish("delaynamed", b"owned")
        owned = client.queue_consume_as("delaynamed", "delay-worker", visibility=30.0)
        assert owned is not None
        with KuttiDBClient(port=PORT) as outsider:
            assert outsider.queue_nack("delaynamed", owned["id"]) is False
            assert outsider.queue_ack("delaynamed", owned["id"]) is False
            assert outsider.queue_nack("delaynamed", 999999) is False
            assert outsider.queue_ack("delaynamed", 999999) is False
            assert client.queue_stats("delaynamed") == {"depth": 1, "inflight": 1}
            # A second connection legitimately attached to the same named
            # consumer shares the owner token and may disposition it.
            assert outsider.queue_consumer_register("delay-worker") > 0
            assert outsider.queue_consume_as("delaynamed", "delay-worker") is None
            assert outsider.queue_nack("delaynamed", owned["id"]) is True
            # Duplicate NACK of the now-requeued delivery is a miss.
            assert outsider.queue_nack("delaynamed", owned["id"]) is False
        back = client.queue_consume_as("delaynamed", "delay-worker", visibility=30.0)
        assert back["message_id"] == owned["message_id"] and back["redelivered"]
        assert client.queue_ack("delaynamed", back["id"])

        # Terminal rejection of a named-consumer delivery routes to the DLQ.
        client.queue_declare("delaydlq", durable=True,
                             dead_letter_queue="delaydlq-dead")
        client.queue_publish("delaydlq", b"reject-me")
        held = client.queue_consume_as("delaydlq", "delay-worker", visibility=30.0)
        assert client.queue_nack("delaydlq", held["id"], requeue=False)
        assert client.queue_stats("delaydlq") == {"depth": 0, "inflight": 0}
        assert client.queue_stats("delaydlq-dead") == {"depth": 1, "inflight": 0}

        # Malformed delayed-NACK payloads fail closed over the raw protocol.
        s = socket.create_connection(("127.0.0.1", PORT), 2)
        try:
            stale = struct.pack("<Q", 999999)
            assert raw_request(s, QUEUE_NACK_OP, b"delaynamed", b"\x01" * 16)[0] == 0x02
            assert raw_request(s, QUEUE_NACK_OP, b"delaynamed", b"\x01" * 10)[0] == 0x02
            assert raw_request(s, QUEUE_NACK_OP, b"delaynamed",
                               stale + b"\x02" + struct.pack("<Q", 0))[0] == 0x02
            assert raw_request(s, QUEUE_NACK_OP, b"delaynamed", stale + b"\x02")[0] == 0x02
            assert raw_request(s, QUEUE_NACK_OP, b"delaynamed",
                               stale + b"\x01" + struct.pack("<Q", 5))[0] == 0x01
        finally:
            s.close()

    # Crash: a successful named-consumer delayed NACK survives SIGKILL
    # before the retry becomes due — the message survives once, keeps its
    # delayed availability, and is consumable when due.
    with KuttiDBClient(port=PORT) as client:
        client.queue_declare("delaycrash", durable=True)
        client.queue_consumer_register("crash-worker")
        client.queue_publish("delaycrash", b"survive-once")
        d = client.queue_consume_as("delaycrash", "crash-worker", visibility=30.0)
        nack_at = time.time()
        assert client.queue_nack("delaycrash", d["id"], requeue=True, delay=3.0)
    proc.kill(); proc.wait(); proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        assert client.queue_stats("delaycrash") == {"depth": 1, "inflight": 0}, \
            "crash after delayed NACK did not keep the message exactly once"
        if time.time() - nack_at < 2.9:
            assert client.queue_consume("delaycrash") is None, \
                "recovered message ignored its delayed availability"
        deadline = time.time() + 10
        survived = None
        while time.time() < deadline:
            survived = client.queue_consume("delaycrash")
            if survived is not None:
                break
            time.sleep(0.05)
        assert survived is not None and survived["value"] == b"survive-once"
        assert survived["message_id"] == d["message_id"]
        assert client.queue_ack("delaycrash", survived["id"])
        assert client.queue_stats("delaycrash") == {"depth": 0, "inflight": 0}

    # Delayed retry must not lose a message whose delay expires after later
    # messages were already delivered (consume scan hint invariant).
    with KuttiDBClient(port=PORT) as client:
        client.queue_declare("hintq", durable=True)
        client.queue_publish_batch("hintq", [b"a", b"b", b"c"])
        first = client.queue_consume("hintq")
        assert first["value"] == b"a"
        assert client.queue_nack("hintq", first["id"], requeue=True, delay=0.3)
        second = client.queue_consume("hintq")
        assert second["value"] == b"b", "delayed head blocked later message"
        third = client.queue_consume("hintq")
        assert third["value"] == b"c"
        time.sleep(0.35)
        late = client.queue_consume("hintq")
        assert late is not None and late["value"] == b"a" and late["redelivered"], \
            "delayed message was lost after the scan hint moved past it"
        assert client.queue_ack("hintq", late["id"])
        assert client.queue_ack("hintq", second["id"])
        assert client.queue_ack("hintq", third["id"])
        assert client.queue_consume("hintq") is None

    # Batch operations: publish/consume/ACK round trip with one durability
    # round trip per batch (capability bit 12).
    with KuttiDBClient(port=PORT) as client:
        client.queue_declare("batch", durable=True, max_depth=8)
        ids = client.queue_publish_batch("batch", [b"m0", b"m1", b"m2", b"m3"])
        assert len(ids) == 4 and all(ids)
        assert client.queue_stats("batch") == {"depth": 4, "inflight": 0}
        overflow = False
        try:
            client.queue_publish_batch("batch", [b"x"] * 6)
        except Exception:
            overflow = True
        assert overflow, "batch publish ignored max_depth"
        assert client.queue_stats("batch") == {"depth": 4, "inflight": 0}
        deliveries = client.queue_consume_batch("batch", 3)
        assert [d["value"] for d in deliveries] == [b"m0", b"m1", b"m2"]
        assert [d["message_id"] for d in deliveries] == ids[:3]
        assert client.queue_stats("batch") == {"depth": 4, "inflight": 3}
        assert client.queue_ack_batch("batch", [deliveries[0]["id"], 999999,
                                                deliveries[1]["id"]]) == 2
        assert client.queue_stats("batch") == {"depth": 2, "inflight": 1}
        assert client.queue_nack_batch("batch", [deliveries[2]["id"]],
                                       requeue=True) == 1
        again = client.queue_consume("batch")
        assert again["value"] == b"m2" and again["redelivered"]
        assert client.queue_ack("batch", again["id"])
        fresh = client.queue_consume("batch")
        assert fresh["value"] == b"m3" and not fresh["redelivered"]
        assert client.queue_ack("batch", fresh["id"])
        assert client.queue_consume_batch("batch", 4) == []
        unknown = False
        try:
            client.queue_consume_batch("ghost", 4)
        except Exception:
            unknown = True
        assert unknown, "batch consume of a missing queue was accepted"
    proc.kill(); proc.wait(); proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        client.queue_publish_batch("batch", [b"after", b"restart"])
        got = client.queue_consume_batch("batch", 2)
        assert [d["value"] for d in got] == [b"after", b"restart"]
        assert client.queue_ack_batch("batch", [d["id"] for d in got]) == 2
        assert client.queue_stats("batch") == {"depth": 0, "inflight": 0}
    # Checkpoint under the live server: drained history crosses the WAL
    # trigger, maintenance compacts it, and a SIGKILL restart keeps the
    # live messages (capability: queue_checkpoint_maybe).
    proc.kill(); proc.wait(); proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        client.queue_declare("ckpt", durable=True)
        blob = b"p" * 100
        for _ in range(60):
            client.queue_publish_batch("ckpt", [blob] * 256)
            deliveries = client.queue_consume_batch("ckpt", 256)
            client.queue_ack_batch("ckpt", [d["id"] for d in deliveries])
        for i in range(10):
            client.queue_publish("ckpt", b"live-%d" % i)
        time.sleep(2.5)  # maintenance interval passes at least once
    wal_size = os.path.getsize(wal)
    print("checkpoint: WAL bounded to %d bytes (history was ~2.9 MB)" % wal_size)
    proc.kill(); proc.wait(); proc = start(wal)
    with KuttiDBClient(port=PORT) as client:
        stats = client.queue_stats("ckpt")
        assert stats and stats["depth"] == 10, \
            "checkpoint restart lost live messages: %r" % stats
        got = client.queue_consume_batch("ckpt", 10)
        assert [d["value"] for d in got] == [b"live-%d" % i for i in range(10)]
        assert client.queue_ack_batch("ckpt", [d["id"] for d in got]) == 10
        assert client.queue_stats("ckpt")["depth"] == 0
    # The trigger fires when the WAL exceeds 2x live state + 1 MiB, so a
    # tail of up to the 1 MiB floor may legitimately remain after the last
    # checkpoint. Bounded means: below the threshold, not proportional to
    # the ~2.9 MB drained history.
    if wal_size > 12 * 1024 * 1024 // 10:
        raise AssertionError("checkpoint did not bound the WAL: %d bytes" % wal_size)
    print("QUEUE PROTOCOL + RECOVERY TESTS PASSED")
finally:
    if proc:
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass
    shutil.rmtree(tmp, ignore_errors=True)
