"""Atomic cache-plus-message protocol and crash-boundary tests.

Boundary cases are built by crafting the two WAL files directly, so each
commit window is tested deterministically against the real server binary:
  1. queue prepare without a cache commit   -> neither side
  2. cache commit without a queue TX_COMMIT -> recovery materializes both
  3. prepare + commit in the queue WAL      -> replay materializes both
  4. torn / CRC-corrupt cache commit        -> neither side
A live SIGKILL loop then checks the strongest guarantee: every acknowledged
transaction is fully present after recovery, and every recovered message
belongs to a cache key that also exists.
"""

import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import zlib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient

PORT = 7407


def wait_port():
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.1).close()
            return
        except OSError:
            time.sleep(0.03)
    raise RuntimeError("atomic server did not start")


def start(kuttidb_wal, queue_wal):
    server = os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb"))
    stderr = None
    if os.environ.get("KUTTIDB_STDERR"):
        stderr = open(os.environ["KUTTIDB_STDERR"], "ab")
    proc = subprocess.Popen([server, str(PORT), kuttidb_wal, "100",
                             "--queue-wal", queue_wal],
                            stderr=stderr, stdout=stderr, start_new_session=True)
    if stderr:
        stderr.close()
    wait_port()
    if os.environ.get("KUTTIDB_TRACE"):
        print(f"started pid={proc.pid} for {kuttidb_wal}", flush=True)
    return proc


# ---- WAL crafting helpers (formats: server.c wal_log_*, queue.c append_record)

def kuttidb_rec(op, key, blob):
    hdr = struct.pack("<BHI", op, len(key), len(blob))
    return hdr + struct.pack("<I", zlib.crc32(key + blob) & 0xFFFFFFFF) + key + blob

def kuttidb_put(key, value):
    return kuttidb_rec(0x01, key, value)

def kuttidb_tx(tx_id, sub_op, exp, key, value, corrupt=False, truncate=0):
    blob = struct.pack("<QBI", tx_id, sub_op, exp) + value
    rec = kuttidb_rec(0x08, key, blob)
    if truncate:
        return rec[:truncate]
    if corrupt:
        rec = bytearray(rec)
        rec[-1] ^= 0xFF
        rec = bytes(rec)
    return rec

def qrec(op, name, payload, durable=1, ident=0, expires=0):
    hdr = struct.pack("<BBHIQQ", op, durable, len(name), len(payload),
                      ident, expires)
    crc = zlib.crc32(hdr[:24] + name + payload) & 0xFFFFFFFF
    return hdr + struct.pack("<I", crc) + name + payload

def qdeclare(name, durable=1, max_depth=0):
    return qrec(1, name, b"", durable=durable, expires=max_depth)

def tx_prepare(tx_id, targets, msg, expires=0):
    p = struct.pack("<QIQH", tx_id, len(msg), expires, len(targets))
    for qname, msg_id in targets:
        p += struct.pack("<H", len(qname)) + qname + struct.pack("<Q", msg_id)
    return p + msg

def tx_commit(tx_id):
    return struct.pack("<Q", tx_id)


def stop(proc):
    """Kill and reap. SIGKILL is asynchronous: without wait() the next
    server's wait_port can succeed against the dying process's still-open
    listen backlog and the real client gets reset."""
    proc.kill()
    proc.wait()


tmp = tempfile.mkdtemp(prefix="kuttidb-atomic-proto-")
procs = []
try:
    # ---- boundary 1: prepare without cache commit -> neither side ----------
    cw = os.path.join(tmp, "b1.wal"); qw = os.path.join(tmp, "b1.queues")
    open(cw, "wb").write(kuttidb_put(b"plain", b"v"))
    open(qw, "wb").write(qdeclare(b"jobs") +
                         qrec(9, b"jobs", tx_prepare(0xAAAA0001,
                                                     [(b"jobs", 1)], b"m1")))
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        assert c.get("plain") == b"v", "cache recovery broken"
        assert c.get("tx-key-1") is None
        assert c.queue_consume("jobs") is None, "prepare alone must not deliver"
    stop(procs.pop())

    # ---- boundary 2: cache commit, queue TX_COMMIT missing -> both sides ---
    cw = os.path.join(tmp, "b2.wal"); qw = os.path.join(tmp, "b2.queues")
    open(cw, "wb").write(kuttidb_put(b"plain", b"v") +
                         kuttidb_tx(0xAAAA0002, 1, 0, b"tx-key-2", b"v2"))
    open(qw, "wb").write(qdeclare(b"jobs") +
                         qrec(9, b"jobs", tx_prepare(0xAAAA0002,
                                                     [(b"jobs", 2)], b"m2")))
    procs.append(start(cw, qw))
    try:
        with KuttiDBClient(port=PORT) as c:
            assert c.get("tx-key-2") == b"v2", "cache commit must survive"
            delivery = c.queue_consume("jobs")
            assert delivery and delivery["value"] == b"m2", \
                "reconciliation must materialize the event"
            assert c.queue_ack("jobs", delivery["id"])
    except Exception as e:
        print("DEBUG b2 failure:", type(e).__name__, e,
              "poll =", procs[-1].poll(), flush=True)
        raise
    stop(procs.pop())

    # recovery wrote the TX_COMMIT: a second restart replays it exactly once
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        assert c.queue_consume("jobs") is None, "no duplicate from replay"
        assert c.get("tx-key-2") == b"v2", "cache commit still present"
    stop(procs.pop())

    # ---- boundary 3: prepare + commit both present -> direct replay --------
    cw = os.path.join(tmp, "b3.wal"); qw = os.path.join(tmp, "b3.queues")
    open(cw, "wb").write(kuttidb_tx(0xAAAA0003, 1, 0, b"tx-key-3", b"v3"))
    open(qw, "wb").write(qdeclare(b"jobs") +
                         qrec(9, b"jobs", tx_prepare(0xAAAA0003,
                                                     [(b"jobs", 3)], b"m3")) +
                         qrec(10, b"jobs", tx_commit(0xAAAA0003)))
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        assert c.get("tx-key-3") == b"v3"
        delivery = c.queue_consume("jobs")
        assert delivery and delivery["value"] == b"m3"
        assert delivery["message_id"] == 3, "pre-allocated id preserved"
        c.queue_ack("jobs", delivery["id"])
    stop(procs.pop())

    # ---- boundary 4: torn and corrupt cache commits -> neither side --------
    for mode in ("torn", "corrupt"):
        cw = os.path.join(tmp, f"b4-{mode}.wal")
        qw = os.path.join(tmp, f"b4-{mode}.queues")
        tx_bytes = kuttidb_tx(0xAAAA0004, 1, 0, b"tx-key-4", b"v4")
        body = kuttidb_put(b"plain", b"v")
        if mode == "torn":
            body += tx_bytes[:len(tx_bytes) // 2]
        else:
            body += kuttidb_tx(0xAAAA0004, 1, 0, b"tx-key-4", b"v4", corrupt=True)
        open(cw, "wb").write(body)
        open(qw, "wb").write(qdeclare(b"jobs") +
                             qrec(9, b"jobs", tx_prepare(0xAAAA0004,
                                                         [(b"jobs", 4)],
                                                         b"m4")))
        procs.append(start(cw, qw))
        with KuttiDBClient(port=PORT) as c:
            assert c.get("plain") == b"v", "valid prefix survives"
            assert c.get("tx-key-4") is None, "broken commit must not apply"
            assert c.queue_consume("jobs") is None, \
                "uncommitted prepare must be discarded"
        stop(procs.pop())

    # ---- boundary 5: fanout transaction recovers into every target ---------
    cw = os.path.join(tmp, "b5.wal"); qw = os.path.join(tmp, "b5.queues")
    open(cw, "wb").write(kuttidb_tx(0xAAAA0005, 1, 0, b"tx-key-5", b"v5"))
    open(qw, "wb").write(qdeclare(b"f1") + qdeclare(b"f2") +
                         qrec(9, b"fx", tx_prepare(0xAAAA0005,
                                                   [(b"f1", 5), (b"f2", 6)],
                                                   b"m5")))
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        assert c.get("tx-key-5") == b"v5"
        for queue in ("f1", "f2"):
            delivery = c.queue_consume(queue)
            assert delivery and delivery["value"] == b"m5"
            assert c.queue_ack(queue, delivery["id"])
        assert c.queue_consume("f1") is None and c.queue_consume("f2") is None
    stop(procs.pop())

    # ---- boundary 6: UPDATE_AND_EMIT miss commits nothing (live) -----------
    cw = os.path.join(tmp, "b6.wal"); qw = os.path.join(tmp, "b6.queues")
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        c.exchange_declare("ux", type="direct")
        c.queue_declare("uq", durable=True)
        c.exchange_bind("ux", "uq", "rk")
        result = c.update_and_emit("absent-state", b"v9",
                                   exchange="ux", routing_key="rk")
        assert result["tx_id"] == 0 and result["unroutable"], \
            "update of a missing key must MISS"
        assert c.get("absent-state") is None, "miss must not create the key"
        assert c.queue_consume("uq") is None, "miss must not emit an event"
        # a deleted key behaves the same
        c.put("gone-state", b"old")
        c.delete("gone-state")
        result = c.update_and_emit("gone-state", b"v9",
                                   exchange="ux", routing_key="rk")
        assert result["tx_id"] == 0 and c.get("gone-state") is None
        assert c.queue_consume("uq") is None
        # hit path commits both sides under one durable tx id
        c.put("live-state", b"old")
        result = c.update_and_emit("live-state", b"new",
                                   exchange="ux", routing_key="rk")
        assert result["tx_id"] and result["routed"] == 1, result
        assert c.get("live-state") == b"new"
        delivery = c.queue_consume("uq")
        assert delivery and delivery["value"] == b"new", \
            "emitted event must carry the committed update"
        assert c.queue_ack("uq", delivery["id"])
    stop(procs.pop())

    # ---- boundary 7: conditional marker for an existing key ----------------
    # (sub_op 8: replay applies the update and reconciliation materializes
    #  the prepared event -- both sides)
    cw = os.path.join(tmp, "b7.wal"); qw = os.path.join(tmp, "b7.queues")
    open(cw, "wb").write(kuttidb_put(b"upd-state", b"seed") +
                         kuttidb_tx(0xAAAA0007, 8, 0, b"upd-state", b"upd7"))
    open(qw, "wb").write(qdeclare(b"uq") +
                         qrec(9, b"uq", tx_prepare(0xAAAA0007,
                                                   [(b"uq", 7)], b"upd7")))
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        assert c.get("upd-state") == b"upd7", \
            "conditional marker must apply to an existing key"
        delivery = c.queue_consume("uq")
        assert delivery and delivery["value"] == b"upd7", \
            "committed conditional transaction must materialize its event"
        assert c.queue_ack("uq", delivery["id"])
    stop(procs.pop())

    # idempotent: a second restart replays the same marker exactly once
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        assert c.get("upd-state") == b"upd7"
        assert c.queue_consume("uq") is None, "no duplicate from replay"
    stop(procs.pop())

    # ---- boundary 8: conditional marker for a missing key -> neither side --
    # (a live server never writes this; a crafted one must not materialize
    #  the event either, so recovery keeps both-or-neither)
    cw = os.path.join(tmp, "b8.wal"); qw = os.path.join(tmp, "b8.queues")
    open(cw, "wb").write(kuttidb_put(b"other", b"v") +
                         kuttidb_tx(0xAAAA0008, 8, 0, b"never-seeded", b"x"))
    open(qw, "wb").write(qdeclare(b"uq") +
                         qrec(9, b"uq", tx_prepare(0xAAAA0008,
                                                   [(b"uq", 8)], b"ghost")))
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        assert c.get("other") == b"v", "valid prefix survives"
        assert c.get("never-seeded") is None, \
            "conditional marker must not create a missing key"
        assert c.queue_consume("uq") is None, \
            "event without its cache update must be discarded"
    stop(procs.pop())

    # ---- live SIGKILL: acknowledged transactions survive, nothing partial --
    cw = os.path.join(tmp, "kill.wal"); qw = os.path.join(tmp, "kill.queues")
    procs.append(start(cw, qw))
    confirmed = 0
    with KuttiDBClient(port=PORT) as c:
        c.queue_declare("jobs", durable=True, max_depth=100000)
        deadline = time.time() + 0.4
        i = 0
        while time.time() < deadline:
            i += 1
            try:
                result = c.put_and_enqueue(f"kill-key-{i}", f"kill-key-{i}".encode(),
                                           queue="jobs")
            except Exception:
                break  # server died mid-request
            if not result.get("unroutable"):
                confirmed = i
    stop(procs[-1])
    assert confirmed > 20, f"killed too early ({confirmed} confirmed)"
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        # every acknowledged transaction is fully present
        for i in range(1, confirmed + 1):
            key = f"kill-key-{i}".encode()
            assert c.get(f"kill-key-{i}") == key, f"acknowledged key {i} lost"
        drained = []
        while True:
            delivery = c.queue_consume("jobs")
            if delivery is None:
                break
            drained.append(delivery["value"])
            assert c.queue_ack("jobs", delivery["id"])
        # every recovered message has its cache counterpart (both or neither)
        for value in drained:
            assert c.get(value.decode()) == value, \
                "message without committed cache state"
        assert len(drained) >= confirmed, \
            "acknowledged message lost"
        stats = c.stats()
        recovered_first = len(drained)
    stop(procs.pop())

    # ---- live SIGKILL with UPDATE_AND_EMIT ---------------------------------
    # Every acknowledged update must keep its emitted event and leave the
    # cache value equal to the acked payload (each key is updated once, so
    # the mapping is unambiguous), while a seeded key whose update was never
    # acknowledged may keep its seed.
    cw = os.path.join(tmp, "ukill.wal"); qw = os.path.join(tmp, "ukill.queues")
    procs.append(start(cw, qw))
    uconfirmed = 0
    with KuttiDBClient(port=PORT) as c:
        c.exchange_declare("ux", type="direct")
        c.queue_declare("uq", durable=True)
        c.exchange_bind("ux", "uq", "rk")
        deadline = time.time() + 0.4
        i = 0
        while time.time() < deadline:
            i += 1
            try:
                c.put(f"upd-key-{i}", b"seed")
                result = c.update_and_emit(f"upd-key-{i}",
                                           f"upd-{i}".encode(),
                                           exchange="ux", routing_key="rk")
            except Exception:
                break  # server died mid-request
            if not result.get("unroutable"):
                uconfirmed = i
    stop(procs[-1])
    assert uconfirmed > 20, f"killed too early ({uconfirmed} confirmed)"
    procs.append(start(cw, qw))
    with KuttiDBClient(port=PORT) as c:
        for i in range(1, uconfirmed + 1):
            key = f"upd-key-{i}"
            assert c.get(key) == f"upd-{i}".encode(), \
                f"acknowledged update {i} not fully present"
        drained = set()
        while True:
            delivery = c.queue_consume("uq")
            if delivery is None:
                break
            drained.add(delivery["value"])
            assert c.queue_ack("uq", delivery["id"])
        missing = {f"upd-{i}".encode() for i in range(1, uconfirmed + 1)} - drained
        assert not missing, f"acknowledged update events lost: {sorted(missing)[:5]}"
        # no event without its committed cache state
        for value in drained:
            if value.startswith(b"upd-"):
                assert c.get("upd-key-" + value.decode().split("-")[1]) == value, \
                    "update event without committed cache state"
    stop(procs.pop())
    print(f"ATOMIC PROTOCOL + CRASH BOUNDARY TESTS PASSED "
          f"({confirmed} acknowledged at SIGKILL, {recovered_first} recovered, "
          f"{uconfirmed} acknowledged updates at SIGKILL)")
finally:
    for proc in procs:
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass
    shutil.rmtree(tmp, ignore_errors=True)
