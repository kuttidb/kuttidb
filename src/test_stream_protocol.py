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

PORT = 7411
LEGACY_PORT = 7415

def wait_port(port=PORT):
    until = time.time() + 8
    while time.time() < until:
        try:
            socket.create_connection(("127.0.0.1", port), .1).close()
            return
        except OSError:
            time.sleep(.03)
    raise RuntimeError("server did not start")

def start(wal, *extra):
    p = subprocess.Popen([os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb")),
                          str(PORT), "-", "100", "--stream-wal", wal, *extra],
                         stderr=subprocess.DEVNULL, start_new_session=True)
    wait_port()
    return p

tmp = tempfile.mkdtemp(prefix="kuttidb-stream-proto-")
wal = os.path.join(tmp, "stream.wal")
p = None
try:
    p = start(wal)
    with KuttiDBClient(port=PORT) as c:
        c.stream_declare("orders", partitions=3)
        assert c.stream_append("orders", b"one", partition=0) == {"partition": 0, "offset": 0}
        assert c.stream_append("orders", b"two", partition=0) == {"partition": 0, "offset": 1}
        batch = c.stream_append_many("orders", [(b"customer-1", b"three"),
                                                  (b"customer-2", b"four")])
        assert len(batch) == 2
        # Keyed selection is deterministic and offsets are per partition.
        first = c.stream_append("orders", b"key", key="customer-42")
        again = c.stream_append("orders", b"key2", key="customer-42")
        assert first["partition"] == again["partition"] and again["offset"] == first["offset"] + 1
        items = c.stream_fetch("orders", partition=0, offset=0)
        assert [x["value"] for x in items[:2]] == [b"one", b"two"]
        records = [record for partition in range(3)
                   for record in c.stream_fetch("orders", partition=partition)]
        assert (b"customer-1", b"three") in [(r["key"], r["value"]) for r in records]
        assert (b"customer-42", b"key") in [(r["key"], r["value"]) for r in records]
        topics = c.stream_list()
        # six records appended so far ("one", "two", the two-item batch, and
        # the two keyed appends)
        assert topics == [{"topic": "orders", "partitions": 3,
                           "records": 6, "bytes": topics[0]["bytes"]}], topics
        first = c.stream_group_join("orders", "workers")
        assert first.partitions == [0, 1, 2] and first.generation == 1
        groups = c.stream_group_list()
        assert groups == [{"topic": "orders", "group": "workers",
                           "generation": 1, "members": 1}], groups
        # A heartbeat must not change the generation.
        assert c.stream_group_join("orders", "workers").generation == 1
        c.stream_commit("orders", "workers", 0, 2)
        assert c.stream_group_offset("orders", "workers", 0) == 2
        with KuttiDBClient(port=PORT) as second:
            c.stream_group_join("orders", "rebalance")
            second_assignment = second.stream_group_join("orders", "rebalance")
            first_assignment = c.stream_group_join("orders", "rebalance")
            # The second join is a membership change: the generation moves.
            assert sorted(first_assignment.partitions +
                          second_assignment.partitions) == [0, 1, 2]
            assert first_assignment.generation == second_assignment.generation == 2
            # A disconnect must eventually remove the member and move the
            # generation again; the connection close is asynchronous.
            third = KuttiDBClient(port=PORT)
            third.stream_group_join("orders", "rebalance")
            third.close()
            deadline = time.time() + 5
            while True:
                drained = c.stream_group_join("orders", "rebalance")
                if drained.generation >= 4:
                    break
                if time.time() > deadline:
                    raise AssertionError("disconnect rebalance was not observed")
                time.sleep(0.05)
            assert set(drained.partitions) | set(second_assignment.partitions) == {0, 1, 2}
        deadline = time.time() + 5
        while True:
            final = c.stream_group_join("orders", "rebalance")
            if final.partitions == [0, 1, 2]:
                break
            if time.time() > deadline:
                raise AssertionError("second member's leave was not observed")
            time.sleep(0.05)
        assert c.stats()["stream_topics"] == 1
        # A graceful leave releases the assignment immediately and is an
        # error for a group this connection never joined.
        c.stream_group_join("orders", "drain")
        c.stream_group_leave("orders", "drain")
        refused = False
        try:
            c.stream_group_leave("orders", "drain")
        except Exception:
            refused = True
        assert refused, "leave of an unjoined group was accepted"
    p.kill(); p.wait(); p = start(wal)
    with KuttiDBClient(port=PORT) as c:
        assert [x["value"] for x in c.stream_fetch("orders", partition=0)[:2]] == [b"one", b"two"]
        records = [record for partition in range(3)
                   for record in c.stream_fetch("orders", partition=partition)]
        assert len(records) == 6
        assert (b"customer-2", b"four") in [(r["key"], r["value"]) for r in records]
        assert (b"customer-42", b"key2") in [(r["key"], r["value"]) for r in records]
        assert c.stream_group_offset("orders", "workers", 0) == 2
        bad = False
        try:
            c.stream_declare("orders", partitions=2)
        except Exception:
            bad = True
        assert bad
    p.kill(); p.wait(); p = None
    # The stream engine must reject an oversized fetch before allocating an
    # unbounded response. The record itself is allowed by max-value; only the
    # server's response/batch ceiling is deliberately smaller.
    bounded_wal = os.path.join(tmp, "bounded.wal")
    p = start(bounded_wal, "--max-batch-mb", "1")
    with KuttiDBClient(port=PORT) as c:
        c.stream_declare("bounded", partitions=1)
        c.stream_append("bounded", b"x" * (1 << 20), partition=0)
        refused = False
        try:
            c.stream_fetch("bounded", partition=0)
        except Exception:
            refused = True
        assert refused, "oversized stream fetch was accepted"
    p.kill(); p.wait(); p = start(wal)

    # Batch offset commit: several partitions of one group in one round trip.
    with KuttiDBClient(port=PORT) as c:
        c.stream_declare("batchc", partitions=3)
        for part in range(3):
            for i in range(2):
                c.stream_append("batchc", b"r", partition=part)
        c.stream_group_join("batchc", "g", lease=60.0)
        c.stream_commit("batchc", "g", 0, 1)
        c.stream_commit_batch("batchc", "g", [(1, 2), (2, 1), (0, 2)])
        assert c.stream_group_offset("batchc", "g", 0) == 2
        assert c.stream_group_offset("batchc", "g", 1) == 2
        assert c.stream_group_offset("batchc", "g", 2) == 1
        bad = False
        try:
            c.stream_commit_batch("batchc", "g", [(1, 3), (3, 0)])
        except Exception:
            bad = True
        assert bad, "commit batch accepted an unknown partition"
        assert c.stream_group_offset("batchc", "g", 1) == 2
    p.kill(); p.wait(); p = start(wal)
    with KuttiDBClient(port=PORT) as c:
        # Committed batch offsets survive restart.
        assert c.stream_group_offset("batchc", "g", 1) == 2
        assert c.stream_group_offset("batchc", "g", 2) == 1

    # --- native replay contract (0x6d, capability bit 17, protocol 1.9) ---
    import struct as _struct
    import zlib as _zlib

    def meta_request(sock, topic, partition, offset, max_records, expected=None):
        value = (_struct.pack("<IQI", partition, offset, max_records) +
                 (b"\x01" + expected if expected else b"\x00"))
        frame = (bytes([0x6d]) + _struct.pack("<H", len(topic)) +
                 _struct.pack("<I", len(value)) + topic.encode() + value)
        sock.sendall(frame)
        head = b""
        while len(head) < 5:
            chunk = sock.recv(5 - len(head))
            if not chunk:
                raise RuntimeError("connection closed")
            head += chunk
        need = _struct.unpack("<I", head[1:5])[0]
        payload = b""
        while len(payload) < need:
            chunk = sock.recv(need - len(payload))
            if not chunk:
                raise RuntimeError("connection closed")
            payload += chunk
        return head[0], payload

    def parse_meta(payload):
        at = 0
        rng = payload[at]; at += 1
        base, nxt, resume = _struct.unpack("<QQQ", payload[at:at + 24]); at += 24
        stream_id = payload[at:at + 16].hex(); at += 16
        count = _struct.unpack("<I", payload[at:at + 4])[0]; at += 4
        records = []
        for _ in range(count):
            off, klen, vlen = _struct.unpack("<QHI", payload[at:at + 14]); at += 14
            key = payload[at:at + klen]; at += klen
            value = payload[at:at + vlen]; at += vlen
            records.append({"offset": off, "key": key, "value": value})
        assert at == len(payload), "trailing bytes in metadata response"
        return {"range": rng, "base": base, "next": nxt, "resume": resume,
                "stream_id": stream_id, "records": records}

    with KuttiDBClient(port=PORT) as c:
        caps = c.capabilities()
        assert caps["minor"] == 9, "protocol minor must be 1.9"
        assert caps["features"] & (1 << 17), "stream replay capability missing"
        c.stream_declare("replay", partitions=2, max_bytes=25)
        s = socket.create_connection(("127.0.0.1", PORT), 2)
        try:
            # Never-written partition: base=next=0; request 0 → success,
            # empty, resume=0, valid stream ID.
            status, payload = meta_request(s, "replay", 1, 0, 10)
            assert status == 0x00
            meta = parse_meta(payload)
            assert meta["range"] == 0 and meta["base"] == 0 and meta["next"] == 0
            assert meta["resume"] == 0 and len(meta["records"]) == 0
            assert len(meta["stream_id"]) == 32
            # Retained 5..9 via size retention (ten 5-byte records, 25B).
            batch = c.stream_append_many("replay", [(b"r%02dxy" % i)[:5] for i in range(10)])
            assert batch[0]["partition"] == 0 and batch[9]["offset"] == 9
            # Request 5 → success, 5 records, resume=10.
            status, payload = meta_request(s, "replay", 0, 5, 5)
            meta = parse_meta(payload)
            assert meta["range"] == 0 and meta["base"] == 5 and meta["next"] == 10
            assert meta["resume"] == 10 and len(meta["records"]) == 5
            actual_id = bytes.fromhex(meta["stream_id"])
            assert [r["offset"] for r in meta["records"]] == [5, 6, 7, 8, 9]
            actual_id = bytes.fromhex(meta["stream_id"])
            # Request 9 → success starting at the requested offset.
            status, payload = meta_request(s, "replay", 0, 9, 5)
            meta = parse_meta(payload)
            assert meta["range"] == 0 and len(meta["records"]) == 1 and meta["resume"] == 10
            # Request 3 → offset_expired with boundaries, no records.
            status, payload = meta_request(s, "replay", 0, 3, 5)
            meta = parse_meta(payload)
            assert meta["range"] == 1 and meta["base"] == 5 and meta["next"] == 10
            assert meta["records"] == []
            # Request 10 (tail) → success, empty, resume=10.
            status, payload = meta_request(s, "replay", 0, 10, 5)
            meta = parse_meta(payload)
            assert meta["range"] == 0 and meta["resume"] == 10 and meta["records"] == []
            # Request 11 → offset_ahead with boundaries.
            status, payload = meta_request(s, "replay", 0, 11, 5)
            meta = parse_meta(payload)
            assert meta["range"] == 2 and meta["base"] == 5 and meta["next"] == 10
            # Matching expected identity keeps the page.
            status, payload = meta_request(s, "replay", 0, 5, 5, expected=actual_id)
            meta = parse_meta(payload)
            assert meta["range"] == 0 and len(meta["records"]) == 5
            # Wrong expected identity → stream_recreated even for a valid
            # offset; the actual identity and boundaries come back.
            mismatched = bytearray(actual_id)
            mismatched[0] ^= 0xFF
            status, payload = meta_request(s, "replay", 0, 5, 5, expected=bytes(mismatched))
            meta = parse_meta(payload)
            assert meta["range"] == 3 and meta["records"] == []
            assert meta["stream_id"] == actual_id.hex()
            # Missing topic → typed error, never an empty valid stream.
            status, payload = meta_request(s, "ghost", 0, 0, 5)
            assert status == 0x02 and payload == b"\x01"
            # Invalid partition → same typed error.
            status, payload = meta_request(s, "replay", 2, 0, 5)
            assert status == 0x02 and payload == b"\x01"
            # Malformed request shape fails closed with a typed code.
            value = b"\x01" * 16
            frame = bytes([0x6d]) + _struct.pack("<H", 6) + _struct.pack("<I", 16) + b"replay" + value
            s.sendall(frame)
            head = s.recv(5)
            assert head[0] == 0x02, "malformed metadata request accepted"
        finally:
            s.close()

    # Legacy fetch stays byte-compatible on the same (new-format) WAL.
    with KuttiDBClient(port=PORT) as c:
        items = c.stream_fetch("replay", partition=0, offset=5, max_records=5)
        assert [x["offset"] for x in items] == [5, 6, 7, 8, 9]

    # Crash: the acknowledged declaration identity survives SIGKILL, gaps
    # survive complete expiry across restart, and the next append does not
    # rewind the high-water mark.
    p.kill(); p.wait(); p = start(wal)
    with KuttiDBClient(port=PORT) as c:
        c.stream_declare("crashq", partitions=2, max_age=0.2)
        c.stream_append("crashq", b"gone", partition=0)
        c.stream_append("crashq", b"gone2", partition=0)
        time.sleep(0.35)
        sock = socket.create_connection(("127.0.0.1", PORT), 2)
        try:
            status, payload = meta_request(sock, "crashq", 0, 0, 10)
            meta = parse_meta(payload)
            assert meta["range"] == 1 and meta["base"] == 2 and meta["next"] == 2, meta
            crash_id = meta["stream_id"]
        finally:
            sock.close()
    p.kill(); p.wait(); p = start(wal)
    with KuttiDBClient(port=PORT) as c:
        sock = socket.create_connection(("127.0.0.1", PORT), 2)
        try:
            status, payload = meta_request(sock, "crashq", 0, 0, 10)
            meta = parse_meta(payload)
            assert meta["range"] == 1 and meta["base"] == 2 and meta["next"] == 2
            assert meta["stream_id"] == crash_id, "restart changed the topic incarnation"
        finally:
            sock.close()
        assert c.stream_append("crashq", b"fresh", partition=0) == {"partition": 0, "offset": 2}

    # Legacy WAL migration: a pre-identity WAL (old binary format) upgrades
    # in place, assigning durable identities without losing anything.
    legacy = os.path.join(tmp, "legacy.wal")
    def legacy_record(op, body):
        crc = _zlib.crc32(bytes([op])) ^ _zlib.crc32(body)
        return bytes([op]) + _struct.pack("<I", len(body)) + _struct.pack("<I", crc) + body
    buf = bytearray()
    name = b"legacy"
    buf += legacy_record(1, _struct.pack("<H", 6) + name + _struct.pack("<IQQ", 2, 0, 0))
    for off in (0, 1, 2):
        body = (_struct.pack("<H", 6) + name + _struct.pack("<IQQI", 0, off, 1700000000000, 3) + b"val")
        buf += legacy_record(2, body)
    body = (_struct.pack("<H", 6) + name + _struct.pack("<H", 1) + b"g" + _struct.pack("<IQ", 0, 2))
    buf += legacy_record(3, body)
    with open(legacy, "wb") as fh:
        fh.write(bytes(buf))
    p.kill(); p.wait()
    lproc = subprocess.Popen([os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb")),
                              str(LEGACY_PORT), "-", "100", "--stream-wal", legacy],
                             stderr=subprocess.DEVNULL, start_new_session=True)
    wait_port(LEGACY_PORT)
    try:
        with KuttiDBClient(port=LEGACY_PORT) as c:
            sock = socket.create_connection(("127.0.0.1", LEGACY_PORT), 2)
            try:
                status, payload = meta_request(sock, "legacy", 0, 0, 10)
                assert status == 0x00
                meta = parse_meta(payload)
                assert meta["range"] == 0 and meta["base"] == 0 and meta["next"] == 3
                assert len(meta["records"]) == 3
                migrated = meta["stream_id"]
            finally:
                sock.close()
            # Records and group commits are preserved by the upgrade.
            items = c.stream_fetch("legacy", partition=0, offset=0, max_records=10)
            assert [x["value"] for x in items] == [b"val", b"val", b"val"]
            assert c.stream_group_offset("legacy", "g", 0) == 2
        lproc.kill(); lproc.wait()
        # The migrated identity is durable: reopening assigns nothing new.
        lproc = subprocess.Popen([os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb")),
                                  str(LEGACY_PORT), "-", "100", "--stream-wal", legacy],
                                 stderr=subprocess.DEVNULL, start_new_session=True)
        wait_port(LEGACY_PORT)
        with KuttiDBClient(port=LEGACY_PORT) as c:
            sock = socket.create_connection(("127.0.0.1", LEGACY_PORT), 2)
            try:
                status, payload = meta_request(sock, "legacy", 0, 0, 10)
                meta = parse_meta(payload)
                assert meta["stream_id"] == migrated, "replay regenerated a migrated identity"
            finally:
                sock.close()
    finally:
        lproc.kill(); lproc.wait()
    print("STREAM PROTOCOL + RECOVERY TESTS PASSED")
finally:
    if p:
        p.kill(); p.wait()
    shutil.rmtree(tmp, ignore_errors=True)
