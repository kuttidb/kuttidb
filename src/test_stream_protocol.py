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

def wait_port():
    until = time.time() + 8
    while time.time() < until:
        try:
            socket.create_connection(("127.0.0.1", PORT), .1).close()
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
    print("STREAM PROTOCOL + RECOVERY TESTS PASSED")
finally:
    if p:
        p.kill(); p.wait()
    shutil.rmtree(tmp, ignore_errors=True)
