#!/usr/bin/env python3
"""Cross-version durable WAL compatibility and SIGKILL recovery.

A WAL written by one build must replay completely under the other, in both
directions: the staging buffer, the explicit write offset, the space
reservation and the slice-by-8 checksum all changed how bytes reach the file,
so this verifies the bytes themselves are still interchangeable.
"""
import os, shutil, signal, socket, subprocess, sys, tempfile, time
from pathlib import Path
sys.path.insert(0, "/root/perf/work/src")
from kuttidb_client import KuttiDBClient

PORT = int(os.environ.get("PORT", "17999"))
VALUE = b"compat-" + b"y" * 93


def start(binary, data_dir, port):
    log = open(Path(data_dir) / "server.log", "a")
    p = subprocess.Popen([binary, str(port), str(Path(data_dir) / "cache.wal"), "100",
                          "--threads", "1"], stdout=log, stderr=log)
    for _ in range(500):
        if p.poll() is not None:
            raise RuntimeError(f"{binary} exited: rc={p.returncode}")
        try:
            with KuttiDBClient(port=port, timeout=0.2) as db:
                db.health()
            return p
        except (OSError, RuntimeError):
            time.sleep(0.02)
    raise RuntimeError(f"{binary} never became ready")


def leg(writer, reader, count, label, port):
    data = tempfile.mkdtemp(prefix="walcompat-", dir="/root/perf/data")
    try:
        srv = start(writer, data, port)
        with KuttiDBClient(port=port, timeout=30) as db:
            db.queue_declare("compat", durable=True)
            db.queue_prefetch(1024)
            ids = []
            for s in range(0, count, 256):
                ids.extend(db.queue_publish_batch("compat", [VALUE] * min(256, count - s)))
            # A few singles too: they take the non-batch durable path.
            for _ in range(20):
                ids.append(db.queue_publish("compat", VALUE))
            depth = db.queue_stats("compat")["depth"]
        # SIGKILL: no clean close, so the reader must replay the raw WAL,
        # including any reserved tail the writer left behind.
        srv.send_signal(signal.SIGKILL)
        srv.wait()
        wal = Path(data) / "cache.wal.queues"
        size = wal.stat().st_size if wal.exists() else -1

        srv = start(reader, data, port + 1)
        with KuttiDBClient(port=port + 1, timeout=30) as db:
            stats = db.queue_stats("compat")
            got = []
            while len(got) < len(ids):
                msgs = db.queue_consume_batch("compat", 256)
                if not msgs:
                    break
                for m in msgs:
                    if m["value"] != VALUE:
                        raise AssertionError(f"{label}: payload mismatch at {len(got)}")
                    got.append(m["message_id"])
                db.queue_ack_batch("compat", [m["id"] for m in msgs])
            final = db.queue_stats("compat")
        srv.terminate(); srv.wait(timeout=10)
        ok = got == ids and stats["depth"] == depth and final["depth"] == 0
        print(f"{label:34s} wrote={len(ids):6d} depth_after_replay={stats['depth']:6d} "
              f"read_back={len(got):6d} order_ok={got == ids} drained={final['depth'] == 0} "
              f"wal={size}B -> {'PASS' if ok else 'FAIL'}")
        return ok
    finally:
        shutil.rmtree(data, ignore_errors=True)


if __name__ == "__main__":
    old, new = sys.argv[1], sys.argv[2]
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 5000
    results = [
        leg(old, new, n, "old writes -> new replays", PORT),
        leg(new, old, n, "new writes -> old replays", PORT + 10),
        leg(new, new, n, "new writes -> new replays", PORT + 20),
        leg(old, old, n, "old writes -> old replays", PORT + 30),
    ]
    print("ALL PASS" if all(results) else "FAILURES PRESENT")
    sys.exit(0 if all(results) else 1)
