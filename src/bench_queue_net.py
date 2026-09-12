#!/usr/bin/env python3
"""Verified single-client queue benchmark; timings include client validation.

python3 src/bench_queue_net.py 7411 200000 --data-dir /path/on/ssd --repeats 5
Use --binary to compare builds with the exact same harness. JSONL output retains
all repetitions, batch latency, process CPU deltas, RSS and binary identity.
Durable publish, delivery and ACK each wait for the server's fsync-covered reply.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from kuttidb_client import KuttiDBClient


def process_usage(pid):
    """Linux server counters; never substitute missing measurements with zero."""
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        status = dict(line.split(":", 1) for line in
                      Path(f"/proc/{pid}/status").read_text().splitlines())
        io = dict(line.split(":", 1) for line in
                  Path(f"/proc/{pid}/io").read_text().splitlines())
        return {"cpu_s": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                "rss_kib": int(status["VmRSS"].split()[0]),
                "peak_rss_kib": int(status["VmHWM"].split()[0]),
                "write_bytes": int(io["write_bytes"])}
    except (OSError, KeyError, ValueError):
        return None


def timed_phase(pid, count, operation):
    before = process_usage(pid)
    cpu = time.process_time()
    start = time.perf_counter()
    latencies = operation()
    elapsed = time.perf_counter() - start
    client_cpu = time.process_time() - cpu
    after = process_usage(pid)
    result = {"count": count, "seconds": elapsed, "messages_per_second": count / elapsed,
              "client_cpu_s": client_cpu, "client_cpu_s_per_million": client_cpu * 1e6 / count,
              "server_after": after}
    ordered = sorted(latencies)
    for q in (50, 95, 99):
        result[f"batch_p{q}_us"] = ordered[min(len(ordered) - 1, (len(ordered) * q + 99) // 100 - 1)] * 1e6
    if before and after:
        used = after["cpu_s"] - before["cpu_s"]
        result.update(server_cpu_s=used, server_cpu_s_per_million=used * 1e6 / count,
                      server_write_bytes=after["write_bytes"] - before["write_bytes"])
    return result


def wait_ready(server, port):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError("benchmark server exited before readiness")
        try:
            with KuttiDBClient(port=port, timeout=0.2) as db:
                db.health()
            return
        except (OSError, RuntimeError):
            time.sleep(0.02)
    raise RuntimeError("benchmark server did not start")


@contextmanager
def benchmark_directory(parent):
    path = tempfile.mkdtemp(prefix="kuttidb-queue-bench-", dir=parent)
    try:
        yield path
    except BaseException:
        print(f"Failed run: retaining server log and WALs in {path}", file=sys.stderr)
        raise
    else:
        shutil.rmtree(path)


def run(args, repeat):
    # Refuse an occupied endpoint rather than accidentally benchmarking an
    # unrelated server. Recheck child liveness after protocol readiness.
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", args.port))
    with benchmark_directory(args.data_dir) as tmp:
        command = [args.binary, str(args.port), str(Path(tmp) / "cache.wal"),
                   str(args.fsync_ms), "--threads", str(args.threads)]
        with open(Path(tmp) / "server.log", "w+") as log:
            server = subprocess.Popen(command, stderr=log, stdout=log)
            result = {}
            try:
                wait_ready(server, args.port)
                if server.poll() is not None:
                    raise RuntimeError("benchmark server failed to bind")
                result = {"repeat": repeat, "command": command, "idle": process_usage(server.pid)}
                with KuttiDBClient(port=args.port, timeout=30) as db:
                    db.queue_declare("bench", durable=True)
                    db.queue_prefetch(args.batch * 4)
                    value = b"x" * args.value_size
                    ids = []

                    def publish():
                        latencies = []
                        for start in range(0, args.count, args.batch):
                            n = min(args.batch, args.count - start)
                            t = time.perf_counter()
                            published = db.queue_publish_batch("bench", [value] * n)
                            if len(published) != n:
                                raise RuntimeError("publish count mismatch")
                            ids.extend(published)
                            latencies.append(time.perf_counter() - t)
                        return latencies

                    result["publish"] = timed_phase(server.pid, args.count, publish)
                    if len(set(ids)) != args.count:
                        raise RuntimeError("duplicate published message IDs")
                    state = db.queue_stats("bench")
                    if state["depth"] != args.count or state["inflight"] != 0:
                        raise RuntimeError(f"incorrect published queue state: {state}")

                    def consume():
                        latencies = []
                        got = 0
                        while got < args.count:
                            t = time.perf_counter()
                            messages = db.queue_consume_batch("bench", args.batch)
                            if not messages:
                                raise RuntimeError(f"partial drain: {got}/{args.count}")
                            for index, message in enumerate(messages):
                                if (got + index >= len(ids) or message["message_id"] != ids[got + index]
                                        or message["value"] != value or message["delivery_count"] != 1
                                        or message["redelivered"]):
                                    raise RuntimeError("delivery ID/order/payload/redelivery mismatch")
                            if db.queue_ack_batch("bench", [m["id"] for m in messages]) != len(messages):
                                raise RuntimeError("ACK count mismatch")
                            got += len(messages)
                            result["consumed_count"] = got
                            latencies.append(time.perf_counter() - t)
                        return latencies

                    result["consume_ack"] = timed_phase(server.pid, args.count, consume)
                    state = db.queue_stats("bench")
                    if state["depth"] != 0 or state["inflight"] != 0:
                        raise RuntimeError(f"queue did not fully drain: {state}")
                    if args.single_count:
                        def singles():
                            latencies = []
                            for _ in range(args.single_count):
                                t = time.perf_counter()
                                db.queue_publish("bench", value)
                                latencies.append(time.perf_counter() - t)
                            return latencies
                        result["publish_single"] = timed_phase(server.pid, args.single_count, singles)
                    result["stats"] = db.stats()
                    if result["stats"].get("queue_wal_failed"):
                        raise RuntimeError("queue persistence failed")
                    result["verified"] = True
                return result
            except Exception as error:
                diagnostic = {"error": str(error), "server_exit_code_before_cleanup": server.poll(),
                              "server_usage": process_usage(server.pid), "directory": tmp,
                              "partial_result": result}
                print(json.dumps(diagnostic), file=sys.stderr)
                Path(tmp, "failure.json").write_text(json.dumps(diagnostic) + "\n")
                log.flush()
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("port", type=int, nargs="?", default=7411)
    parser.add_argument("count", type=int, nargs="?", default=20000)
    parser.add_argument("--binary", default=str(ROOT / "kuttidb"))
    parser.add_argument("--data-dir", default=tempfile.gettempdir())
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--batch", type=int, default=256)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--value-size", type=int, default=100)
    parser.add_argument("--single-count", type=int, default=2000)
    parser.add_argument("--fsync-ms", type=int, default=100,
                        help="cache WAL periodic fsync interval; the queue WAL "
                             "is always fsync-covered regardless of this value")
    parser.add_argument("--jsonl", help="Create a new result file; refuses to overwrite prior evidence")
    args = parser.parse_args()
    if (args.count < 1 or args.repeats < 1 or not 1 <= args.batch <= 256
            or not 1 <= args.threads <= 64 or args.value_size < 1
            or args.single_count < 0 or args.fsync_ms < 1):
        parser.error("invalid count, repeats, batch, threads, value size, "
                     "single count or fsync interval")
    args.binary = str(Path(args.binary).resolve())
    args.data_dir = str(Path(args.data_dir).resolve(strict=True))
    filesystem = None
    if platform.system() == "Linux":
        filesystem = subprocess.check_output(["stat", "-f", "-c", "%T", args.data_dir], text=True).strip()
        if filesystem in ("tmpfs", "ramfs"):
            parser.error("durable benchmarks require disk storage; choose --data-dir on the SSD")
    metadata = {"platform": platform.platform(), "cpus": os.cpu_count(), "filesystem": filesystem,
                "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
                "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "args": vars(args), "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "features": subprocess.check_output([args.binary, "--features"], text=True).strip(),
                "durability": "fsync-covered publish/delivery/ack; single client; no TLS",
                "rss_note": "Linux VmHWM is lifetime peak, not per-phase peak; CPU tick resolution applies"}
    output = open(args.jsonl, "x") if args.jsonl else None
    try:
        results = []
        for repeat in range(1, args.repeats + 1):
            try:
                result = run(args, repeat)
            except Exception as error:
                if output:
                    output.write(json.dumps({"repeat": repeat, "verified": False,
                                             "error": repr(error), "metadata": metadata}) + "\n")
                    output.flush()
                raise
            result["metadata"] = metadata
            if output:
                output.write(json.dumps(result) + "\n")
                output.flush()
            results.append(result)
            print(f"run {repeat}: publish={result['publish']['messages_per_second']:.0f} "
                  f"consume+ack={result['consume_ack']['messages_per_second']:.0f} msgs/s; verified", flush=True)
        for phase in ("publish", "consume_ack", "publish_single"):
            rates = [r[phase]["messages_per_second"] for r in results if phase in r]
            if rates:
                print(f"{phase}: median={statistics.median(rates):.0f}, min={min(rates):.0f}, max={max(rates):.0f} msgs/s")
    finally:
        if output:
            output.close()


if __name__ == "__main__":
    main()
