"""Repeatable fresh-server KuttiDB performance matrix.

Reports throughput, batch p50/p95/p99 latency, server-reported live/allocated
bytes, and RSS. Each case uses a fresh server so scaling results are not biased
by earlier datasets.
"""

import argparse
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient


def wait_port(port):
    deadline = time.time() + 8
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), 0.1).close()
            return
        except OSError:
            time.sleep(0.02)
    raise RuntimeError("benchmark server did not start")


def rss_kib(pid):
    try:
        return int(subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "rss="], text=True).strip())
    except (OSError, ValueError, subprocess.CalledProcessError):
        return -1


def run_case(port, clients, per, batch, value, durability, tmp):
    wal = "-" if durability == "off" else os.path.join(
        tmp, f"{clients}-{batch}-{value}.wal")
    command = [os.path.join(ROOT, "kuttidb"), str(port), wal, "100",
               "--threads", "4"]
    if durability != "off":
        command += ["--durability", durability]
    server = subprocess.Popen(command, stderr=subprocess.DEVNULL,
                              start_new_session=True)
    try:
        wait_port(port)
        idle_rss = rss_kib(server.pid)
        # Multi-client scaling is measured with independent one-thread client
        # processes: in-process benchmark threads contend on client-side
        # process resources and understate server throughput (see
        # BENCHMARKS.md, client-scaling table).
        procs = []
        for index in range(clients):
            procs.append(subprocess.Popen(
                [os.path.join(ROOT, "kuttidb-bench"), str(port), "1", str(per),
                 str(batch), str(value)],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True))
        outputs = [p.communicate(timeout=180)[0].strip() for p in procs]
        for pr in procs:
            if pr.returncode != 0:
                raise RuntimeError("benchmark client failed")
        rates = [float(re.search(r"= ([0-9.]+) ops/s", o).group(1)) for o in outputs]
        p50s = [float(re.search(r"p50=([0-9.]+) us", o).group(1)) for o in outputs]
        p95s = [float(re.search(r"p95=([0-9.]+) us", o).group(1)) for o in outputs]
        p99s = [float(re.search(r"p99=([0-9.]+) us", o).group(1)) for o in outputs]
        total = sum(rates)
        # Latency: worst-case tail across clients, median-typical p50 as the mean.
        p50 = sum(p50s) / len(p50s)
        p95 = max(p95s)
        p99 = max(p99s)
        ops_s = int(total)
        with KuttiDBClient(port=port) as client:
            stats = client.stats()
        return {
            "clients": clients, "batch": batch, "value": value,
            "ops_s": ops_s, "idle_rss_kib": idle_rss,
            "loaded_rss_kib": rss_kib(server.pid),
            "live_bytes": stats["mem_bytes"],
            "allocated_bytes": stats["allocated_bytes"],
            "p50_us": round(p50, 1),
            "p95_us": round(p95, 1),
            "p99_us": round(p99, 1),
        }
    finally:
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            server.kill(); server.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=7410)
    parser.add_argument("--durability", choices=("off", "periodic", "always"),
                        default="off")
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()

    scaling_per = 25_000 if args.quick else 100_000
    cases = [(n, scaling_per, 256, 100) for n in (1, 2, 4, 8)]
    if not args.quick:
        cases += [
            (4, 20_000, 1, 100), (4, 40_000, 16, 100),
            (4, 60_000, 64, 100),
            (4, 50_000, 64, 16), (4, 40_000, 64, 1024),
            (4, 4_000, 32, 9000),
        ]

    print("clients,batch,value_bytes,ops_s,p50_us,p95_us,p99_us,idle_rss_kib,loaded_rss_kib,live_bytes,allocated_bytes")
    results = []
    with tempfile.TemporaryDirectory(prefix="kuttidb-bench-") as tmp:
        for index, case in enumerate(cases):
            result = run_case(args.port + index, *case, args.durability, tmp)
            results.append(result)
            print(",".join(str(result[name]) for name in (
                "clients", "batch", "value", "ops_s", "p50_us", "p95_us", "p99_us",
                "idle_rss_kib", "loaded_rss_kib", "live_bytes",
                "allocated_bytes")), flush=True)

    scaling = results[:4]
    one = scaling[0]["ops_s"]
    four = scaling[2]["ops_s"]
    if args.durability == "off" and four < one * 1.35:
        raise SystemExit("scaling gate failed: four clients did not beat one by 35%")
    if max(row["idle_rss_kib"] for row in results) > 10 * 1024:
        raise SystemExit("footprint gate failed: idle RSS exceeded 10 MiB")


if __name__ == "__main__":
    main()
