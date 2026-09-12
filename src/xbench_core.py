#!/usr/bin/env python3
"""Shared measurement core for the cross-server queue comparison.

Every system runs the same logical workload through one Python process:
publish N 100-byte messages into one durable queue, then drain all N with an
explicit consumer acknowledgement, verifying the count. Each system is
configured for the same durability tier so the acknowledgements being
compared mean the same thing. Server cost is read from /proc for the whole
server process tree, so it does not depend on the client library.
"""
import json
import os
import resource
import signal
import subprocess
import time
from pathlib import Path

CLK = os.sysconf("SC_CLK_TCK")
VALUE = b"x" * 100


def tree_pids(root):
    """The server's whole process tree: brokers on the JVM or BEAM fork."""
    found, frontier = {root}, [root]
    while frontier:
        parent = frontier.pop()
        try:
            kids = Path(f"/proc/{parent}/task").glob("*/children")
            for child_file in kids:
                for pid in child_file.read_text().split():
                    pid = int(pid)
                    if pid not in found:
                        found.add(pid)
                        frontier.append(pid)
        except OSError:
            pass
    return found


def usage(root):
    """CPU seconds, current and peak RSS, and bytes written, summed over the
    tree. Returns None if nothing could be read: a missing measurement is
    never reported as zero."""
    total = {"cpu_s": 0.0, "rss_kib": 0, "peak_rss_kib": 0, "write_bytes": 0}
    seen = 0
    for pid in tree_pids(root):
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            status = dict(l.split(":", 1) for l in
                          Path(f"/proc/{pid}/status").read_text().splitlines() if ":" in l)
            total["cpu_s"] += (int(stat[11]) + int(stat[12])) / CLK
            total["rss_kib"] += int(status["VmRSS"].split()[0])
            total["peak_rss_kib"] += int(status["VmHWM"].split()[0])
            try:
                io = dict(l.split(":", 1) for l in
                          Path(f"/proc/{pid}/io").read_text().splitlines() if ":" in l)
                total["write_bytes"] += int(io["write_bytes"])
            except (OSError, KeyError):
                pass
            seen += 1
        except (OSError, KeyError, IndexError, ValueError):
            continue
    return total if seen else None


def dir_bytes(path):
    try:
        return int(subprocess.check_output(["du", "-sb", str(path)], text=True).split()[0])
    except (OSError, subprocess.CalledProcessError, ValueError):
        return None


def phase(root, count, operation):
    """Run one phase and attribute wall time, client CPU and server cost."""
    before = usage(root)
    client_before = resource.getrusage(resource.RUSAGE_SELF)
    start = time.perf_counter()
    done = operation()
    elapsed = time.perf_counter() - start
    client_after = resource.getrusage(resource.RUSAGE_SELF)
    after = usage(root)
    if done != count:
        raise RuntimeError(f"workload incomplete: {done}/{count}")
    client_cpu = ((client_after.ru_utime - client_before.ru_utime) +
                  (client_after.ru_stime - client_before.ru_stime))
    out = {"count": count, "seconds": elapsed, "messages_per_second": count / elapsed,
           "client_cpu_s_per_million": client_cpu * 1e6 / count}
    if before and after:
        out["server_cpu_s_per_million"] = (after["cpu_s"] - before["cpu_s"]) * 1e6 / count
        out["server_write_bytes_per_message"] = (
            (after["write_bytes"] - before["write_bytes"]) / count)
        out["server_rss_kib"] = after["rss_kib"]
        out["server_peak_rss_kib"] = after["peak_rss_kib"]
    return out


def wait_for(check, seconds, what, proc=None):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"{what}: server exited rc={proc.returncode}")
        try:
            if check():
                return
        except Exception as error:          # a broker that is not listening yet
            last = error
        time.sleep(0.2)
    raise RuntimeError(f"{what}: not ready after {seconds}s ({last})")


def stop(proc, hard_after=20):
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=hard_after)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def record(path, row):
    with open(path, "a") as out:
        out.write(json.dumps(row) + "\n")
