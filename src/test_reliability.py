import concurrent.futures
import os
import resource
import shutil
import signal
import socket
import subprocess
import struct
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "clients"))

from kuttidb_client import KuttiDBClient, KuttiDBError
from kuttidb_embed import KuttiEmbed


def wait_for_port(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), 0.1).close()
            return
        except OSError:
            time.sleep(0.03)
    raise RuntimeError(f"server on {port} did not start")


def start_server(port, wal, *extra, preexec_fn=None):
    proc = subprocess.Popen(
        [os.path.join(ROOT, "kuttidb"), str(port), wal, "50", *extra],
        stderr=subprocess.DEVNULL, start_new_session=True, preexec_fn=preexec_fn)
    wait_for_port(port)
    return proc


def embedded_crash_writer(path):
    db = KuttiEmbed(path)
    value = b"c" * (4 << 20)
    while True:
        db.put("crash-key", value)


if len(sys.argv) == 3 and sys.argv[1] == "--crash-writer":
    embedded_crash_writer(sys.argv[2])
    raise SystemExit(0)


tmp = tempfile.mkdtemp(prefix="kuttidb-reliability-")
procs = []
try:
    # Absolute expirations must not be extended by crash downtime.
    wal = os.path.join(tmp, "ttl.wal")
    proc = start_server(7401, wal)
    procs.append(proc)
    with KuttiDBClient(port=7401) as client:
        client.put("deadline", b"value", ttl=1.0)
    time.sleep(0.2)  # periodic WAL flush
    proc.kill(); proc.wait(); procs.remove(proc)
    time.sleep(1.1)
    proc = start_server(7401, wal)
    procs.append(proc)
    with KuttiDBClient(port=7401) as client:
        assert client.get("deadline") is None, "TTL was extended across downtime"
    proc.terminate(); proc.wait(); procs.remove(proc)

    # Legacy headerless snapshots remain readable after the CSN2 upgrade.
    legacy_wal = os.path.join(tmp, "legacy.wal")
    key, value = b"legacy", b"snapshot-value"
    with open(legacy_wal + ".snap", "wb") as legacy:
        legacy.write(struct.pack("<HI", len(key), len(value)) + key + value)
    open(legacy_wal, "wb").close()
    proc = start_server(7401, legacy_wal)
    procs.append(proc)
    with KuttiDBClient(port=7401) as client:
        assert client.get("legacy") == value
    proc.terminate(); proc.wait(); procs.remove(proc)

    # The recovered final value must match the live ordering under contention.
    order_wal = os.path.join(tmp, "order.wal")
    proc = start_server(7401, order_wal)
    procs.append(proc)

    def writer(worker):
        with KuttiDBClient(port=7401, timeout=20) as client:
            for seq in range(250):
                client.put("contended", f"{worker}:{seq}".encode())

    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        list(pool.map(writer, range(8)))
    with KuttiDBClient(port=7401) as client:
        live = client.get("contended")
    time.sleep(0.2)
    proc.kill(); proc.wait(); procs.remove(proc)
    proc = start_server(7401, order_wal)
    procs.append(proc)
    with KuttiDBClient(port=7401) as client:
        assert client.get("contended") == live, "WAL order diverged from live order"
    proc.terminate(); proc.wait(); procs.remove(proc)

    # A runtime WAL failure must fail acknowledgements and latch the writer shut.
    fail_wal = os.path.join(tmp, "fail.wal")

    def limit_wal():
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_FSIZE, (4096, 4096))

    proc = start_server(7401, fail_wal, "--durability", "always",
                        preexec_fn=limit_wal)
    procs.append(proc)
    failed = False
    try:
        with KuttiDBClient(port=7401, timeout=10) as client:
            client.put_many((f"fail-{i}", b"x" * 200) for i in range(100))
    except KuttiDBError:
        failed = True
    assert failed, "WAL write failure was acknowledged as success"
    with KuttiDBClient(port=7401) as client:
        assert client.stats()["wal_failed"] == 1
    proc.kill(); proc.wait(); procs.remove(proc)

    # A failing snapshot must not corrupt or lose data: the shutdown snapshot
    # is written to a temp file and renamed, so a failure leaves the WAL as
    # the complete source of truth and the server exits cleanly.
    snap_wal = os.path.join(tmp, "snap", "snap.wal")
    os.makedirs(os.path.join(tmp, "snap"))
    proc = start_server(7403, snap_wal, "--durability", "always")
    procs.append(proc)
    with KuttiDBClient(port=7403) as client:
        client.put_many((f"snap-{i}", b"s" * 100) for i in range(50))
    # Root deliberately bypasses directory mode bits, so a root-run Docker
    # test cannot inject this write failure with chmod.  Keep the recovery
    # part of the test in that environment, while exercising the failure and
    # cleanup assertions everywhere a real permission denial is possible.
    permission_injection = os.geteuid() != 0
    if permission_injection:
        os.chmod(os.path.join(tmp, "snap"), 0o555)  # snapshot creation will fail
    proc.terminate(); proc.wait(timeout=5); procs.remove(proc)
    if permission_injection:
        assert not os.path.exists(snap_wal + ".snap"), \
            "failed snapshot must not leave a snapshot file"
        assert not os.path.exists(snap_wal + ".snap.tmp"), \
            "failed snapshot must not leave temp files"
        os.chmod(os.path.join(tmp, "snap"), 0o755)
    proc = start_server(7403, snap_wal, "--durability", "always")
    procs.append(proc)
    with KuttiDBClient(port=7403) as client:
        assert all(client.get(f"snap-{i}") == b"s" * 100 for i in range(50)), \
            "data lost after a failed snapshot"
        client.put("snap-final", b"ok")
    proc.terminate(); proc.wait(timeout=5); procs.remove(proc)
    assert os.path.exists(snap_wal + ".snap"), \
        "snapshot must succeed once the directory is writable again"
    proc = start_server(7403, snap_wal, "--durability", "always")
    procs.append(proc)
    with KuttiDBClient(port=7403) as client:
        assert client.get("snap-final") == b"ok", \
            "snapshot + wal recovery broken"
    proc.terminate(); proc.wait(timeout=5); procs.remove(proc)

    # Mapping refcounts, allocator reuse, and owner-death recovery.
    embed = os.path.join(tmp, "db.embed")
    proc = start_server(7402, "-", "", "0", embed,
                        "--embed-region-mb", "32")
    procs.append(proc)
    first = KuttiEmbed(embed)
    second = KuttiEmbed(embed)
    first.put("shared", b"one")
    assert second.get("shared") == b"one"
    first.close()
    second.put("after-close", b"still mapped")
    assert second.get("after-close") == b"still mapped"

    for size in (70_000, 120_000, 260_000, 9000):
        value = bytes([size & 0xFF]) * size
        second.put("varying", value)
        assert second.get("varying") == value
        assert second.delete("varying")

    child = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "--crash-writer", embed],
        stderr=subprocess.DEVNULL)
    time.sleep(0.08)
    child.kill(); child.wait(timeout=5)
    second.put("crash-key", b"recovered")
    assert second.get("crash-key") == b"recovered"
    second.close()
    proc.terminate(); proc.wait(); procs.remove(proc)

    print("RELIABILITY + CRASH TESTS PASSED")
finally:
    for proc in procs:
        try:
            proc.kill(); proc.wait(timeout=2)
        except Exception:
            pass
    shutil.rmtree(tmp, ignore_errors=True)
