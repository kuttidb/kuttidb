"""Managed-local lifecycle integration tests.

The test intentionally uses deadlines instead of fixed server-start sleeps.
Some restricted CI sandboxes prohibit AF_UNIX bind entirely; that environment
cannot exercise a Unix-only managed service, so it reports a clear skip rather
than turning a host policy into a product failure.
"""

from __future__ import annotations

import concurrent.futures
import errno
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from kuttidb_client import KuttiDBClient, KuttiDBError  # noqa: E402

SERVER = os.environ.get("KUTTIDB_SERVER", str(ROOT / "kuttidb"))


def unix_supported() -> bool:
    root = Path(tempfile.mkdtemp(prefix="kuttidb-unix-"))
    path = root / "probe.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(str(path))
        return True
    except OSError as error:
        if error.errno in {errno.EPERM, errno.EACCES}:
            return False
        raise
    finally:
        sock.close()
        shutil.rmtree(root, ignore_errors=True)


def ensure(data_dir: Path, timeout: float = 8.0) -> dict:
    result = subprocess.run(
        [SERVER, "ensure", "--data-dir", str(data_dir), "--listen",
         f"unix:{data_dir / 'kuttidb.sock'}", "--idle-timeout-ms", "250",
         "--startup-orphan-timeout-ms", "2000", "--startup-timeout-ms",
         str(int(timeout * 1000)), "--json"],
        capture_output=True, timeout=timeout + 2, check=False,
    )
    if result.returncode:
        raise AssertionError(f"ensure failed with {result.returncode}: {result.stderr[:512]!r}")
    return json.loads(result.stdout)


def ensure_raw(data_dir: Path, *extra: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [SERVER, "ensure", "--data-dir", str(data_dir), "--listen",
         f"unix:{data_dir / 'kuttidb.sock'}", "--idle-timeout-ms", "250",
         "--startup-orphan-timeout-ms", "2000", "--startup-timeout-ms",
         "3000", *extra, "--json"],
        capture_output=True, timeout=5, check=False,
    )


def eventually(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.025)
    return predicate()


def free_loopback_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def tcp_absent(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.05):
            return False
    except OSError:
        return True


def main() -> None:
    if not unix_supported():
        print("SKIP: host policy denies Unix-domain socket bind")
        return
    root = Path(tempfile.mkdtemp(prefix="kuttidb-managed-"))
    try:
        # Sixty-four independent launchers must converge on one instance ID.
        with concurrent.futures.ThreadPoolExecutor(64) as executor:
            outcomes = list(executor.map(lambda _: ensure(root), range(64)))
        assert {item["instance_id"] for item in outcomes} == {(root / "instance.id").read_text().strip()}

        with KuttiDBClient.managed(data_dir=str(root), executable=SERVER,
                                   idle_timeout=0.25, startup_timeout=5) as client:
            client.put("managed", b"value")
            assert client.get("managed") == b"value"
            stats = client.stats()
            assert stats["lifecycle"] == "managed-idle"
            assert stats["lifecycle_state"] == "running"
            assert stats["managed_connections"] >= 1
            assert "managed_idle_remaining_ms" in stats
            client.queue_declare("managed-queue")
            client.queue_publish("managed-queue", b"job")
            job = client.queue_consume("managed-queue")
            assert job and job["value"] == b"job"
            assert client.queue_ack("managed-queue", job["id"])
            client.stream_declare("managed-stream")
            client.stream_append("managed-stream", b"record")
        assert eventually(lambda: not (root / "kuttidb.sock").exists())

        # Recovered server keeps the same instance identity and durable state.
        original_id = (root / "instance.id").read_text().strip()
        with KuttiDBClient.managed(data_dir=str(root), executable=SERVER,
                                   idle_timeout=0.25, startup_timeout=5) as client:
            assert client.get("managed") == b"value"
            assert client.stream_fetch("managed-stream")[0]["value"] == b"record"
        assert (root / "instance.id").read_text().strip() == original_id

        # Repeated close/reopen cycles exercise the socket-unlink versus
        # ownership-lock hand-off at the idle-shutdown boundary.
        for _ in range(4):
            assert eventually(lambda: not (root / "kuttidb.sock").exists())
            with KuttiDBClient.managed(data_dir=str(root), executable=SERVER,
                                       idle_timeout=0.25, startup_timeout=5) as client:
                assert client.get("managed") == b"value"

        # Rotation occurs only while the bootstrap lock is held, before the
        # replacement server is spawned. It retains exactly one private log.
        assert eventually(lambda: not (root / "kuttidb.sock").exists())
        (root / "kuttidb.log").write_bytes(b"x" * (5 * 1024 * 1024 + 1))
        with KuttiDBClient.managed(data_dir=str(root), executable=SERVER,
                                   idle_timeout=0.25, startup_timeout=5):
            assert (root / "kuttidb.log.1").is_file()

        # A configuration that makes server startup fail is remembered for a
        # short period. A second caller receives the same categorized failure
        # rather than serially forking another identical broken server.
        bad_root = root / "bad-startup"
        first_bad = ensure_raw(bad_root, "--auth-file", str(bad_root / "missing-token"))
        second_bad = ensure_raw(bad_root, "--auth-file", str(bad_root / "missing-token"))
        assert first_bad.returncode == second_bad.returncode == 67
        cooldown = (bad_root / ".startup-failure").read_text(encoding="ascii")
        assert "format=1" in cooldown and "status=67" in cooldown

        # Explicit advanced managed TCP remains local-only and retains the
        # same identity and idle lifecycle semantics as the Unix default.
        tcp_root = root / "tcp"
        tcp_port = free_loopback_port()
        with KuttiDBClient.managed(data_dir=str(tcp_root), transport="tcp",
                                   host="127.0.0.1", port=tcp_port,
                                   executable=SERVER, idle_timeout=0.25,
                                   startup_timeout=5) as client:
            client.put("tcp-managed", b"value")
            assert client.get("tcp-managed") == b"value"
        # A TCP probe is itself a lease, so do not poll by connecting while
        # testing idle shutdown.  Wait beyond the grace period, then probe once.
        time.sleep(0.6)
        assert tcp_absent(tcp_port)

        # The typed advanced settings are forwarded by `ensure`, not merely
        # accepted by the SDK.  Authentication, a smaller value ceiling,
        # fsync tuning, and explicit queue/stream WAL locations all exercise
        # distinct server configuration paths.
        advanced_root = root / "advanced"
        token_file = advanced_root / "auth.token"
        advanced_root.mkdir(mode=0o700)
        token_file.write_text("managed-test-token\n", encoding="ascii")
        token_file.chmod(0o600)
        queue_wal = advanced_root / "queue.override.wal"
        stream_wal = advanced_root / "stream.override.wal"
        with KuttiDBClient.managed(data_dir=str(advanced_root), executable=SERVER,
                                   auth_token="managed-test-token", auth_file=str(token_file),
                                   max_value_mb=1, max_batch_mb=1, max_clients=8,
                                   threads=1, fsync_ms=0, queue_wal=str(queue_wal),
                                   stream_wal=str(stream_wal), idle_timeout=0.25,
                                   startup_timeout=5) as client:
            client.put("advanced", b"ok")
            client.queue_declare("advanced-queue")
            client.queue_publish("advanced-queue", b"job")
            client.stream_declare("advanced-stream")
            client.stream_append("advanced-stream", b"record")
            try:
                client.put("too-large", b"x" * (2 << 20))
                raise AssertionError("max_value_mb was not applied")
            # The wire parser defensively tears down an oversized frame; that
            # is also a proof that the configured one-megabyte ceiling won.
            except (KuttiDBError, OSError):
                pass
        assert queue_wal.is_file() and stream_wal.is_file()
        print("managed server integration tests passed")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
