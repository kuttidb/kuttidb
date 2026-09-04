import os
import socket
import ssl
import stat
import struct
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kuttidb_client import KuttiDBClient, KuttiDBError, CAP_STREAM_BATCH


PORT = 7398
TOKEN = b"correct horse battery staple"


def wait_for_port(port, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), 0.1).close()
            return
        except OSError:
            time.sleep(0.03)
    raise RuntimeError("security-test server did not start")


def recv_exact(sock, size):
    out = b""
    while len(out) < size:
        part = sock.recv(size - len(out))
        if not part:
            raise ConnectionError("connection closed")
        out += part
    return out


def auth_socket(token=TOKEN):
    sock = socket.create_connection(("127.0.0.1", PORT), 2)
    sock.settimeout(2)
    sock.sendall(struct.pack("<BHI", 0x06, len(token), 0) + token)
    assert recv_exact(sock, 5) == b"\x00\x00\x00\x00\x00"
    return sock


def wait_for_tls(cert_path, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client = KuttiDBClient(
                port=PORT + 1, auth_token=TOKEN, tls=True,
                ca_file=cert_path, server_hostname="localhost")
            return client
        except (OSError, KuttiDBError, ssl.SSLError):
            time.sleep(0.04)
    raise RuntimeError("TLS security-test server did not start")


with tempfile.TemporaryDirectory(prefix="kuttidb-security-") as tmp:
    token_path = os.path.join(tmp, "auth.token")
    wal_path = os.path.join(tmp, "kuttidb.wal")
    unix_path = os.path.join(tmp, "cache.sock")
    with open(token_path, "wb") as token_file:
        token_file.write(TOKEN + b"\n")
    os.chmod(token_path, 0o600)

    # Exposing TCP beyond loopback without authentication must fail closed.
    refused = subprocess.run(
        ["./kuttidb", str(PORT), "-", "--bind", "0.0.0.0"],
        capture_output=True, timeout=5)
    assert refused.returncode != 0
    assert b"refusing non-loopback bind" in refused.stderr

    proc = subprocess.Popen(
        ["./kuttidb", str(PORT), wal_path, "50", unix_path, "0",
         "--auth-file", token_path, "--max-value-mb", "1",
         "--max-batch-mb", "2", "--max-clients", "32"],
        stderr=subprocess.PIPE, start_new_session=True)
    try:
        wait_for_port(PORT)

        # Normal commands are forbidden until AUTH succeeds.
        raw = socket.create_connection(("127.0.0.1", PORT), 2)
        raw.settimeout(2)
        raw.sendall(struct.pack("<BHI", 0x04, 0, 0))
        assert recv_exact(raw, 1) == b"\x02"
        raw.close()

        try:
            KuttiDBClient(port=PORT, auth_token=b"wrong")
            raise AssertionError("wrong token was accepted")
        except KuttiDBError:
            pass

        with KuttiDBClient(port=PORT, auth_token=TOKEN) as client:
            client.put("secure", b"value")
            assert client.get("secure") == b"value"
            assert client.health() is True
            assert client.capabilities()["major"] == 1
            assert client.capabilities()["features"] & CAP_STREAM_BATCH
            stats = client.stats()
            assert stats["auth_failures"] >= 2

        ctl = subprocess.run(
            [sys.executable, "./kuttidb-cli", "--port", str(PORT), "--auth-file", token_path,
             "put", "cli-secure", "works"], capture_output=True, timeout=5)
        assert ctl.returncode == 0, ctl.stderr
        ctl = subprocess.run(
            [sys.executable, "./kuttidb-cli", "--port", str(PORT), "--auth-file", token_path,
             "get", "cli-secure"], capture_output=True, timeout=5)
        assert ctl.returncode == 0 and ctl.stdout == b"works", ctl.stderr
        ctl = subprocess.run(
            [sys.executable, "./kuttidb-cli", "--port", str(PORT), "--auth-file", token_path,
             "health"], capture_output=True, timeout=5)
        assert ctl.returncode == 0, ctl.stderr

        # Empty batches complete immediately instead of underflowing state.
        raw = auth_socket()
        raw.sendall(struct.pack("<BHI", 0x11, 0, 0))
        assert recv_exact(raw, 1) == b"\x00"
        raw.sendall(struct.pack("<BHI", 0x12, 0, 0))
        assert recv_exact(raw, 4) == b"\x00\x00\x00\x00"
        raw.close()

        # Capability negotiation rejects a different protocol major without
        # accepting an ambiguous request shape.
        raw = auth_socket()
        raw.sendall(struct.pack("<BHIHH", 0x0A, 0, 4, 2, 0))
        assert recv_exact(raw, 5) == b"\x01\x00\x00\x00\x00"
        raw.close()

        # An oversized declaration is rejected before its body is allocated.
        raw = auth_socket()
        raw.sendall(struct.pack("<BHI", 0x01, 1, (1 << 20) + 1))
        assert raw.recv(1) == b""
        raw.close()

        assert stat.S_IMODE(os.stat(wal_path).st_mode) == 0o600
        assert stat.S_IMODE(os.stat(unix_path).st_mode) == 0o600
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    assert stat.S_IMODE(os.stat(wal_path + ".snap").st_mode) == 0o600

    features = subprocess.run(
        ["./kuttidb", "--features"], capture_output=True, text=True,
        timeout=5).stdout
    if "tls=openssl" in features:
        cert_path = os.path.join(tmp, "server.crt")
        key_path = os.path.join(tmp, "server.key")
        made_cert = subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
             "-keyout", key_path, "-out", cert_path, "-days", "1",
             "-subj", "/CN=localhost", "-addext",
             "subjectAltName=DNS:localhost,IP:127.0.0.1"],
            capture_output=True, timeout=20)
        assert made_cert.returncode == 0, made_cert.stderr
        os.chmod(key_path, 0o600)

        tls_proc = subprocess.Popen(
            ["./kuttidb", str(PORT + 1), "-", "100",
             "--auth-file", token_path, "--tls-cert", cert_path,
             "--tls-key", key_path], stderr=subprocess.PIPE,
            start_new_session=True)
        try:
            client = wait_for_tls(cert_path)
            with client:
                assert client.sock.version() in ("TLSv1.2", "TLSv1.3")
                client.put("encrypted", b"traffic")
                assert client.get("encrypted") == b"traffic"
                encrypted_items = [(f"tls-{i}", os.urandom(4096)) for i in range(512)]
                client.put_many(encrypted_items)
                assert client.get_many([k for k, _ in encrypted_items]) == [
                    v for _, v in encrypted_items]

            # A normal plaintext client cannot speak to a TLS-only TCP port.
            try:
                with KuttiDBClient(port=PORT + 1, timeout=1) as plain:
                    plain.stats()
                raise AssertionError("plaintext was accepted on the TLS listener")
            except (OSError, KuttiDBError):
                pass

            ctl = subprocess.run(
                [sys.executable, "./kuttidb-cli", "--port", str(PORT + 1),
                 "--tls", "--ca-file", cert_path, "--server-name", "localhost",
                 "--auth-file", token_path, "get", "encrypted"],
                capture_output=True, timeout=5)
            assert ctl.returncode == 0 and ctl.stdout == b"traffic", ctl.stderr
        finally:
            tls_proc.terminate()
            tls_proc.wait(timeout=10)

    # --- Prometheus scrape endpoint -------------------------------------
    # A non-loopback metrics bind without a token is refused, like the main
    # listener.
    refused_metrics = subprocess.run(
        ["./kuttidb", str(PORT), "-", "--bind", "127.0.0.1",
         "--metrics-bind", f"0.0.0.0:{PORT + 3}"], capture_output=True, timeout=5)
    assert refused_metrics.returncode != 0
    assert b"refusing non-loopback --metrics-bind" in refused_metrics.stderr

    metrics_token_path = os.path.join(tmp, "metrics.token")
    with open(metrics_token_path, "wb") as f:
        f.write(TOKEN)
    os.chmod(metrics_token_path, 0o600)
    metrics_port = PORT + 2
    metrics_proc = subprocess.Popen(
        ["./kuttidb", str(PORT), "-", "100",
         "--auth-file", token_path,
         "--queue-wal", os.path.join(tmp, "metrics.queues"),
         "--stream-wal", os.path.join(tmp, "metrics.streams"),
         "--metrics-bind", f"127.0.0.1:{metrics_port}",
         "--metrics-token-file", metrics_token_path],
        stderr=subprocess.PIPE, start_new_session=True)
    try:
        wait_for_port(metrics_port)

        def http_get(path, headers=()):
            conn = socket.create_connection(("127.0.0.1", metrics_port), 2)
            conn.settimeout(2)
            req = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n" + \
                "".join(h + "\r\n" for h in headers) + "Connection: close\r\n\r\n"
            conn.sendall(req.encode())
            out = b""
            while True:
                part = conn.recv(4096)
                if not part:
                    break
                out += part
            conn.close()
            return out

        # The scrape requires the configured bearer token.
        body = http_get("/metrics")
        assert body.startswith(b"HTTP/1.1 401"), body[:60]
        body = http_get("/metrics", ["Authorization: Bearer wrong"])
        assert body.startswith(b"HTTP/1.1 401")
        body = http_get("/metrics",
                        ["Authorization: Bearer " + TOKEN.decode()])
        assert body.startswith(b"HTTP/1.1 200")
        head, _, payload = body.partition(b"\r\n\r\n")
        assert b"text/plain; version=0.0.4" in head.split(b"\r\n")[1].lower()
        assert b"kuttidb_up 1" in payload
        assert b"kuttidb_ready 1" in payload
        assert b"kuttidb_cache_entries " in payload
        assert b"kuttidb_event_backend_info{backend=" in payload
        assert b"kuttidb_queue_wal_failed " in payload
        assert b"kuttidb_stream_topics " in payload
        assert b"kuttidb_auth_failures_total " in payload
        # Unknown paths are rejected and oversized requests dropped.
        assert http_get("/other",
                        ["Authorization: Bearer " + TOKEN.decode()]
                        ).startswith(b"HTTP/1.1 404")
        # Readiness/liveness endpoints honor the same token and report the
        # HEALTH durability contract.
        assert http_get("/ready").startswith(b"HTTP/1.1 401")
        ready = http_get("/ready", ["Authorization: Bearer " + TOKEN.decode()])
        assert ready.startswith(b"HTTP/1.1 200")
        live = http_get("/live", ["Authorization: Bearer " + TOKEN.decode()])
        assert live.startswith(b"HTTP/1.1 200")
        # Labeled per-queue and per-topic series appear once objects exist.
        with KuttiDBClient(port=PORT, auth_token=TOKEN) as mc:
            mc.queue_declare("billing.jobs", durable=False, max_depth=10)
            mc.queue_publish("billing.jobs", b"x" * 5)  # no consume: depth stays
            mc.stream_declare("metrics.topic", partitions=2)
            mc.stream_append("metrics.topic", b"r", partition=1)
            body = http_get("/metrics",
                            ["Authorization: Bearer " + TOKEN.decode()])
            assert body.startswith(b"HTTP/1.1 200")
            payload = body.partition(b"\r\n\r\n")[2]
            assert b'kuttidb_queue_depth{name="billing.jobs"} 1' in payload
            assert b'kuttidb_queue_inflight{name="billing.jobs"} 0' in payload
            assert b'kuttidb_topic_partitions{topic="metrics.topic"} 2' in payload
            assert b'kuttidb_topic_records{topic="metrics.topic"} 1' in payload
            assert b"kuttidb_queue_metrics_truncated" not in payload
        bad = socket.create_connection(("127.0.0.1", metrics_port), 2)
        bad.settimeout(2)
        bad.sendall(b"GET /" + b"a" * 9000 + b" HTTP/1.1\r\n\r\n")
        try:
            assert bad.recv(1) == b""
        except ConnectionResetError:
            pass  # dropping the connection is the expected rejection
        bad.close()
    finally:
        metrics_proc.terminate()
        metrics_proc.wait(timeout=10)

print("SECURITY TESTS PASSED")
