"""Python client for the KuttiDB server."""

import errno
import ipaddress
import json
import os
from dataclasses import dataclass
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import stat
from typing import NamedTuple

OP_PUT = 0x01
OP_GET = 0x02
OP_DELETE = 0x03
OP_STATS = 0x04
OP_PUT_TTL = 0x05
OP_AUTH = 0x06
OP_HEALTH = 0x09
OP_CAPABILITIES = 0x0a
OP_SERVER_INFO = 0x0c
OP_PUT_BATCH = 0x11
OP_GET_BATCH = 0x12
OP_PUT_BATCH_TTL = 0x13
OP_QUEUE_DECLARE = 0x20
OP_QUEUE_PUBLISH = 0x21
OP_QUEUE_CONSUME = 0x22
OP_QUEUE_ACK = 0x23
OP_QUEUE_NACK = 0x24
OP_QUEUE_PUBLISH_TTL = 0x25
OP_QUEUE_STATS = 0x26
OP_QUEUE_PREFETCH = 0x27
OP_QUEUE_CANCEL = 0x28
OP_QUEUE_CONSUMER_REGISTER = 0x29
OP_QUEUE_CONSUMER_UNREGISTER = 0x2A
OP_QUEUE_CONSUME_AS = 0x2B
OP_EXCHANGE_DECLARE = 0x30
OP_EXCHANGE_BIND = 0x31
OP_EXCHANGE_UNBIND = 0x32
OP_EXCHANGE_PUBLISH = 0x33
OP_ATOMIC_PUT_PUBLISH = 0x40
OP_ATOMIC_PUT_ENQUEUE = 0x41
OP_ATOMIC_DELETE_PUBLISH = 0x42
OP_ATOMIC_UPDATE_EMIT = 0x43
OP_SF_GET_OR_CLAIM = 0x50
OP_SF_WAIT_FOR_KEY = 0x51
OP_SF_PUT_AND_RELEASE = 0x52
OP_SF_RELEASE_CLAIM = 0x53
OP_SF_GET_OR_REFRESH = 0x54
OP_PUT_SWR = 0x0B
OP_STREAM_DECLARE = 0x60
OP_STREAM_APPEND = 0x61
OP_STREAM_FETCH = 0x62
OP_STREAM_COMMIT = 0x63
OP_STREAM_GROUP_OFFSET = 0x64
OP_STREAM_GROUP_JOIN = 0x65
OP_STREAM_GROUP_LAG = 0x66
OP_STREAM_APPEND_BATCH = 0x67
OP_STREAM_GROUP_LEAVE = 0x68
OP_STREAM_LIST = 0x69
OP_STREAM_GROUP_LIST = 0x6a
OP_STREAM_COMMIT_BATCH = 0x6b
OP_STREAM_FETCH_KEYS = 0x6c
OP_QUEUE_LIST = 0x2c
OP_QUEUE_PUBLISH_BATCH = 0x2d
OP_QUEUE_CONSUME_BATCH = 0x2e
OP_QUEUE_ACK_BATCH = 0x2f

EXCHANGE_TYPES = {"direct": 0, "fanout": 1, "topic": 2}

ST_OK = 0x00
ST_MISS = 0x01
ST_ERR = 0x02

MAX_KEY = (1 << 16) - 1
MAX_VALUE = 64 << 20
BATCH_SIZE = 256
PROTOCOL_MAJOR = 1
PROTOCOL_MINOR = 8

CAP_CACHE = 1 << 0
CAP_QUEUES = 1 << 1
CAP_EXCHANGES = 1 << 2
CAP_ATOMIC = 1 << 3
CAP_SINGLEFLIGHT = 1 << 4
CAP_STREAMS = 1 << 5
CAP_STREAM_BATCH = 1 << 6
CAP_HEALTH = 1 << 7
CAP_STREAM_GEN = 1 << 8
CAP_QUEUE_CONSUMERS = 1 << 9
CAP_ATOMIC_UPDATE = 1 << 10
CAP_SWR = 1 << 11
CAP_STREAM_KEYS = 1 << 14
CAP_SERVER_INFO = 1 << 15


class StreamAssignment(NamedTuple):
    """Result of joining or heartbeating a stream consumer group.

    ``partitions`` is this connection's deterministic assignment and
    ``generation`` increases on every membership change (join, graceful
    leave, disconnect, lease expiry). A generation change with a changed
    assignment means the member must finish in-flight work for partitions
    it still owns and drain the rest; commits for partitions the member
    no longer owns are refused by the server."""

    partitions: list
    generation: int

_HDR = struct.Struct("<BHI")
_U32 = struct.Struct("<I")
_U16 = struct.Struct("<H")
_U64 = struct.Struct("<Q")


class KuttiDBError(Exception):
    pass


class ManagedServerConfigurationError(KuttiDBError):
    pass


class ManagedServerStartupError(KuttiDBError):
    def __init__(self, category: str, log_path: str | None = None):
        self.category, self.log_path = category, log_path
        super().__init__(f"managed KuttiDB startup failed: {category}" +
                         (f" (log: {log_path})" if log_path else ""))


class ManagedServerStartupTimeout(ManagedServerStartupError):
    pass


class ManagedServerInstanceMismatch(KuttiDBError):
    pass


class ManagedServerEndpointOccupied(KuttiDBError):
    pass


@dataclass(frozen=True)
class ServerParams:
    """Typed, shell-free configuration for opt-in managed local mode.

    The first release intentionally defaults to an owner-only Unix socket.
    Existing :class:`KuttiDBClient` construction remains connect-only unless
    this object is supplied.
    """
    data_dir: str
    executable: str | None = None
    transport: str = "unix"
    idle_timeout: float = 60.0
    startup_timeout: float = 10.0
    startup_orphan_timeout: float = 60.0
    durability: str = "periodic"
    max_memory_mb: int | None = None
    auth_file: str | None = None
    fsync_ms: int | None = None
    max_value_mb: int | None = None
    max_batch_mb: int | None = None
    max_clients: int | None = None
    threads: int | None = None
    queue_wal: str | None = None
    stream_wal: str | None = None
    tls_cert: str | None = None
    tls_key: str | None = None
    metrics_bind: str | None = None
    metrics_token_file: str | None = None
    admin_bind: str | None = None
    admin_token_file: str | None = None
    admin_audit_log: str | None = None
    admin_allow_origins: tuple[str, ...] = ()
    admin_tls_cert: str | None = None
    admin_tls_key: str | None = None
    admin_max_clients: int | None = None
    admin_max_tail_clients: int | None = None
    admin_session_limit: int | None = None
    admin_job_limit: int | None = None

    def __post_init__(self):
        data_dir = os.path.abspath(os.fspath(self.data_dir))
        object.__setattr__(self, "data_dir", data_dir)
        if self.transport not in {"unix", "tcp"}:
            raise ManagedServerConfigurationError("managed transport must be 'unix' or 'tcp'")
        if any(not isinstance(v, (int, float)) or v <= 0 for v in
               (self.idle_timeout, self.startup_timeout, self.startup_orphan_timeout)):
            raise ManagedServerConfigurationError("managed timeouts must be positive")
        if self.durability not in {"periodic", "always"}:
            raise ManagedServerConfigurationError("durability must be periodic or always")
        for name in ("max_memory_mb", "max_value_mb", "max_batch_mb", "max_clients", "threads",
                     "admin_max_clients", "admin_max_tail_clients", "admin_session_limit", "admin_job_limit"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, int) or value <= 0):
                raise ManagedServerConfigurationError(f"{name} must be a positive integer")
        if self.fsync_ms is not None and (not isinstance(self.fsync_ms, int) or self.fsync_ms < 0):
            raise ManagedServerConfigurationError("fsync_ms must be a non-negative integer")
        for name in ("auth_file", "queue_wal", "stream_wal", "tls_cert", "tls_key", "metrics_token_file",
                     "admin_token_file", "admin_audit_log", "admin_tls_cert", "admin_tls_key"):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, (str, os.PathLike)) or not os.fspath(value):
                    raise ManagedServerConfigurationError(f"{name} must be a non-empty path")
                object.__setattr__(self, name, os.path.abspath(os.fspath(value)))
        if (self.tls_cert is None) != (self.tls_key is None):
            raise ManagedServerConfigurationError("tls_cert and tls_key must be supplied together")
        if self.transport == "unix" and self.tls_cert is not None:
            raise ManagedServerConfigurationError("managed Unix mode cannot configure TLS")
        if (self.admin_tls_cert is None) != (self.admin_tls_key is None):
            raise ManagedServerConfigurationError("admin_tls_cert and admin_tls_key must be supplied together")
        if self.metrics_token_file is not None and self.metrics_bind is None:
            raise ManagedServerConfigurationError("metrics_token_file requires metrics_bind")
        if self.admin_bind is None and any((self.admin_token_file, self.admin_audit_log, self.admin_tls_cert,
                                            self.admin_tls_key, self.admin_allow_origins, self.admin_max_clients,
                                            self.admin_max_tail_clients, self.admin_session_limit, self.admin_job_limit)):
            raise ManagedServerConfigurationError("admin settings require admin_bind")
        if self.admin_bind is not None and (self.admin_token_file is None or self.admin_audit_log is None):
            raise ManagedServerConfigurationError("admin_bind requires admin_token_file and admin_audit_log")
        if not isinstance(self.admin_allow_origins, tuple) or any(not isinstance(v, str) or not v for v in self.admin_allow_origins):
            raise ManagedServerConfigurationError("admin_allow_origins must be a tuple of non-empty origins")


def _read_instance_id(data_dir: str) -> str | None:
    """Read, but never adopt or create, an existing managed identity."""
    path = Path(data_dir) / "instance.id"
    try:
        metadata = os.lstat(path)
        if (not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode) or
                metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077):
            raise ManagedServerConfigurationError("managed instance identity is unsafe")
        value = path.read_text(encoding="ascii")
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ManagedServerConfigurationError("cannot read managed instance identity") from error
    value = value.rstrip("\n")
    if len(value) != 32 or any(ch not in "0123456789abcdef" for ch in value):
        raise ManagedServerConfigurationError("managed instance identity is invalid")
    return value


class KuttiDBClient:
    def __init__(self, host="127.0.0.1", port=7379, timeout=5.0,
                 auth_token: str | bytes | None = None,
                 tls: bool | ssl.SSLContext = False, ca_file: str | None = None,
                 server_hostname: str | None = None, unix_path: str | None = None,
                 server: ServerParams | None = None):
        self._buf = b""
        self._managed_server = server
        if server is not None:
            if server.transport == "unix":
                if (host != "127.0.0.1" or port != 7379 or tls or
                        (unix_path and os.path.abspath(unix_path) != os.path.join(server.data_dir, "kuttidb.sock"))):
                    raise ManagedServerConfigurationError("managed Unix mode does not allow TCP/TLS or a contradictory socket path")
                unix_path = os.path.join(server.data_dir, "kuttidb.sock")
            else:
                try:
                    address = ipaddress.ip_address(host)
                except ValueError as error:
                    raise ManagedServerConfigurationError("managed TCP requires a literal loopback IPv4 address") from error
                if address.version != 4 or not address.is_loopback or unix_path:
                    raise ManagedServerConfigurationError("managed TCP requires a literal loopback IPv4 endpoint")
            if server.tls_cert is not None and not tls:
                raise ManagedServerConfigurationError("managed TLS certificate settings require tls=True or an SSLContext")
            if tls and server.tls_cert is None:
                raise ManagedServerConfigurationError("managed TLS requires tls_cert and tls_key server settings")
            self._connect_managed(host, port, timeout, auth_token, tls, ca_file, server_hostname, unix_path, server)
            return
        self._connect_once(host, port, timeout, auth_token, tls, ca_file, server_hostname, unix_path)

    @classmethod
    def managed(cls, *, data_dir: str, idle_timeout: float = 60.0,
                startup_timeout: float = 10.0, executable: str | None = None,
                auth_token: str | bytes | None = None, host: str = "127.0.0.1",
                port: int = 7379, **settings):
        """Connect to one app-owned local instance, starting it if absent."""
        params = ServerParams(data_dir=data_dir, idle_timeout=idle_timeout,
                              startup_timeout=startup_timeout, executable=executable,
                              **settings)
        return cls(host=host, port=port, timeout=min(5.0, startup_timeout),
                   auth_token=auth_token,
                   unix_path=(os.path.join(params.data_dir, "kuttidb.sock")
                              if params.transport == "unix" else None), server=params)

    def _connect_once(self, host, port, timeout, auth_token, tls, ca_file, server_hostname, unix_path):
        if unix_path:
            if tls:
                raise KuttiDBError("TLS is not supported over a Unix socket")
            raw_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            raw_sock.settimeout(timeout)
            try:
                raw_sock.connect(unix_path)
            except Exception:
                raw_sock.close()
                raise
        else:
            raw_sock = socket.create_connection((host, port), timeout=timeout)
        if tls:
            context = tls if isinstance(tls, ssl.SSLContext) else ssl.create_default_context(cafile=ca_file)
            try:
                self.sock = context.wrap_socket(raw_sock, server_hostname=server_hostname or host)
            except Exception:
                raw_sock.close()
                raise
        else:
            self.sock = raw_sock
        if not unix_path:
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._buf = b""
        if auth_token is not None:
            token = auth_token.encode() if isinstance(auth_token, str) else auth_token
            if not token or len(token) > 1024:
                self.sock.close()
                raise KuttiDBError("auth token must contain 1..1024 bytes")
            try:
                self._send(OP_AUTH, token)
                status, _ = self._recv_response()
            except Exception:
                self.sock.close()
                raise
            if status != ST_OK:  # defensive: error responses normally raise above
                self.sock.close()
                raise KuttiDBError("authentication failed")

    def _connect_managed(self, host, port, timeout, auth_token, tls, ca_file,
                         server_hostname, unix_path, server):
        expected = _read_instance_id(server.data_dir)
        endpoint = f"unix:{unix_path}" if unix_path else f"tcp:{host}:{port}"
        try:
            self._connect_once(host, port, timeout, auth_token, tls, ca_file,
                               server_hostname, unix_path)
        except OSError as error:
            absent_errors = (errno.ENOENT, errno.ECONNREFUSED) if unix_path else (errno.ECONNREFUSED,)
            if error.errno not in absent_errors:
                raise ManagedServerEndpointOccupied("managed endpoint is unavailable") from error
            launch = self._ensure(server, endpoint)
            expected = launch["instance_id"]
            self._connect_once(host, port, timeout, auth_token, tls, ca_file,
                               server_hostname, unix_path)
        else:
            if expected is None:
                self.close()
                raise ManagedServerEndpointOccupied("endpoint is occupied by an unverifiable server")
        self._verify_managed_identity(expected)

    @staticmethod
    def _server_executable(server: ServerParams) -> str:
        candidates = [server.executable, os.environ.get("KUTTIDB_SERVER"),
                      os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "kuttidb"), "kuttidb"]
        for candidate in candidates:
            if not candidate:
                continue
            if os.path.sep in candidate:
                if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                    return os.path.abspath(candidate)
            else:
                from shutil import which
                found = which(candidate)
                if found:
                    return found
        raise ManagedServerConfigurationError("could not find the kuttidb server executable")

    def _ensure(self, server: ServerParams, endpoint: str) -> dict:
        executable = self._server_executable(server)
        command = [executable, "ensure", "--data-dir", server.data_dir, "--listen", endpoint,
                   "--idle-timeout-ms", str(int(server.idle_timeout * 1000)),
                   "--startup-orphan-timeout-ms", str(int(server.startup_orphan_timeout * 1000)),
                   "--startup-timeout-ms", str(int(server.startup_timeout * 1000)), "--json"]
        command.extend(["--durability", server.durability])
        optional = (
            ("auth_file", "--auth-file"), ("max_memory_mb", "--max-memory-mb"),
            ("fsync_ms", "--fsync-ms"), ("max_value_mb", "--max-value-mb"),
            ("max_batch_mb", "--max-batch-mb"), ("max_clients", "--max-clients"),
            ("threads", "--threads"), ("queue_wal", "--queue-wal"), ("stream_wal", "--stream-wal"),
            ("tls_cert", "--tls-cert"), ("tls_key", "--tls-key"),
            ("metrics_bind", "--metrics-bind"), ("metrics_token_file", "--metrics-token-file"),
            ("admin_bind", "--admin-bind"), ("admin_token_file", "--admin-token-file"),
            ("admin_audit_log", "--admin-audit-log"), ("admin_tls_cert", "--admin-tls-cert"),
            ("admin_tls_key", "--admin-tls-key"), ("admin_max_clients", "--admin-max-clients"),
            ("admin_max_tail_clients", "--admin-max-tail-clients"), ("admin_session_limit", "--admin-session-limit"),
            ("admin_job_limit", "--admin-job-limit"),
        )
        for attribute, flag in optional:
            value = getattr(server, attribute)
            if value is not None:
                command.extend([flag, str(value)])
        for origin in server.admin_allow_origins:
            command.extend(["--admin-allow-origin", origin])
        try:
            completed = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                       timeout=server.startup_timeout + 1, check=False)
        except subprocess.TimeoutExpired as error:
            raise ManagedServerStartupTimeout("timeout") from error
        except OSError as error:
            raise ManagedServerStartupError("executable") from error
        stdout = completed.stdout[:8192]
        if completed.returncode:
            categories = {
                64: "configuration", 65: "unsafe_path", 66: "ownership",
                67: "startup", 68: "timeout",
            }
            category = categories.get(completed.returncode, "startup")
            if category == "configuration":
                raise ManagedServerConfigurationError("managed launcher rejected the configuration")
            exc = ManagedServerStartupTimeout if category == "timeout" else ManagedServerStartupError
            raise exc(category, os.path.join(server.data_dir, "kuttidb.log"))
        try:
            response = json.loads(stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ManagedServerStartupError("invalid_launcher_response") from error
        if (completed.returncode != 0 or not isinstance(response, dict) or
                response.get("status") not in {"started", "existing", "starting"} or
                not isinstance(response.get("instance_id"), str) or
                len(response["instance_id"]) != 32):
            category = "timeout" if completed.returncode == 68 else "startup"
            exc = ManagedServerStartupTimeout if category == "timeout" else ManagedServerStartupError
            raise exc(category, response.get("log") if isinstance(response, dict) else None)
        return response

    def _verify_managed_identity(self, expected: str | None) -> None:
        if not expected:
            self.close()
            raise ManagedServerEndpointOccupied("managed instance identity is missing")
        capabilities = self.capabilities()
        if not capabilities["features"] & CAP_SERVER_INFO:
            self.close()
            raise ManagedServerEndpointOccupied("endpoint uses an incompatible KuttiDB protocol")
        self._send(OP_SERVER_INFO, b"")
        status, value = self._recv_response()
        if status != ST_OK or len(value) != 52 or value[0] != 1 or value[1] != 32:
            self.close()
            raise ManagedServerEndpointOccupied("endpoint does not expose a managed instance identity")
        actual = value[2:34].decode("ascii", "replace")
        if actual != expected:
            self.close()
            raise ManagedServerInstanceMismatch("endpoint belongs to another KuttiDB instance")

    def close(self):
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- low level -----------------------------------------------------------

    def _send(self, op, key, value=b""):
        klen = len(key)
        if klen > MAX_KEY:
            raise KuttiDBError("key too large")
        if len(value) > MAX_VALUE:
            raise KuttiDBError("value too large")
        self.sock.sendall(_HDR.pack(op, klen, len(value)) + key + value)

    def _recv_exact(self, n):
        while len(self._buf) < n:
            chunk = self.sock.recv(1 << 20)
            if not chunk:
                raise KuttiDBError("connection closed")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _recv_response(self):
        status, vlen = struct.unpack("<BI", self._recv_exact(5))
        if vlen > MAX_VALUE:
            raise KuttiDBError("invalid response length")
        value = self._recv_exact(vlen) if vlen else b""
        if status == ST_ERR:
            raise KuttiDBError("server error")
        return status, value

    # -- single ops ----------------------------------------------------------

    def put(self, key: str, value: bytes, ttl: float | None = None) -> None:
        """ttl in seconds (optional)."""
        kb = key.encode()
        if len(kb) > MAX_KEY:
            raise KuttiDBError("key too large")
        if len(value) > MAX_VALUE:
            raise KuttiDBError("value too large")
        if ttl is None:
            self._send(OP_PUT, kb, value)
        else:
            ttl_ms = max(1, int(ttl * 1000))
            self.sock.sendall(
                struct.pack("<BHII", OP_PUT_TTL, len(kb), len(value), ttl_ms) + kb + value)
        self._recv_response()

    def get(self, key: str) -> bytes | None:
        self._send(OP_GET, key.encode())
        status, value = self._recv_response()
        return value if status == ST_OK else None

    def delete(self, key: str) -> bool:
        self._send(OP_DELETE, key.encode())
        status, _ = self._recv_response()
        return status == ST_OK

    def stats(self) -> dict:
        import json
        self._send(OP_STATS, b"")
        status, value = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("stats unavailable")
        return json.loads(value)

    def health(self) -> bool:
        """Return whether every configured persistence engine is writable.

        On authenticated servers this operation is authenticated too; a
        failed durability engine raises :class:`KuttiDBError` rather than being
        presented as ready.
        """
        self._send(OP_HEALTH, b"")
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("server is not ready")
        return True

    def capabilities(self) -> dict:
        """Negotiate protocol v1 and return the server feature bitset.

        A server with a different major version is rejected before any
        feature-specific command is issued. Minor versions remain compatible.
        """
        self._send(OP_CAPABILITIES, b"", _U16.pack(PROTOCOL_MAJOR) +
                   _U16.pack(PROTOCOL_MINOR))
        status, value = self._recv_response()
        if status == ST_MISS:
            raise KuttiDBError("incompatible protocol major version")
        if status != ST_OK or len(value) != 12:
            raise KuttiDBError("capability negotiation failed")
        major, minor, features = struct.unpack("<HHQ", value)
        if major != PROTOCOL_MAJOR:
            raise KuttiDBError("incompatible protocol major version")
        return {"major": major, "minor": minor, "features": features}

    # -- batched ops (one round trip per batch) ------------------------------

    def put_many(self, items) -> None:
        """items: iterable of (key, value) or (key, value, ttl_seconds|None).
        One round trip per BATCH_SIZE."""
        items = iter(items)
        while True:
            chunk = []
            for kv in items:
                chunk.append(kv if len(kv) == 3 else (*kv, None))
                if len(chunk) >= BATCH_SIZE:
                    break
            if not chunk:
                return
            with_ttl = any(t is not None for _, _, t in chunk)
            parts = []
            total = 7
            for k, v, t in chunk:
                kb = k.encode()
                if len(kb) > MAX_KEY:
                    raise KuttiDBError("key too large")
                if len(v) > MAX_VALUE:
                    raise KuttiDBError("value too large")
                total += (10 if with_ttl else 6) + len(kb) + len(v)
                if total > MAX_VALUE:
                    raise KuttiDBError("batch too large")
                parts.append(_U16.pack(len(kb)))
                parts.append(_U32.pack(len(v)))
                if with_ttl:
                    parts.append(_U32.pack(max(1, int((t or 0) * 1000))))
                parts.append(kb)
                parts.append(v)
            op = OP_PUT_BATCH_TTL if with_ttl else OP_PUT_BATCH
            self.sock.sendall(bytes([op, 0, 0]) + _U32.pack(len(chunk)) + b"".join(parts))
            status = self._recv_exact(1)[0]
            if status != ST_OK:
                raise KuttiDBError("server error in put_many")

    def get_many(self, keys) -> list:
        """keys: iterable of str. Returns list of bytes|None, one round trip per batch."""
        keys = list(keys)
        result = [None] * len(keys)
        for start in range(0, len(keys), BATCH_SIZE):
            chunk = keys[start:start + BATCH_SIZE]
            enc = [k.encode() for k in chunk]
            if any(len(kb) > MAX_KEY for kb in enc):
                raise KuttiDBError("key too large")
            self.sock.sendall(
                bytes([OP_GET_BATCH, 0, 0]) + _U32.pack(len(chunk)) +
                b"".join(_U16.pack(len(kb)) + kb for kb in enc)
            )
            count = _U32.unpack(self._recv_exact(4))[0]
            for i in range(count):
                status, vlen = struct.unpack("<BI", self._recv_exact(5))
                if vlen > MAX_VALUE:
                    raise KuttiDBError("invalid response length")
                val = self._recv_exact(vlen) if vlen else b""
                if status == ST_ERR:
                    raise KuttiDBError("server error in get_many")
                result[start + i] = val if status == ST_OK else None
        return result

    # -- durable work queues ------------------------------------------------

    def queue_declare(self, name: str, *, durable=True, max_depth=0,
                      dead_letter_queue: str | None = None,
                      max_deliveries: int = 0) -> None:
        key = name.encode()
        if not key or len(key) > MAX_KEY or max_depth < 0:
            raise KuttiDBError("invalid queue declaration")
        value = bytes([bool(durable)]) + _U64.pack(max_depth)
        if dead_letter_queue is not None:
            dlq = dead_letter_queue.encode()
            if not dlq or len(dlq) > 255 or dlq == key:
                raise KuttiDBError("invalid dead-letter queue")
            if max_deliveries < 0 or max_deliveries >= (1 << 32):
                raise KuttiDBError("invalid max_deliveries")
            ext = _U16.pack(len(dlq)) + dlq + _U32.pack(max_deliveries)
            value += _U16.pack(len(ext)) + ext
        self._send(OP_QUEUE_DECLARE, key, value)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("queue declaration failed")

    def queue_list(self) -> list[dict]:
        """Inventory of declared queues: name, depth, in-flight deliveries.
        Read-only snapshot, bounded at 256 entries."""
        self._send(OP_QUEUE_LIST, b"", b"")
        status, response = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("queue list failed")
        n = struct.unpack("<H", response[:2])[0]
        out, at = [], 2
        for _ in range(n):
            nlen = struct.unpack("<H", response[at:at + 2])[0]
            name = response[at + 2:at + 2 + nlen].decode()
            depth, inflight = struct.unpack("<QQ", response[at + 2 + nlen:at + 18 + nlen])
            out.append({"name": name, "depth": depth, "inflight": inflight})
            at += 18 + nlen
        return out

    def queue_publish(self, name: str, value: bytes, *, ttl: float | None = None) -> int:
        if ttl is not None and ttl < 0:
            raise KuttiDBError("TTL must be non-negative")
        op = OP_QUEUE_PUBLISH if ttl is None else OP_QUEUE_PUBLISH_TTL
        payload = value if ttl is None else _U64.pack(max(0, int(ttl * 1000))) + value
        self._send(op, name.encode(), payload)
        status, response = self._recv_response()
        if status != ST_OK or len(response) != 8:
            raise KuttiDBError("queue publish failed")
        return _U64.unpack(response)[0]

    def queue_consume(self, name: str, *, visibility=30.0):
        if visibility < 0:
            raise KuttiDBError("visibility must be non-negative")
        self._send(OP_QUEUE_CONSUME, name.encode(),
                   _U64.pack(max(0, int(visibility * 1000))))
        status, response = self._recv_response()
        if status == ST_MISS:
            return None
        if status != ST_OK or len(response) < 21:
            raise KuttiDBError("queue consume failed")
        delivery_tag = _U64.unpack(response[:8])[0]
        message_id = _U64.unpack(response[8:16])[0]
        return {"id": delivery_tag, "message_id": message_id,
                "redelivered": bool(response[16]),
                "delivery_count": _U32.unpack(response[17:21])[0],
                "value": response[21:]}

    def queue_stats(self, name: str):
        self._send(OP_QUEUE_STATS, name.encode())
        status, response = self._recv_response()
        if status == ST_MISS:
            return None
        if status != ST_OK or len(response) != 16:
            raise KuttiDBError("queue stats failed")
        return {"depth": _U64.unpack(response[:8])[0],
                "inflight": _U64.unpack(response[8:])[0]}

    def queue_prefetch(self, count: int) -> None:
        if count < 0 or count >= (1 << 32):
            raise KuttiDBError("invalid queue prefetch")
        self._send(OP_QUEUE_PREFETCH, b"_", _U32.pack(count))
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("queue prefetch failed")

    def queue_cancel(self) -> None:
        self._send(OP_QUEUE_CANCEL, b"_")
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("queue cancel failed")

    def queue_ack(self, name: str, delivery_tag: int) -> bool:
        self._send(OP_QUEUE_ACK, name.encode(), _U64.pack(delivery_tag))
        status, _ = self._recv_response()
        if status == ST_ERR:
            raise KuttiDBError("queue ACK failed")
        return status == ST_OK

    # -- durable named consumers ----------------------------------------------

    def queue_consumer_register(self, consumer: str) -> int:
        """Register (or heartbeat) a durable named consumer and bind this
        connection to it. Returns the consumer's stable owner token.

        Deliveries made via :meth:`queue_consume_as` use this token: if the
        connection drops, its in-flight deliveries follow their visibility
        deadlines instead of being requeued immediately, and reconnecting
        with the same name keeps ownership. Registration survives a server
        restart on durable stores; delivery tags stay one-use per process."""
        name = consumer.encode()
        if not name or len(name) > 255:
            raise KuttiDBError("invalid consumer name")
        self._send(OP_QUEUE_CONSUMER_REGISTER, name)
        status, response = self._recv_response()
        if status != ST_OK or len(response) != 8:
            raise KuttiDBError("consumer registration failed")
        return _U64.unpack(response)[0]

    def queue_consumer_unregister(self, consumer: str) -> None:
        """Remove a durable named consumer; its in-flight deliveries are
        requeued immediately. Unregistering an unknown consumer is an error."""
        name = consumer.encode()
        if not name or len(name) > 255:
            raise KuttiDBError("invalid consumer name")
        self._send(OP_QUEUE_CONSUMER_UNREGISTER, name)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("consumer unregistration failed")

    def queue_consume_as(self, name: str, consumer: str, *,
                         visibility: float = 30.0):
        """Consume from a queue as a named durable consumer. Delivery tags
        returned here are valid for this process; the consumer's owner token
        keeps in-flight accounting stable across reconnects."""
        gb = consumer.encode()
        if not gb or len(gb) > 255 or visibility < 0:
            raise KuttiDBError("invalid queue consume-as")
        self._send(OP_QUEUE_CONSUME_AS, name.encode(),
                   _U16.pack(len(gb)) + gb + _U64.pack(int(visibility * 1000)))
        status, response = self._recv_response()
        if status == ST_MISS:
            return None
        if status != ST_OK or len(response) < 21:
            raise KuttiDBError("queue consume-as failed")
        delivery_tag = _U64.unpack(response[:8])[0]
        message_id = _U64.unpack(response[8:16])[0]
        return {"id": delivery_tag, "message_id": message_id,
                "redelivered": bool(response[16]),
                "delivery_count": _U32.unpack(response[17:21])[0],
                "value": response[21:]}

    def queue_nack(self, name: str, delivery_tag: int, *, requeue=True,
                   delay: float = 0.0) -> bool:
        if delay < 0:
            raise KuttiDBError("retry delay must be non-negative")
        payload = _U64.pack(delivery_tag) + bytes([bool(requeue)])
        if delay:
            payload += _U64.pack(int(delay * 1000))
        self._send(OP_QUEUE_NACK, name.encode(), payload)
        status, _ = self._recv_response()
        if status == ST_ERR:
            raise KuttiDBError("queue NACK failed")
        return status == ST_OK

    # -- queue batch operations (server capability bit 12) ---------------------

    def queue_publish_batch(self, name: str, values) -> list:
        """Publish a bounded batch (1-256 messages) to one queue with a
        single durability round trip. Returns the message IDs in order. The
        whole batch is capacity-checked first: nothing is written when it
        would exceed the queue's max depth."""
        key = name.encode()
        payload = _U32.pack(len(values))
        for value in values:
            payload += _U32.pack(len(value)) + value
        self._send(OP_QUEUE_PUBLISH_BATCH, key, payload)
        status, response = self._recv_response()
        if status != ST_OK or len(response) < 4:
            raise KuttiDBError("queue publish batch failed")
        count = _U32.unpack(response[:4])[0]
        if len(response) != 4 + 8 * count:
            raise KuttiDBError("queue publish batch response truncated")
        return [_U64.unpack(response[4 + 8 * i:12 + 8 * i])[0] for i in range(count)]

    def queue_consume_batch(self, name: str, max_count: int, *, visibility=30.0):
        """Consume up to max_count (1-256) ready messages in one round trip.
        Returns a list of the same delivery dicts as :meth:`queue_consume`
        (empty when the queue has nothing ready). All delivery records share
        one durability round trip; the visibility lease applies to every
        delivered message."""
        if not 1 <= max_count <= 256:
            raise KuttiDBError("batch size must be 1-256")
        self._send(OP_QUEUE_CONSUME_BATCH, name.encode(),
                   _U32.pack(max_count))
        status, response = self._recv_response()
        if status == ST_MISS:
            return None
        if status != ST_OK or len(response) < 4:
            raise KuttiDBError("queue consume batch failed")
        count = _U32.unpack(response[:4])[0]
        out = []
        at = 4
        for _ in range(count):
            if at + 25 > len(response):
                raise KuttiDBError("queue consume batch response truncated")
            delivery_tag = _U64.unpack(response[at:at + 8])[0]
            message_id = _U64.unpack(response[at + 8:at + 16])[0]
            delivery_count = _U32.unpack(response[at + 16:at + 20])[0]
            redelivered = bool(response[at + 20])
            mlen = _U32.unpack(response[at + 21:at + 25])[0]
            at += 25
            if at + mlen > len(response):
                raise KuttiDBError("queue consume batch response truncated")
            out.append({"id": delivery_tag, "message_id": message_id,
                        "redelivered": redelivered,
                        "delivery_count": delivery_count,
                        "value": response[at:at + mlen]})
            at += mlen
        return out

    def queue_ack_batch(self, name: str, delivery_tags) -> int:
        """ACK a bounded batch (1-256) of delivery tags in one round trip.
        Returns the number of tags that were still valid; unknown or already
        processed tags are skipped. All ACK records share one fsync."""
        tags = list(delivery_tags)
        if not 1 <= len(tags) <= 256:
            raise KuttiDBError("batch size must be 1-256")
        payload = bytes([0]) + _U32.pack(len(tags))
        payload += b"".join(_U64.pack(int(t)) for t in tags)
        self._send(OP_QUEUE_ACK_BATCH, name.encode(), payload)
        status, response = self._recv_response()
        if status != ST_OK or len(response) != 4:
            raise KuttiDBError("queue ACK batch failed")
        return _U32.unpack(response)[0]

    def queue_nack_batch(self, name: str, delivery_tags, *, requeue=True) -> int:
        """NACK a bounded batch (1-256) of delivery tags in one round trip.
        requeue=True makes the messages immediately ready again;
        requeue=False discards them or routes them to the dead-letter queue.
        Returns the number of tags that were still valid."""
        tags = list(delivery_tags)
        if not 1 <= len(tags) <= 256:
            raise KuttiDBError("batch size must be 1-256")
        payload = bytes([1 if requeue else 2]) + _U32.pack(len(tags))
        payload += b"".join(_U64.pack(int(t)) for t in tags)
        self._send(OP_QUEUE_ACK_BATCH, name.encode(), payload)
        status, response = self._recv_response()
        if status != ST_OK or len(response) != 4:
            raise KuttiDBError("queue NACK batch failed")
        return _U32.unpack(response)[0]

    # -- exchanges and routing ------------------------------------------------

    def exchange_declare(self, name: str, *, type: str = "direct",
                         durable: bool = True,
                         alternate_exchange: str | None = None) -> None:
        """Declare an exchange (direct, fanout, or topic). Redeclaring with
        different parameters fails; the default exchange is unnamed and
        always exists."""
        key = name.encode()
        if not key or len(key) > 255:
            raise KuttiDBError("invalid exchange name")
        if type not in EXCHANGE_TYPES:
            raise KuttiDBError("unknown exchange type")
        value = bytes([bool(durable), EXCHANGE_TYPES[type]])
        if alternate_exchange is not None:
            ae = alternate_exchange.encode()
            if not ae or len(ae) > 255 or ae == key:
                raise KuttiDBError("invalid alternate exchange")
            ext = _U16.pack(len(ae)) + ae
            value += _U16.pack(len(ext)) + ext
        self._send(OP_EXCHANGE_DECLARE, key, value)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("exchange declaration failed")

    def exchange_bind(self, exchange: str, queue: str,
                      routing_key: str | bytes = "") -> None:
        """Bind an existing queue to an exchange. The routing key is exact
        (direct), ignored (fanout), or a '*'/ '#' pattern (topic)."""
        xk = exchange.encode()
        qk = queue.encode()
        rk = routing_key.encode() if isinstance(routing_key, str) else routing_key
        if not xk or len(xk) > 255 or not qk or len(qk) > 255 or len(rk) > 255:
            raise KuttiDBError("invalid exchange binding")
        payload = _U16.pack(len(qk)) + qk + _U16.pack(len(rk)) + rk
        self._send(OP_EXCHANGE_BIND, xk, payload)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("exchange binding failed")

    def exchange_unbind(self, exchange: str, queue: str,
                        routing_key: str | bytes = "") -> bool:
        """Remove one binding. Returns True when a binding was removed."""
        xk = exchange.encode()
        qk = queue.encode()
        rk = routing_key.encode() if isinstance(routing_key, str) else routing_key
        if not xk or len(xk) > 255 or not qk or len(qk) > 255 or len(rk) > 255:
            raise KuttiDBError("invalid exchange binding")
        payload = _U16.pack(len(qk)) + qk + _U16.pack(len(rk)) + rk
        self._send(OP_EXCHANGE_UNBIND, xk, payload)
        status, _ = self._recv_response()
        if status == ST_ERR:
            raise KuttiDBError("exchange unbind failed")
        return status == ST_OK

    def exchange_publish(self, exchange: str, routing_key: str | bytes,
                         value: bytes, *, ttl: float | None = None) -> int:
        """Publish through an exchange. Returns the number of queues that
        received a copy; 0 means the message was unroutable. An empty
        exchange name routes to the queue named by the routing key."""
        xk = exchange.encode()
        rk = routing_key.encode() if isinstance(routing_key, str) else routing_key
        if len(xk) > 255 or len(rk) > 255:
            raise KuttiDBError("invalid exchange publish")
        if ttl is not None and ttl < 0:
            raise KuttiDBError("TTL must be non-negative")
        ttl_ms = max(0, int((ttl or 0) * 1000))
        payload = _U16.pack(len(rk)) + _U64.pack(ttl_ms) + rk + value
        self._send(OP_EXCHANGE_PUBLISH, xk, payload)
        status, response = self._recv_response()
        if status == ST_MISS:
            return 0
        if status != ST_OK or len(response) != 4:
            raise KuttiDBError("exchange publish failed")
        return _U32.unpack(response)[0]

    # -- atomic cache plus messaging -------------------------------------------

    def _atomic_result(self, status: int, response: bytes) -> dict:
        if status == ST_MISS:
            return {"tx_id": 0, "routed": 0, "unroutable": True}
        if status != ST_OK or len(response) != 12:
            raise KuttiDBError("atomic operation failed")
        tx_id, routed = struct.unpack("<QI", response)
        return {"tx_id": tx_id, "routed": routed, "unroutable": False}

    def put_and_publish(self, key: str, value: bytes, *, exchange: str,
                        routing_key: str | bytes = "",
                        ttl: float | None = None) -> dict:
        """Atomically put a cache entry and publish the same bytes through an
        exchange. After a crash, recovery shows both or neither; the returned
        tx_id identifies the durable commit record."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY or len(value) > MAX_VALUE:
            raise KuttiDBError("invalid atomic put")
        xk = exchange.encode()
        rk = routing_key.encode() if isinstance(routing_key, str) else routing_key
        if len(xk) > 255 or len(rk) > 255:
            raise KuttiDBError("invalid atomic publish target")
        ttl_ms = max(0, int((ttl or 0) * 1000)) if ttl is not None else 0
        payload = (_U16.pack(len(xk)) + xk + _U16.pack(len(rk)) + rk +
                   _U32.pack(ttl_ms) + value)
        self._send(OP_ATOMIC_PUT_PUBLISH, kb, payload)
        status, response = self._recv_response()
        return self._atomic_result(status, response)

    def update_and_emit(self, key: str, value: bytes, *, exchange: str,
                        routing_key: str | bytes = "",
                        ttl: float | None = None) -> dict:
        """Atomically replace an existing cache entry and emit the same bytes
        through an exchange. The key must already exist: a miss commits
        nothing and returns ``{"tx_id": 0, "routed": 0, "unroutable": True}``
        (the same MISS shape the wire uses for an unroutable publish — either
        way nothing was committed). On success the returned tx_id identifies
        the durable commit record, and recovery shows the update and the
        event together or not at all."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY or len(value) > MAX_VALUE:
            raise KuttiDBError("invalid atomic update")
        xk = exchange.encode()
        rk = routing_key.encode() if isinstance(routing_key, str) else routing_key
        if len(xk) > 255 or len(rk) > 255:
            raise KuttiDBError("invalid atomic publish target")
        ttl_ms = max(0, int((ttl or 0) * 1000)) if ttl is not None else 0
        payload = (_U16.pack(len(xk)) + xk + _U16.pack(len(rk)) + rk +
                   _U32.pack(ttl_ms) + value)
        self._send(OP_ATOMIC_UPDATE_EMIT, kb, payload)
        status, response = self._recv_response()
        return self._atomic_result(status, response)

    def put_and_enqueue(self, key: str, value: bytes, *, queue: str,
                        ttl: float | None = None) -> dict:
        """Atomically put a cache entry and enqueue the same bytes as a
        durable message. Requires a durable queue and cache persistence."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY or len(value) > MAX_VALUE:
            raise KuttiDBError("invalid atomic put")
        qk = queue.encode()
        if not qk or len(qk) > 255:
            raise KuttiDBError("invalid atomic enqueue target")
        ttl_ms = max(0, int((ttl or 0) * 1000)) if ttl is not None else 0
        payload = _U16.pack(len(qk)) + qk + _U32.pack(ttl_ms) + value
        self._send(OP_ATOMIC_PUT_ENQUEUE, kb, payload)
        status, response = self._recv_response()
        return self._atomic_result(status, response)

    def delete_and_publish(self, key: str, *, exchange: str,
                           routing_key: str | bytes = "",
                           message: bytes | None = None) -> dict:
        """Atomically delete a cache entry and publish an event (defaults to
        the key bytes). The delete is idempotent: a missing key still emits
        the event and reports the commit id."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY:
            raise KuttiDBError("invalid atomic delete")
        xk = exchange.encode()
        rk = routing_key.encode() if isinstance(routing_key, str) else routing_key
        if len(xk) > 255 or len(rk) > 255:
            raise KuttiDBError("invalid atomic publish target")
        payload_msg = key.encode() if message is None else message
        payload = (_U16.pack(len(xk)) + xk + _U16.pack(len(rk)) + rk +
                   _U32.pack(len(payload_msg)) + payload_msg)
        self._send(OP_ATOMIC_DELETE_PUBLISH, kb, payload)
        status, response = self._recv_response()
        return self._atomic_result(status, response)

    # -- anti-cache-stampede (singleflight) ------------------------------------

    SF_STATES = {0: "value", 1: "claimed", 2: "wait", 3: "negative",
                 4: "released", 5: "timeout", 6: "lost", 7: "stale",
                 8: "refresh"}

    def _sf_result(self, status: int, response: bytes) -> dict:
        if status == ST_ERR:
            raise KuttiDBError("singleflight operation refused")
        if status != ST_OK or len(response) < 1:
            raise KuttiDBError("singleflight operation failed")
        state = self.SF_STATES.get(response[0], f"unknown-{response[0]}")
        out = {"state": state}
        if response[0] in (0, 7, 8):
            out["value"] = response[1:]
        return out

    def _swr_result(self, status: int, response: bytes) -> dict:
        """GET_OR_REFRESH envelope: [state:1][holder:1][value]. holder=1 means
        this caller now owns the revalidation lease and must reload."""
        if status == ST_ERR:
            raise KuttiDBError("singleflight operation refused")
        if status != ST_OK or len(response) < 2:
            raise KuttiDBError("singleflight operation failed")
        state = self.SF_STATES.get(response[0], f"unknown-{response[0]}")
        out = {"state": state, "holder": bool(response[1])}
        if response[0] in (0, 7, 8):
            out["value"] = response[2:]
        return out

    def get_or_claim(self, key: str, *, lease: float = 5.0) -> dict:
        """Singleflight entry point. Returns state 'value' (hit, value
        attached), 'negative' (cached negative answer), 'claimed' (caller
        must load and call put_and_release), or 'wait' (another client is
        loading; call wait_for_key)."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY:
            raise KuttiDBError("invalid singleflight key")
        if lease < 0 or lease * 1000 > 60000:
            raise KuttiDBError("lease must be 0..60 seconds")
        self._send(OP_SF_GET_OR_CLAIM, kb,
                   _U32.pack(max(1, int(lease * 1000))))
        status, response = self._recv_response()
        return self._sf_result(status, response)

    def wait_for_key(self, key: str, *, timeout: float = 10.0) -> dict:
        """Wait (server-side, bounded) for another client's put_and_release.
        Returns state 'value' (value attached), 'negative', 'released'
        (retry), 'timeout', or 'lost' (value evicted; retry)."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY:
            raise KuttiDBError("invalid singleflight key")
        ms = int(timeout * 1000)
        if ms <= 0 or ms > 60000:
            raise KuttiDBError("timeout must be 0..60 seconds")
        self._send(OP_SF_WAIT_FOR_KEY, kb, _U32.pack(ms))
        status, response = self._recv_response()
        return self._sf_result(status, response)

    def put_and_release(self, key: str, value: bytes, *, ttl: float | None = None,
                        negative: bool = False) -> None:
        """Store the loaded value (or a cached negative answer) and wake every
        waiter of the key. Release the claim even when loading failed."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY or len(value) > MAX_VALUE:
            raise KuttiDBError("invalid singleflight put")
        ttl_ms = max(0, int((ttl or 0) * 1000)) if ttl is not None else 0
        payload = _U32.pack(ttl_ms) + bytes([bool(negative)]) + value
        self._send(OP_SF_PUT_AND_RELEASE, kb, payload)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("singleflight put failed")

    def release_claim(self, key: str) -> None:
        """Release a claim without storing anything (load failed). Waiters are
        woken with state 'released' so a new loader can claim the key."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY:
            raise KuttiDBError("invalid singleflight key")
        self._send(OP_SF_RELEASE_CLAIM, kb)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("singleflight release failed")

    def get_or_load(self, key: str, loader, *, ttl: float = 60.0,
                    lease: float = 5.0, wait: float = 10.0):
        """Cache-aside with native single-flight: the first requester for a
        missing key loads it while others wait server-side for the same
        result. `loader` returns the value bytes, or None for a cached
        negative answer. Returns the value bytes, or None when the answer is
        negatively cached."""
        hit = self.get(key)
        if hit is not None:
            return hit
        r = self.get_or_claim(key, lease=lease)
        if r["state"] == "value":
            return r["value"]
        if r["state"] == "negative":
            return None
        if r["state"] == "wait":
            for _ in range(3):  # bounded retry across releases/losses
                w = self.wait_for_key(key, timeout=wait)
                if w["state"] == "value":
                    return w["value"]
                if w["state"] == "negative":
                    return None
                if w["state"] == "timeout":
                    return None
                r = self.get_or_claim(key, lease=lease)
                if r["state"] == "value":
                    return r["value"]
                if r["state"] == "negative":
                    return None
                if r["state"] == "claimed":
                    break
        elif r["state"] == "claimed":
            pass
        else:  # 'released' or 'lost': one bounded retry of the whole cycle
            r = self.get_or_claim(key, lease=lease)
            if r["state"] == "value":
                return r["value"]
            if r["state"] == "negative":
                return None
        if r["state"] != "claimed":
            return None
        try:
            loaded = loader()
        except Exception:
            self.release_claim(key)
            raise
        if loaded is None:
            self.put_and_release(key, b"", ttl=ttl, negative=True)
            return None
        self.put_and_release(key, loaded, ttl=ttl)
        return loaded

    def put_swr(self, key: str, value: bytes, *, ttl: float,
                stale_for: float, refresh_after: float | None = None) -> None:
        """Store a value with a stale-while-revalidate window. Behaves exactly
        like put(ttl=...) for plain GETs; additionally retains a bounded
        in-memory stale copy until ttl + stale_for so get_or_refresh can serve
        it while one revalidator reloads. refresh_after optionally marks the
        value refresh-due before its TTL expires. The window is coordination
        state: lost on restart, never durable, never visible to plain GET."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY or len(value) > MAX_VALUE:
            raise KuttiDBError("invalid swr put")
        ttl_ms = int(ttl * 1000) if ttl > 0 else 0
        stale_ms = int(stale_for * 1000) if stale_for > 0 else 0
        refresh_ms = int(refresh_after * 1000) if refresh_after else 0
        if ttl_ms <= 0 or stale_ms <= 0:
            raise KuttiDBError("put_swr requires ttl > 0 and stale_for > 0")
        if stale_ms > 7 * 24 * 3600 * 1000 or refresh_ms > 7 * 24 * 3600 * 1000:
            raise KuttiDBError("swr windows must be <= 7 days")
        # raw frame: [op][klen:2][vlen:4][ttl:4][stale:4][refresh:4][key][value]
        # (matches the server's PUT_TTL convention: metadata precedes the key)
        self.sock.sendall(struct.pack("<BHIIII", OP_PUT_SWR, len(kb),
                                      len(value), ttl_ms, stale_ms, refresh_ms)
                          + kb + value)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("swr put refused")
    def get_or_refresh(self, key: str, *, lease: float = 5.0) -> dict:
        """Stale-while-revalidate read. States: 'value' (fresh), 'refresh'
        (fresh, but the refresh-ahead window is due), 'stale' (expired value
        served from the retained copy), 'negative', 'claimed', or 'wait'.
        'value'/'stale'/'refresh' carry the value bytes; states 'stale' and
        'refresh' also carry ``holder``: True means this caller holds the
        revalidation lease and must reload (via put_swr + release_claim),
        False means another caller is already revalidating and the value can
        be used as-is."""
        kb = key.encode()
        if not kb or len(kb) > MAX_KEY:
            raise KuttiDBError("invalid singleflight key")
        if lease < 0 or lease * 1000 > 60000:
            raise KuttiDBError("lease must be 0..60 seconds")
        self._send(OP_SF_GET_OR_REFRESH, kb,
                   _U32.pack(max(1, int(lease * 1000))))
        status, response = self._recv_response()
        return self._swr_result(status, response)

    def get_or_load_swr(self, key: str, loader, *, ttl: float = 60.0,
                        stale_for: float = 300.0,
                        refresh_after: float | None = None,
                        lease: float = 5.0, wait: float = 10.0):
        """Cache-aside with native single-flight plus stale-while-revalidate:
        fresh values answer immediately, an expired value is served stale
        while exactly one caller revalidates (the holder flag), and a
        refresh-ahead window revalidates before expiry. The loader returns
        the value bytes, or None for a cached negative answer. Revalidation
        re-arms the stale window through put_swr."""
        r = self.get_or_refresh(key, lease=lease)
        if r["state"] == "value":
            return r["value"]
        if r["state"] == "negative":
            return None
        if r["state"] in ("stale", "refresh"):
            if not r.get("holder"):
                # another caller holds the lease: use the served value
                return r.get("value")
            # this caller holds the revalidation lease: load below
        elif r["state"] == "wait":
            for _ in range(3):
                w = self.wait_for_key(key, timeout=wait)
                if w["state"] == "value":
                    return w["value"]
                if w["state"] == "negative":
                    return None
                if w["state"] == "timeout":
                    return None
                r = self.get_or_refresh(key, lease=lease)
                if r["state"] == "value":
                    return r["value"]
                if r["state"] == "negative":
                    return None
                if r["state"] in ("stale", "refresh"):
                    return r.get("value")
                if r["state"] == "claimed":
                    break
        elif r["state"] == "claimed":
            pass  # this caller holds the lease: load below
        else:  # 'released' or 'lost': one bounded retry of the whole cycle
            r = self.get_or_refresh(key, lease=lease)
            if r["state"] == "value":
                return r["value"]
            if r["state"] == "negative":
                return None
            if r["state"] in ("stale", "refresh") and not r.get("holder"):
                return r.get("value")
        if r["state"] not in ("claimed", "stale", "refresh"):
            return None
        try:
            loaded = loader()
        except Exception:
            self.release_claim(key)
            raise
        if loaded is None:
            self.put_and_release(key, b"", ttl=ttl, negative=True)
            return None
        self.put_swr(key, loaded, ttl=ttl, stale_for=stale_for,
                     refresh_after=refresh_after)
        self.release_claim(key)
        return loaded

    # -- durable partitioned streams ------------------------------------------

    def stream_list(self) -> list[dict]:
        """Inventory of declared topics: partitions, retained records and
        bytes. Read-only snapshot, bounded at 256 entries."""
        self._send(OP_STREAM_LIST, b"", b"")
        status, response = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("stream list failed")
        n = struct.unpack("<H", response[:2])[0]
        out, at = [], 2
        for _ in range(n):
            tlen = struct.unpack("<H", response[at:at + 2])[0]
            topic = response[at + 2:at + 2 + tlen].decode()
            partitions, records, bts = struct.unpack("<IQQ", response[at + 2 + tlen:at + 22 + tlen])
            out.append({"topic": topic, "partitions": partitions,
                        "records": records, "bytes": bts})
            at += 22 + tlen
        return out

    def stream_group_list(self) -> list[dict]:
        """Inventory of consumer groups: topic, group, generation, live
        members. Read-only snapshot, bounded at 256 entries."""
        self._send(OP_STREAM_GROUP_LIST, b"", b"")
        status, response = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("stream group list failed")
        n = struct.unpack("<H", response[:2])[0]
        out, at = [], 2
        for _ in range(n):
            tlen = struct.unpack("<H", response[at:at + 2])[0]
            topic = response[at + 2:at + 2 + tlen].decode()
            glen = struct.unpack("<H", response[at + 2 + tlen:at + 4 + tlen])[0]
            group = response[at + 4 + tlen:at + 4 + tlen + glen].decode()
            generation, members = struct.unpack(
                "<QI", response[at + 4 + tlen + glen:at + 16 + tlen + glen])
            out.append({"topic": topic, "group": group,
                        "generation": generation, "members": members})
            at += 16 + tlen + glen
        return out

    def stream_declare(self, topic: str, *, partitions: int = 1,
                       max_bytes: int = 0, max_age: float | None = None) -> None:
        """Declare a durable, partitioned stream. Retention is bounded by
        max_bytes and/or max_age; zero means unlimited for that dimension."""
        tb = topic.encode()
        if not tb or len(tb) > 255 or not 1 <= partitions <= 256 or max_bytes < 0:
            raise KuttiDBError("invalid stream declaration")
        age_ms = 0 if max_age is None else int(max_age * 1000)
        if age_ms < 0:
            raise KuttiDBError("invalid stream retention age")
        self._send(OP_STREAM_DECLARE, tb, _U32.pack(partitions) +
                   _U64.pack(max_bytes) + _U64.pack(age_ms))
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("stream declaration failed")

    def stream_append(self, topic: str, value: bytes, *, key: bytes | str = b"",
                      partition: int | None = None) -> dict:
        """Append one record. A keyed append selects a stable partition;
        an explicit partition overrides key selection."""
        tb = topic.encode()
        kb = key.encode() if isinstance(key, str) else key
        hint = 0xffffffff if partition is None else partition
        if not tb or len(tb) > 255 or len(kb) > 65535 or len(value) > MAX_VALUE or not 0 <= hint < 1 << 32:
            raise KuttiDBError("invalid stream append")
        self._send(OP_STREAM_APPEND, tb, _U32.pack(hint) + _U16.pack(len(kb)) + kb + value)
        status, response = self._recv_response()
        if status != ST_OK or len(response) != 16:
            raise KuttiDBError("stream append failed")
        partition_id, offset = struct.unpack("<QQ", response)
        return {"partition": partition_id, "offset": offset}

    def stream_append_many(self, topic: str, items, *, partition: int | None = None) -> list[dict]:
        """Append up to 1,024 records durably in one server operation.

        Each item is either ``value`` or ``(key, value)``. Like
        :meth:`stream_append`, an explicit partition overrides keyed routing.
        """
        tb = topic.encode()
        hint = 0xffffffff if partition is None else partition
        try:
            entries = list(items)
        except TypeError as exc:
            raise KuttiDBError("invalid stream batch") from exc
        if not tb or len(tb) > 255 or not entries or len(entries) > 1024 or not 0 <= hint < 1 << 32:
            raise KuttiDBError("invalid stream batch")
        body = bytearray(_U32.pack(hint) + _U32.pack(len(entries)))
        for item in entries:
            if isinstance(item, tuple) and len(item) == 2:
                key, value = item
            else:
                key, value = b"", item
            key = key.encode() if isinstance(key, str) else key
            if not isinstance(key, bytes) or not isinstance(value, bytes) or len(key) > 65535 or len(value) > MAX_VALUE:
                raise KuttiDBError("invalid stream batch item")
            body += _U16.pack(len(key)) + _U32.pack(len(value)) + key + value
        # The server's native stream WAL caps a complete durable record at 64 MiB.
        if len(body) + len(tb) + 24 * len(entries) > MAX_VALUE:
            raise KuttiDBError("stream batch exceeds 64 MiB durable record limit")
        self._send(OP_STREAM_APPEND_BATCH, tb, bytes(body))
        status, response = self._recv_response()
        if status != ST_OK or len(response) < 4:
            raise KuttiDBError("stream batch append failed")
        count = _U32.unpack(response[:4])[0]
        if count != len(entries) or len(response) != 4 + count * 16:
            raise KuttiDBError("invalid stream batch response")
        return [{"partition": partition_id, "offset": offset}
                for partition_id, offset in struct.iter_unpack("<QQ", response[4:])]

    def stream_fetch(self, topic: str, *, partition: int = 0, offset: int = 0,
                     max_records: int = 100) -> list[dict]:
        if not 0 <= partition < 1 << 32 or offset < 0 or not 1 <= max_records <= 1024:
            raise KuttiDBError("invalid stream fetch")
        caps = self.capabilities()["features"]
        keyed = bool(caps & CAP_STREAM_KEYS)
        self._send(OP_STREAM_FETCH_KEYS if keyed else OP_STREAM_FETCH, topic.encode(),
                   _U32.pack(partition) + _U64.pack(offset) + _U32.pack(max_records))
        status, response = self._recv_response()
        if status == ST_MISS:
            return []
        if status != ST_OK or len(response) < 4:
            raise KuttiDBError("stream fetch failed")
        count = _U32.unpack(response[:4])[0]
        items, at = [], 4
        for _ in range(count):
            header = 14 if keyed else 12
            if at + header > len(response):
                raise KuttiDBError("invalid stream response")
            if keyed:
                record_offset, key_len, length = struct.unpack("<QHI", response[at:at + 14])
                at += 14
            else:
                record_offset, length = struct.unpack("<QI", response[at:at + 12])
                key_len = 0
                at += 12
            if at + key_len + length > len(response):
                raise KuttiDBError("invalid stream response")
            item = {"offset": record_offset, "value": response[at + key_len:at + key_len + length]}
            if keyed:
                item["key"] = response[at:at + key_len]
            items.append(item)
            at += key_len + length
        if at != len(response):
            raise KuttiDBError("invalid stream response")
        return items

    def stream_commit(self, topic: str, group: str, partition: int, offset: int) -> None:
        gb = group.encode()
        if not gb or len(gb) > 255 or not 0 <= partition < 1 << 32 or offset < 0:
            raise KuttiDBError("invalid stream commit")
        self._send(OP_STREAM_COMMIT, topic.encode(), _U16.pack(len(gb)) + gb +
                   _U32.pack(partition) + _U64.pack(offset))
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("stream offset commit failed")

    def stream_commit_batch(self, topic: str, group: str, commits) -> None:
        """Commit offsets for several partitions of one topic/group with a
        single durability round trip (server capability bit 13). `commits`
        is an iterable of (partition, offset) pairs applied in order."""
        gb = group.encode()
        if not gb or len(gb) > 255:
            raise KuttiDBError("invalid stream commit batch")
        commits = list(commits)
        if not 1 <= len(commits) <= 256:
            raise KuttiDBError("batch size must be 1-256")
        payload = _U16.pack(len(gb)) + gb + _U32.pack(len(commits))
        for partition, offset in commits:
            if not 0 <= partition < 1 << 32 or offset < 0:
                raise KuttiDBError("invalid stream commit batch entry")
            payload += _U32.pack(partition) + _U64.pack(offset)
        self._send(OP_STREAM_COMMIT_BATCH, topic.encode(), payload)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("stream offset commit batch failed")

    def stream_group_join(self, topic: str, group: str, *, lease: float = 30.0) -> StreamAssignment:
        """Join or heartbeat a consumer group. Returns a StreamAssignment
        holding this connection's deterministic partition assignment and the
        group's current generation; call again before the lease expires and
        whenever the assignment needs refreshing. A generation change tells
        the member a rebalance happened: drain partitions that left the
        assignment and stop committing them."""
        gb = group.encode()
        lease_ms = int(lease * 1000)
        if not gb or len(gb) > 255 or not 1 <= lease_ms <= 60000:
            raise KuttiDBError("invalid stream group join")
        self._send(OP_STREAM_GROUP_JOIN, topic.encode(),
                   _U16.pack(len(gb)) + gb + _U32.pack(lease_ms))
        status, response = self._recv_response()
        if status != ST_OK or len(response) < 4:
            raise KuttiDBError("stream group join failed")
        count = _U32.unpack(response[:4])[0]
        if count > 256 or len(response) < 4 + count * 4:
            raise KuttiDBError("invalid stream group assignment")
        parts = list(struct.unpack("<" + "I" * count, response[4:4 + count * 4])) if count else []
        if len(response) == 4 + count * 4 + 8:
            generation = _U64.unpack(response[4 + count * 4:])[0]
        elif len(response) == 4 + count * 4:
            # Server without CAP_STREAM_GEN: no generation is reported.
            generation = 0
        else:
            raise KuttiDBError("invalid stream group assignment")
        return StreamAssignment(parts, generation)

    def stream_group_leave(self, topic: str, group: str) -> None:
        """Leave a consumer group gracefully after draining in-flight work.
        The group rebalances immediately instead of waiting for the lease to
        expire. Leaving a group this connection never joined is an error."""
        gb = group.encode()
        if not gb or len(gb) > 255:
            raise KuttiDBError("invalid stream group leave")
        self._send(OP_STREAM_GROUP_LEAVE, topic.encode(), _U16.pack(len(gb)) + gb)
        status, _ = self._recv_response()
        if status != ST_OK:
            raise KuttiDBError("stream group leave failed")

    def stream_group_offset(self, topic: str, group: str, partition: int) -> int | None:
        gb = group.encode()
        self._send(OP_STREAM_GROUP_OFFSET, topic.encode(),
                   _U16.pack(len(gb)) + gb + _U32.pack(partition))
        status, response = self._recv_response()
        if status == ST_MISS:
            return None
        if status != ST_OK or len(response) != 8:
            raise KuttiDBError("stream group offset failed")
        return _U64.unpack(response)[0]

    def stream_group_lag(self, topic: str, group: str, partition: int) -> int | None:
        """Return the number of records between a group's committed next
        offset and the current end of a partition."""
        gb = group.encode()
        if not gb or len(gb) > 255 or not 0 <= partition < 1 << 32:
            raise KuttiDBError("invalid stream group lag request")
        self._send(OP_STREAM_GROUP_LAG, topic.encode(),
                   _U16.pack(len(gb)) + gb + _U32.pack(partition))
        status, response = self._recv_response()
        if status == ST_MISS:
            return None
        if status != ST_OK or len(response) != 8:
            raise KuttiDBError("stream group lag failed")
        return _U64.unpack(response)[0]
