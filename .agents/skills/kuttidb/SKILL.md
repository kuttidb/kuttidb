---
name: kuttidb
description: Build, run, and integrate KuttiDB — start or stop the server with any CLI flag or managed local mode, connect and configure clients (Python, Node.js, Go, Java, Rust, C embedded, CLI), set up auth/TLS/metrics, and call the Management API v1. Use when an agent implements against, tests, operates, or debugs KuttiDB in this repository.
---

# KuttiDB: server, clients, and Management API

KuttiDB is one C binary that combines an evictable cache (Keyspace), durable
work queues, direct/fanout/topic exchanges, replayable partitioned streams,
single-flight stampede protection, and an optional authenticated Management
API. One server, one data directory, macOS and Linux only.

## Orientation — where things live

| Path | Contents |
|---|---|
| `src/server.c` | Server CLI parsing, event loops, metrics/ready HTTP |
| `src/managed_launcher.c` | `kuttidb ensure` managed local launcher |
| `src/embed.h`, `src/kuttidb.h` | C embedded shared-memory and core library API |
| `src/kuttidb_client.py` | Canonical Python SDK source (`clients/python/prepare.py` copies it into the package; the staged `client.py` is generated, never edit it directly) |
| `src/job_state.c/.h`, `src/job_completion.c/.h` | Durable state, receipts, and atomic job completion core |
| `src/kuttidb_client.h/.c` | Public C companion client (`libkuttidb_client`; network jobs, not shared memory) |
| `clients/nodejs/`, `clients/go/`, `clients/java/`, `clients/rust/` | Other SDKs |
| `openapi/management-v1.yaml` | Versioned Management API contract |
| `docs/design/PROTOCOL.md` | Binary wire protocol, CLI flags, limits |
| `docs/api/MANAGEMENT_API.md` | Management API resources and security model |

Run `./kuttidb --features` to check a binary's build features (e.g.
`tls=openssl`, `management-api=v1`).

## 1. Build and start the server

```sh
make                      # builds kuttidb, kuttidb-bench, libkuttidb_embed
./kuttidb 7379 kuttidb.wal          # minimal: loopback TCP :7379
./kuttidb 7379 kuttidb.wal 100 /tmp/kuttidb.sock   # + Unix socket, fsync 100ms
make TLS=0                # dependency-free plaintext build (no TLS)
```

Default listener is `127.0.0.1:7379`. Keep WAL files for recovery: the cache,
queues, and streams each use their own log.

## 2. Server parameter reference

### Positional arguments

```
./kuttidb [PORT [WAL [FSYNC_MS [UNIX_PATH [MAX_MEM_MB [EMBED_PATH]]]]]]
```

| # | Arg | Default | Meaning |
|---|---|---|---|
| 1 | `PORT` | `7379` | TCP port on `127.0.0.1` |
| 2 | `WAL` | — | Cache WAL path; `-` disables cache persistence |
| 3 | `FSYNC_MS` | `100` | Background fsync interval in ms; `0` disables fsync |
| 4 | `UNIX_PATH` | — | Extra listener on a Unix domain socket |
| 5 | `MAX_MEM_MB` | unlimited | Memory budget; random (memcached-style) eviction over budget |
| 6 | `EMBED_PATH` | — | Shared-memory region path enabling embedded mode |

Unknown args print usage and exit `2`. `--help` and `--features` exit `0`.

### Data-plane flags

| Flag | Default | Notes |
|---|---|---|
| `--bind IPv4` | `127.0.0.1` | Non-loopback bind refused without `--auth-file` |
| `--auth-file PATH` | off | Regular server-owned `0600` file; token 1..1024 bytes; trailing newline ignored |
| `--tls-cert PATH` / `--tls-key PATH` | off | Must be given together; TLS ≥ 1.2 on every TCP listener; key must be server-owned, no group/other bits |
| `--durability periodic\|always` | `periodic` | `periodic` batches WAL writes at `FSYNC_MS` (acknowledged writes in the window can be lost on OS/power failure); `always` fsyncs before acknowledging each mutation or complete batch |
| `--fsync-ms N` | `100` | Overrides positional `FSYNC_MS`; `0` disables |
| `--max-value-mb N` | `64` | Per-value limit in MiB |
| `--max-batch-mb N` | `64` | Batch size limit in MiB |
| `--max-clients N` | `1024` | Concurrent client connections |
| `--threads N` | min(CPUs, 4) | 1..64 event loops |
| `--embed-region-mb N` | `1024` | Sparse embed region size; minimum 16 |
| `--queue-wal PATH\|-` | `<WAL>.queues` | Durable queue WAL; `-` disables durable declarations (non-durable queues still allowed) |
| `--job-completion` | off | Enables atomic job completion + the `durable` state keyspace; requires a durable Queue WAL (refuses otherwise with exit 2). Capability bit 16. |
| `--job-state-max-memory-mb N` | `64` | 1..65536; reaching it rejects new state growth, never evicts. |
| `--job-receipts-max-memory-mb N` | `64` | 1..65536; receipt/index budget, never evicts unexpired receipts. |
| `--job-receipts-max-count N` | `100000` | 1..100000000; additional receipt count ceiling. |
| `--job-receipt-retention-ms N` | `86400000` | 1000..315360000000; per-receipt retry window assigned at commit. |
| `--job-completion-max-bytes N` | `131072` | 1024..67108864; aggregate canonical operation bound (state + completion). |
| `--stream-wal PATH\|-` | `<WAL>.streams` | Durable stream WAL; `-` disables stream declarations (streams are durable-only — no volatile fallback) |
| `--no-tcp` | off | Disable the TCP listener (Unix/embed only) |

### Metrics listener (optional)

| Flag | Default | Notes |
|---|---|---|
| `--metrics-bind IPv4:PORT` | off | Serves `GET /metrics` (Prometheus text) and `GET /ready` |
| `--metrics-token-file PATH` | off | Optional bearer token for the metrics listener |

`/ready` returns 200 only while cache, queue, and stream engines are all
writable; a latched WAL failure (`*_wal_failed` in STATS) makes it 503.

### Management API listener (optional, disabled by default)

| Flag | Default | Notes |
|---|---|---|
| `--admin-bind IPv4:PORT` | off | Enables the Management API under `/api/admin/v1` |
| `--admin-token-file PATH` | required | Bearer token file; `0600`, server-owned |
| `--admin-audit-log PATH` | required | JSONL audit trail; every mutation is audited before dispatch, fail-closed |
| `--admin-allow-origin ORIGIN` | none | Repeatable exact origin (max 16); never a wildcard; without it no CORS headers are returned |
| `--admin-tls-cert PATH` / `--admin-tls-key PATH` | off | Required for non-loopback administration (separate pair from data TLS) |
| `--admin-max-clients N` | `16` | Normal admin dispatcher capacity |
| `--admin-max-tail-clients N` | `4` | SSE tail worker capacity |
| `--admin-session-limit N` | `256` | Concurrent group sessions |
| `--admin-job-limit N` | `32` | Bounded async maintenance jobs |

Hard rules: non-loopback plaintext administration is refused; the server fails
startup if the token file or audit log is unsafe, or the bind fails.

### Managed/internal flags (set by `kuttidb ensure`, not by hand)

`--data-dir ABS_PATH`, `--listen unix:ABS_PATH|tcp:127.x.x.x:PORT`,
`--lifecycle standalone|managed-idle`, `--idle-timeout-ms N`,
`--startup-orphan-timeout-ms N`, `--ready-fd N`, `--max-memory-mb N`,
`--unix-path PATH`. `--listen tcp:` accepts only a literal IPv4 loopback
endpoint; DNS names are never resolved.

## 3. Managed local mode — `kuttidb ensure`

One application-owned local instance from a data directory. SDKs call this
automatically; you can also run it directly:

```sh
kuttidb ensure --data-dir /abs/path/data --listen /abs/path/data/kuttidb.sock --json
```

- `--data-dir` must be absolute; a Unix `--listen` must be exactly
  `<data-dir>/kuttidb.sock`; TCP `--listen` must be literal loopback.
- Extra settings are an allowlist only (no generic argv pass-through):
  `--durability`, `--auth-file`, `--max-memory-mb`, `--max-value-mb`,
  `--max-batch-mb`, `--max-clients`, `--threads`, `--fsync-ms`,
  `--queue-wal`, `--stream-wal`, `--tls-cert`, `--tls-key`,
  `--metrics-bind`, `--metrics-token-file`, `--admin-bind`,
  `--admin-token-file`, `--admin-allow-origin`, `--admin-tls-cert`,
  `--admin-tls-key`, `--admin-audit-log`, `--admin-max-clients`,
  `--admin-max-tail-clients`, `--admin-session-limit`, `--admin-job-limit`,
  `--job-completion` (boolean), `--job-state-max-memory-mb`,
  `--job-receipts-max-memory-mb`, `--job-receipts-max-count`,
  `--job-receipt-retention-ms`, `--job-completion-max-bytes`,
  plus `--idle-timeout-ms`, `--startup-timeout-ms`,
  `--startup-orphan-timeout-ms`, `--json`.
- JSON result: `{"status": "started"|"existing"|"starting", "instance_id":
  "<32 hex>", "endpoint": ..., "log": "<data-dir>/kuttidb.log"}`.
- Exit codes: `64` configuration, `65` unsafe_path, `66` ownership,
  `67` startup, `68` timeout.
- Data-dir layout: `instance.id` (32-hex identity), `kuttidb.sock`,
  `kuttidb.log`, `.bootstrap.lock`.
- Lifecycle `managed-idle`: the server exits gracefully only after all native
  client connections have been closed for the idle grace period. Connects
  verify identity via the `SERVER_INFO` opcode; an endpoint held by a
  different server is rejected, never adopted.

## 4. Wire protocol essentials

Binary, little-endian, request/response over TCP or Unix; pipelining is safe.
When auth is configured, `AUTH` must be the first request on every connection.

- Statuses: `0x00` OK/HIT, `0x01` MISS, `0x02` ERROR. `0x02` is fail-closed:
  treat it as "the durable effect did not happen" — legacy atomic ops
  (`0x40`–`0x43`) can be in doubt across an interrupted commit window, and the
  job family (0x70+) carries a typed `[code:1][outcome:1][detail]` error body
  (`outcome`: 0 `not_committed`, 1 `unknown`).
- Layout: `[op:1][klen:2][vlen:4][key][value]`; max key 65535 bytes; values
  binary, server limit 64 MiB by default.
- Opcode ranges: `0x01`–`0x0C` cache/AUTH/HEALTH/CAPABILITIES/SERVER_INFO,
  `0x11`–`0x13` KV batches, `0x20`–`0x2F` queues, `0x30`–`0x33` exchanges,
  `0x40`–`0x43` atomic cache+message, `0x50`–`0x54` single-flight/SWR,
  `0x60`–`0x6C` streams, `0x70`–`0x77` atomic job completion (capability bit
  16, protocol 1.8+). Details in `docs/design/PROTOCOL.md`.
- `CAPABILITIES` returns a feature bitset; clients should require absent
  features explicitly rather than assume.
- `STATS` returns JSON with `mem_bytes`, `allocated_bytes`, `wal_failed`,
  `event_loops`, `event_backend` (`kqueue`/`epoll`), `durability`, queue /
  exchange / stream counters, single-flight/SWR counters, and — with
  `--job-completion` — `job_enabled`, `job_state_entries`, `job_state_bytes`,
  `job_receipts`, `job_receipt_bytes`, `job_completions`,
  `job_completions_replayed`, `job_id_conflicts`, `job_state_conflicts`,
  `job_delivery_rejects`, `job_receipt_gc`, and `job_wal_failed`.

## 5. Clients

### Install

| Language | Install | Source |
|---|---|---|
| Python | `python3 -m pip install --pre kuttidb` | `clients/python` |
| Node.js | `npm install @kuttidb/client@beta` | `clients/nodejs` |
| Go | `go get github.com/kuttidb/kuttidb/clients/go` | `clients/go` |
| Rust | `cargo add kuttidb@0.1.0` | `clients/rust` |
| Java | Maven `io.github.kuttidb:kuttidb-client:0.1.2` (Java 17+) | `clients/java` |
| C/C++ | `make` builds `libkuttidb_embed.dylib`/`.so` | `src/embed.h` |

In this repository without the packaged SDKs, import from source:
`sys.path.insert(0, "src"); from kuttidb_client import KuttiDBClient`
(`examples/python_examples.py` shows every pattern).

### Connect

```python
# Python — canonical surface
from kuttidb import KuttiDBClient
with KuttiDBClient(port=7379) as db: ...                          # TCP
with KuttiDBClient(unix_path="/run/kuttidb.sock") as db: ...      # Unix socket
with KuttiDBClient(port=7379, auth_token=t) as db: ...            # AUTH first
with KuttiDBClient(port=7379, tls=True, ca_file="ca.pem") as db: ...  # TLS
```

```js
// Node.js
const { Client } = require("@kuttidb/client");
const db = new Client({ host: "127.0.0.1", port: 7379, poolSize: 4,
                        token: "…", tls: false, socketPath: null });
```

```go
// Go
db, err := kuttidb.New("127.0.0.1:7379", 8)                  // pool of 8
db, err := kuttidb.NewAuthenticated(addr, 8, token)
db, err := kuttidb.NewTLS(addr, 8, token, tlsConfig)
```

```rust
// Rust
let mut db = kuttidb::Client::connect("127.0.0.1:7379")?;
let mut db = kuttidb::Client::connect_authenticated(addr, token)?;
let mut db = kuttidb::Client::connect_tls(addr, server_name)?;
let mut pool = kuttidb::Pool::new_authenticated(addr, 8, token)?;
pool.with(|db| db.put("k", b"v", None))?;
```

```java
// Java (built-in connection pool)
KuttiDBClient db = new KuttiDBClient("127.0.0.1", 7379);
KuttiDBClient db = new KuttiDBClient(host, port, authToken);
KuttiDBClient db = new KuttiDBClient(host, port, authToken, sslContext, poolSize);
```

### Method surface (Python names; other SDKs mirror them)

All of Python, Node.js, Go, Java, and Rust cover cache, KV batches, queues,
exchanges, atomic operations, streams, and atomic job completion.
Single-flight/SWR is not in the CLI. Naming: Node.js camelCase (`putMany`,
`queueAck`), Go PascalCase methods with options structs (`QueueOptions{…}`),
Java builder-style options (`new QueueOptions().durable(true)`), Rust
snake_case with `std::time::Duration` TTLs. The C companion
(`libkuttidb_client`, `src/kuttidb_client.h`) exposes
`kuttidb_job_consume`, `kuttidb_job_complete`, `kuttidb_job_completion`,
`kuttidb_state_get/put/delete`, `kuttidb_durable_operation`,
`kuttidb_queue_manifest` over TCP/Unix with AUTH and verified TLS.

**Cache:** `put(key, value, ttl=None)` · `get(key)` · `delete(key)` ·
`stats()` · `health()` · `capabilities()` · `put_many([(k, v), …])` ·
`get_many([keys])`

**Queues (at-least-once; ACK after work succeeds):**
`queue_declare(name, durable=True, max_depth=0, dead_letter_queue=None,
max_deliveries=None)` · `queue_list()` · `queue_stats(name)` ·
`queue_publish(name, value, ttl=None)` · `queue_publish_batch(name, values)` ·
`queue_consume(name, visibility=30.0)` · `queue_consume_batch(name,
max_count, visibility=30.0)` · `queue_consume_as(name, consumer,
visibility=30.0)` · `queue_consumer_register(consumer)` ·
`queue_consumer_unregister(consumer)` · `queue_ack(name, delivery_tag)` ·
`queue_ack_batch(name, tags)` · `queue_nack(name, delivery_tag,
requeue=True, delay=0)` · `queue_nack_batch(name, tags, requeue=True)` ·
`queue_prefetch(count)` · `queue_cancel()`

**Exchanges and routing:** `exchange_declare(name, type="direct",
durable=True, alternate_exchange=None)` (types: `direct`, `fanout`,
`topic`) · `exchange_bind(exchange, queue, routing_key="")` ·
`exchange_unbind(...)` · `exchange_publish(exchange, routing_key, value,
ttl=None)` — returns routed-copy count; unroutable publishes raise MISS.

**Atomic cache + message (one durable commit id):**
`put_and_publish(key, value, exchange, routing_key="", ttl=None)` ·
`put_and_enqueue(key, value, queue, ttl=None)` ·
`delete_and_publish(key, exchange, routing_key="", message=None)` ·
`update_and_emit(key, value, exchange, routing_key="", ttl=None)` — the key
must already exist; missing key commits nothing. Requires cache persistence
and a durable target.

**Single-flight / SWR:** `get_or_claim(key, lease=5.0)` ·
`wait_for_key(key, timeout=10.0)` · `put_and_release(key, value, ttl=None,
negative=False)` · `release_claim(key)` · `get_or_load(key, loader, ttl=60.0)`
· `put_swr(key, value, ttl, stale_for, refresh_after=None)` ·
`get_or_refresh(key, lease=5.0)` · `get_or_load_swr(key, loader, ttl=60.0, …)`

**Streams:** `stream_declare(topic, partitions=1, max_bytes=0, max_age=None)`
· `stream_list()` · `stream_append(topic, value, key=b"", partition=None)` ·
`stream_append_many(topic, items, partition=None)` · `stream_fetch(topic,
partition=0, offset=0, max_records=100)` · `stream_commit(topic, group,
partition, offset)` · `stream_commit_batch(topic, group, commits)` ·
`stream_group_join(topic, group, lease=30.0)` → `StreamAssignment(partitions,
generation)` · `stream_group_leave(topic, group)` ·
`stream_group_offset(topic, group, partition)` ·
`stream_group_lag(topic, group, partition)` · `stream_group_list()`

`stream_commit` only advances a group's next offset; a delayed lower commit
is a successful no-op. Use the generation-checked reset API for an intentional
rewind.

On generation change with a changed assignment: finish in-flight work for
partitions still owned, drain the rest; commits for lost partitions are
refused by the server.

**Atomic job completion (capability bit 16; requires `--job-completion` on the
server and a durable queue):**
`queue_manifest()` → per-queue stable identity (incarnation, depth, limits) ·
`job_consume(queue, consumer, visibility=30.0)` → `JobDelivery`
(store id, queue + incarnation, stable message id, attempts, redelivered,
lease deadline, opaque one-use `proof`, payload) ·
`job_complete(intent, proof)` → `JobCompletionResult(commit_id, state_version,
output_message_id, completed_at, receipt_expires_at, replayed)` — commits the
durable-state PUT, the input ACK, the optional output publish, and the receipt
together; **never ACK separately after a success** ·
`job_completion(operation_id)` → retained receipt lookup (no proof needed,
works after restart; miss = "not retained", not "never executed") ·
`state_get(key)` → `{version, commit_id, value}` ·
`state_put(key, value, expected_version=…, operation_id=…)` → mutation
receipt (0 = create-only; positive = exact CAS) ·
`state_delete(key, expected_version=…, operation_id=…)` → receipt ·
`durable_operation(operation_id)` → state-mutation receipt lookup.
Intents are serializable (`to_json`/`from_json` — 64-bit fields as lossless
decimal strings, bytes as Base64); the operation id is generated once at
composition and must be reused on retries. Typed errors carry
`code` + `outcome` (`not_committed` vs `unknown`):
`unsupported_feature`, `validation_failed`, `request_too_large`,
`idempotency_conflict`, `state_version_conflict`, `delivery_expired`,
`delivery_not_owned`, `resource_exhausted`, `operation_in_doubt`,
`persistence_unavailable`, `not_found` (`operation_in_progress` reserved).
Guide: `docs/guides/ATOMIC_JOB_COMPLETION.md`.

### CLI client — `kuttidb-cli`

```
put KEY VALUE [-] | get KEY | del KEY | mget KEY… | mput
queues | topics | groups | stats | health | capabilities | manifest
consumer-register NAME | consumer-unregister NAME
job-consume QUEUE CONSUMER [--visibility S] [--output FILE]
job-complete --delivery FILE [--request FILE] | job-completion UUID
state-get KEY | state-put KEY --expected-version N [--value SPEC]
state-delete KEY --expected-version N | durable-operation UUID
  -H/--host (env KUTTIDB_HOST)      -p/--port (env KUTTIDB_PORT, default 7379)
  --auth-file (env KUTTIDB_AUTH_FILE)   --tls (env KUTTIDB_TLS=1)
  --ca-file (env KUTTIDB_CA_FILE)   --server-name NAME
  --unix-path PATH (env KUTTIDB_UNIX)
```

Value SPEC: `-` stdin, `b64:...` Base64, `@FILE` file bytes, else literal.
New structured commands emit JSON with lossless decimal-string ids;
delivery/intent files (`--output`, `--request`) are owner-only `0600` and
carry the sensitive proof/payload — never printed to routine output.

Exit codes: `0` ok, `1` miss/not found, `2` connection/server/validation,
`3` conflict (idempotency or state version), `4` unknown outcome
(`operation_in_doubt`).

## 6. Client configuration reference

### Python `KuttiDBClient(...)`

```python
KuttiDBClient(host="127.0.0.1", port=7379, timeout=5.0,
              auth_token=None,            # str | bytes; 1..1024 bytes
              tls=False,                  # bool or ssl.SSLContext
              ca_file=None, server_hostname=None,
              unix_path=None,             # Unix-socket transport (no TLS)
              server=None)                # ServerParams → managed lifecycle
```

Plain construction is connect-only: it never starts or stops a server.

### Managed local mode (Python)

```python
with KuttiDBClient.managed(data_dir="./data/kuttidb", idle_timeout=60) as db:
    db.put("greeting", b"hello")
```

`managed(data_dir, idle_timeout=60.0, startup_timeout=10.0, executable=None,
auth_token=None, host="127.0.0.1", port=7379, **settings)` — `settings` are
`ServerParams` fields. Defaults to an owner-only Unix socket
(`<data_dir>/kuttidb.sock`); `transport="tcp"` requires a literal
`127.x.x.x` host. DNS and non-loopback endpoints are rejected.

`ServerParams` (typed, frozen dataclass): `data_dir`, `executable`,
`transport="unix"`, `idle_timeout=60.0`, `startup_timeout=10.0`,
`startup_orphan_timeout=60.0`, `durability="periodic"`, `max_memory_mb`,
`auth_file`, `fsync_ms`, `max_value_mb`, `max_batch_mb`, `max_clients`,
`threads`, `queue_wal`, `stream_wal`, `tls_cert`/`tls_key` (TCP only,
together), `metrics_bind`/`metrics_token_file`, `admin_bind`/
`admin_token_file`/`admin_audit_log` (both required with `admin_bind`),
`admin_allow_origins=()`, `admin_tls_cert`/`admin_tls_key`,
`admin_max_clients`, `admin_max_tail_clients`, `admin_session_limit`,
`admin_job_limit`, `job_completion=False` (requires an explicit `queue_wal`),
`job_state_max_memory_mb`, `job_receipts_max_memory_mb`,
`job_receipts_max_count`, `job_receipt_retention_ms`,
`job_completion_max_bytes`. Token values are never passed to the launcher —
only file paths. Every SDK's managed options expose the same job settings
(Node `Client.managed`, Go `ManagedOptions`, Java `ManagedServerOptions`,
Rust `ManagedOptions`); all forward them through the `kuttidb ensure`
allowlist.

Executable discovery order: `executable` param → `KUTTIDB_SERVER` env →
`kuttidb` on PATH. After connecting, the SDK verifies the instance identity
(`SERVER_INFO`) against `<data_dir>/instance.id`.

Error types: `KuttiDBError`, `ManagedServerConfigurationError`,
`ManagedServerStartupError(category, log_path)`,
`ManagedServerStartupTimeout`, `ManagedServerEndpointOccupied`,
`ManagedServerInstanceMismatch`.

### Python local transports

- `LocalKuttiDB(embed_path=…, …client params)` — tries CEMBv3 shared memory
  first (when `embed_path` is given and no managed server is requested), and
  falls back to Unix/TCP/TLS. `.transport` reports
  `shared_memory` | `unix_socket` | `tcp`. It never retries a failed direct
  write over the network.
- `KuttiEmbed(embed_path)` — zero-syscall shared-memory client; requires
  `libkuttidb_embed` (built by `make`) on the library path. Cache ops only
  (`put`/`get`/`delete`/`count`/`memusage`); use socket clients for messaging.
- C API: `kuttidb_embed_open/put/delete/close`, `kuttidb_embed_create(_sized)`
  / `attach` / `detach` — see `src/embed.h` and `src/kuttidb.h`.

### Other SDK configuration

| SDK | Configuration |
|---|---|
| Node.js | `new Client({ host, port, socketPath, poolSize=4, token, tls })`; `Client.managed({ dataDir, transport="unix", idleTimeout, startupTimeout, executable, host, port, token })` (eager connect + identity check) |
| Go | `kuttidb.New(addr, poolSize)`, `NewAuthenticated`, `NewTLS(addr, poolSize, token, *tls.Config)`; `kuttidb.NewManaged(kuttidb.ManagedOptions{DataDir, Executable, Transport, Host, Port, IdleTimeout, StartupTimeout, Token, PoolSize})`; `EmbedDB` via cgo for shared memory |
| Rust | `Client::connect*` variants above; `Pool::new*/connect_managed(options, size)` + `pool.with(…)`; `ManagedOptions { data_dir, executable, transport: ManagedTransport, idle_timeout, startup_timeout, auth_token }` |
| Java | `KuttiDBClient(host, port[, authToken[, SSLContext[, poolSize]]])`; options via builders (`QueueOptions`, `ExchangeOptions`, `StreamOptions`); managed mode supported |
| CLI | Env vars `KUTTIDB_HOST`, `KUTTIDB_PORT`, `KUTTIDB_AUTH_FILE`, `KUTTIDB_TLS`, `KUTTIDB_CA_FILE` |

Managed mode is for one machine and one application-owned data directory —
use standalone mode for containers, service managers, and remote servers.
Never replay an operation after a connection failure without deduplication.

## 7. Operational HTTP surfaces

The binary protocol stays the data protocol. Two optional HTTP surfaces exist:

### Metrics listener

`--metrics-bind 127.0.0.1:9099` serves Prometheus text at `/metrics`
(series include `kuttidb_queue_depth{name}`, `kuttidb_topic_*`,
`kuttidb_stale_entries`, `kuttidb_stale_serves`, `kuttidb_refresh_serves`)
and a readiness probe at `/ready`. Add `--metrics-token-file` to require a
bearer token.

### Management API v1 (`--admin-bind`)

Authenticate every request with `Authorization: Bearer <admin-token>`:

```sh
umask 077
printf '%s\n' 'replace-with-a-long-random-token' > admin.token
./kuttidb 7379 kuttidb.wal \
  --admin-bind 127.0.0.1:7380 \
  --admin-token-file admin.token \
  --admin-audit-log admin-audit.jsonl

curl -H "Authorization: Bearer $(cat admin.token)" \
  http://127.0.0.1:7380/api/admin/v1/status
```

Conventions: all paths under `/api/admin/v1`; collections take `?limit=N`
(default 100, cap 500) and return `count`, `limit`, `next_cursor`,
`snapshot_revision`, `weakly_consistent` — cursors are opaque, scoped, and
expire (restarts invalidate them); resource IDs are `b64u:` URL-safe Base64;
bodies are returned only in explicit Base64 fields; mutations take an
`Idempotency-Key`; conditional mutations use `If-Match: "q-<rev>"` /
`"s-<rev>"` / `"g-<generation>"` plus `X-KuttiDB-Confirm` for destructive
ops; errors are `{"error":{"code":"...","message":"..."}}` with stable codes
(`412 precondition_failed`, `428 precondition_required`, `409 conflict`,
`410 gone`).

Resources (full schema: `openapi/management-v1.yaml`):

| Resource | Purpose |
|---|---|
| `GET /capabilities`, `GET /status` | Feature/engine/TLS/persistence discovery; bounded process and engine health |
| `GET /jobs`, `GET/DELETE /jobs/{id}` | Bounded async maintenance jobs (only queued jobs cancellable) |
| `GET /maintenance`, `POST /maintenance/{keyspace,queue,stream}-checkpoint`, `POST /maintenance/checkpoint-all` | Crash-safe engine checkpoints via the job worker |
| `GET /keyspaces`, `GET /keyspaces/default` | Aggregate Keyspace state |
| `GET/PUT/DELETE /keyspaces/default/entries/{id}`, `GET …/entries` | Exact binary entries; bounded metadata inventory (`prefix`, `expires=present\|none`, cursor) |
| `POST …/entries:batch-get`, `:batch-put`, `:batch-delete` | Ordered batches up to 100 (explicitly `atomic: false` today) |
| `POST …/claims`, `GET …/claims/{id}`, `POST …/claims/{id}:complete\|:release`, `POST …/entries/{id}:get-or-refresh` | Single-flight claims over the Management API (lease-bound, `kc:` IDs) |
| `GET/POST /queues`, `GET/PATCH/DELETE /queues/{id}` | Inventory, durable declaration; durable options are immutable; delete needs ETag + confirm |
| `GET /queues/{id}/messages[/{mid}]` | Non-mutating message browsing (`state=ready\|delayed\|in-flight`, `include=body`, bounded body budget, `body_omitted`) |
| `POST /queues/{id}/messages[`:batch]`, `POST /queues/{id}:purge` | Publish (Base64); purge needs `If-Match` + `X-KuttiDB-Confirm` (destructive) |
| `POST /queues/{id}/deliveries[`:batch`]`, `GET …/deliveries/{id}`, `POST …/deliveries:ack-batch\|:nack-batch` | Safe opaque deliveries (1–50 per disposition batch) |
| `GET/POST /queue-consumers`, `GET/DELETE /queue-consumers/{id}`, `POST …/deliveries` | Durable queue consumers without exposing native owner tokens |
| `POST /atomic-operations` | Tagged all-or-nothing Keyspace-plus-Queue/Routing operation |
| `GET/POST /streams`, `GET/PATCH/DELETE /streams/{id}` | Inventory, declaration, retention ceilings (`PATCH`), conditional delete (async job) |
| `GET /streams/{id}/partitions`, `POST …/records[`:batch`]`, `GET …/partitions/{p}/records[/{offset}]`, `POST …/partitions/{p}:truncate` | Offsets, Base64 appends, bounded fetch pages, ETag-confirmed truncation |
| `GET /streams/{id}/partitions/{p}/records:tail?offset=N` | Authenticated bounded SSE tail (`Accept: text/event-stream`), rate-limited, short lifetime, `offset_out_of_range` event |
| `GET/POST /routing/routers`, `GET/PATCH/DELETE /routing/routers/{id}[/routes…]`, `POST /routing/default/messages` | Router topology (direct/fanout/topic via the console API), routes, default-routing publish |
| `GET /consumer-groups`, `GET /streams/{id}/consumer-groups[/{gid}][/members\|/offsets]`, `PUT …/offsets/{p}`, `POST …/offsets:batch`, `POST …:reset-offsets` | Group state, lag, generation-checked offset commits and resets |
| `POST …/consumer-groups/{gid}/sessions`, `GET …/sessions/{sid}`, `POST …:heartbeat`, `GET …/records`, `POST …/offsets:commit`, `POST …:leave` | Explicit managed group sessions (join can rebalance; requires confirmation) |

Security rules: bearer auth required even for CORS preflights; plaintext
administration only on loopback; every mutation is audited before dispatch
(fail-closed); responses never contain tokens, owner tokens, audit contents,
or filesystem paths; UIs must honor `truncated` and disable controls for
features reported unavailable (e.g. `sse.available: false`).

### Web console (optional)

```sh
pnpm install
ALLOW_LOOPBACK_HTTP=true pnpm ui:dev   # Node 24+, pnpm; open http://localhost:5173
```

Connect it to `http://127.0.0.1:7380` with the admin token; tokens stay in
the console gateway's memory for the browser session.

## 8. Docker

```sh
KUTTIDB_AUTH_TOKEN_FILE=./auth.token docker compose up --build
```

Compose runs a non-root container with durable WALs under
`/var/lib/kuttidb`, loopback-only ports (`7379` data, `9099` metrics),
`--durability always`, and an auth file fed from a Docker secret
(`KUTTIDB_AUTH_SOURCE`). Kubernetes manifests: `docs/operations/KUBERNETES.md`.

## 9. Testing and verification

```sh
make test          # core, platform, queues, exchanges, atomicity, streams, fuzz, embed
make sanitize      # ASan + UBSan on the concurrent core test
make bench-quick   # performance gates (p50/p95/p99 batch latency)
pnpm lint && pnpm test   # required when apps/ or packages/ change
```

Quick manual check:

```sh
./kuttidb 7390 /tmp/t.wal &          # start
./kuttidb-cli -p 7390 put greet hello
./kuttidb-cli -p 7390 get greet      # -> hello
./kuttidb-cli -p 7390 health && kill %1
```

Rules for changes: anything touching acknowledgement or recovery behavior
needs a matching crash-test; durable semantics are the contract; run
`make test` before submitting (see `AGENTS.md`).

## 10. Canonical documentation

- `docs/guides/GETTING_STARTED.md` — first run, values/queues/streams
- `docs/design/PROTOCOL.md` — wire protocol, CLI flags, limits
- `docs/messaging/QUEUES.md`, `EXCHANGES.md`, `STREAMS.md` — semantics
- `docs/design/DURABILITY.md` — acknowledgement points, atomic ops, limits
- `docs/SECURITY.md` — auth, TLS, threat model
- `docs/api/MANAGEMENT_API.md` + `openapi/management-v1.yaml` — admin API
- `docs/operations/DEPLOYMENT.md`, `DOCKER.md`, `KUBERNETES.md` — operations
- `docs/guides/MIGRATION.md` — when Redis/RabbitMQ/Kafka/SQLite fit better
