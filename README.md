<div align="center">

<img src="docs/logo.png" alt="KuttiDB logo" width="170" />

# KuttiDB

**A lightweight, crash-recoverable event cache — with queues, exchanges, streams, and cache state in one binary.**

Zero-copy between local processes · network-accessible across machines · no JVM, no Erlang, no external dependencies for a plaintext build.

![Language](https://img.shields.io/badge/language-C-38251D?logo=c&logoColor=white)
![Platforms](https://img.shields.io/badge/platform-macOS%20%7C%20Linux-D97A24)
![Version](https://img.shields.io/badge/version-0.0.1--beta-74645B)

</div>

---

KuttiDB is one executable, one configuration, one data directory. It started as a cache and grew the
messaging primitives people usually bolt on next to one: durable queues, routing, streams, and
atomic cache-plus-event commits — all recoverable after a crash, all observable over a single
authenticated Management API.

## Highlights

- **Cache** — in-memory key/value store with TTL, batching, pipelining, bounded memory with eviction,
  CRC-checked WAL and snapshots, and an optional shared-memory embedded mode with offset-based
  (ASLR-safe) regions.
- **Durable queues** — durable declarations, publish confirms, manual ACK/NACK, requeue, visibility
  timeouts, redelivery flags, prefetch, dead-letter queues, retry delays, message expiry, bounded
  depth, and crash recovery. At-least-once delivery.
- **Exchanges & routing** — direct, fanout, and topic routing in front of queues, an unnamed default
  exchange, alternate-exchange routing, explicit unroutable reporting, and durable bindings.
- **Atomic cache-plus-messaging** — `put_and_publish`, `put_and_enqueue`, `delete_and_publish`, and
  conditional `update_and_emit` commit a cache mutation and a delivery under one durable commit id:
  after recovery, both sides exist or neither does.
- **Anti-cache-stampede** — native single-flight with leased claims, bounded waits, and negative
  caching, plus stale-while-revalidate and refresh-ahead.
- **Streams** — durable, partitioned append logs with keyed partition choice, replay, retention
  controls, persisted consumer offsets, and leased consumer-group assignment.
- **Management API & Web console** — an authenticated HTTP/1.1 admin surface (audited, bounded,
  SSE-capable) and a self-hosted React console for dashboards and day-two operations.

## Quick start

For a small single-host application, Python can manage one local KuttiDB
instance directly. The opt-in factory derives an owner-only Unix socket and
data files from one directory, verifies the instance identity after connecting,
and requests graceful shutdown only after all native client connections are
closed for the idle grace period:

```python
from kuttidb_client import KuttiDBClient

with KuttiDBClient.managed(data_dir="./data/kuttidb", idle_timeout=60) as db:
    db.put("greeting", b"hello")
```

Ordinary `KuttiDBClient(...)` construction is unchanged: it only connects and
never starts or stops a server. Keep using standalone mode for remote clients,
containers, service managers, or any multi-machine deployment.

The default is Unix-only. Advanced local integrations may explicitly select a
literal loopback TCP endpoint with `transport="tcp"`, `host="127.0.0.1"`, and
`port=...`; DNS names and non-loopback addresses are rejected for managed mode.
`ServerParams` is the typed advanced surface for durability and fsync policy,
memory/value/batch/client/thread limits, an owner-only auth-token file, TCP TLS
certificate paths, explicit queue/stream WAL paths, and optional metrics or
admin listeners. These settings are allowlisted by `kuttidb ensure`; they are
not a shell-command escape hatch, and token values are never passed to the
launcher.

Build and run:

```sh
# option A: prebuilt binary (macOS arm64/x86_64, Linux x86_64/arm64)
curl -fsSL https://kuttidb.github.io/kuttidb/install.sh | bash
kuttidb 7379 kuttidb.wal

# option B: build from source
make            # builds kuttidb, kuttidb-bench, and the embedded library
./kuttidb 7379 kuttidb.wal
```

KuttiDB now listens on `127.0.0.1:7379`. The WAL file is its local recovery log — keep it if you
want durable queue and stream data to survive a restart. For a throwaway experiment, use
`./kuttidb 7379 -` instead.

Talk to it with the Python client (or Go, Java, Rust, Node.js — see [Clients](#clients)):

```python
from kuttidb_client import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    db.put("greeting", b"hello", ttl=60)
    print(db.get("greeting"))                     # b'hello'

    db.queue_declare("jobs", durable=True, max_depth=10_000,
                     dead_letter_queue="jobs.dead", max_deliveries=5)
    db.queue_publish("jobs", b"resize:123")
    delivery = db.queue_consume("jobs", visibility=30)
    if delivery:
        try:
            process(delivery["value"])
            db.queue_ack("jobs", delivery["id"])
        except Exception:
            db.queue_nack("jobs", delivery["id"], requeue=True)
```

The mental model fits in one table:

| Need | Use |
|---|---|
| A value you can read by key | `put` and `get` |
| Work that one worker should finish | Queue + ACK/NACK |
| An ordered history that readers can replay | Stream + offset |

## Management API & Web console

KuttiDB ships an optional, bearer-token-authenticated Management API (`/api/admin/v1`) for
operational dashboards and automation. It is disabled by default and refuses to start unsafely:
the token file must be `0600`, mutations are audited before dispatch, and plaintext
administration is loopback-only.

```sh
umask 077
printf '%s\n' 'replace-with-a-long-random-token' > admin.token
./kuttidb 7379 kuttidb.wal \
  --admin-bind 127.0.0.1:7380 \
  --admin-token-file admin.token \
  --admin-audit-log admin-audit.jsonl
```

The **KuttiDB Console** is a self-hosted web UI (React + Tailwind + shadcn/ui behind a
same-origin Fastify gateway) that covers the full API: overview dashboards, keyspace entries,
queues with browse/publish/deliveries, streams with a live tail, consumer groups, routing,
atomic operations, and maintenance jobs. The gateway deliberately retains administrator tokens
only in bounded process memory — browser storage keeps profile metadata only.

```sh
pnpm install
ALLOW_LOOPBACK_HTTP=true pnpm ui:dev     # http://localhost:5173
```

Connect the console to `http://127.0.0.1:7380` with the token from `admin.token`.
See [MANAGEMENT_API.md](docs/api/MANAGEMENT_API.md) for the security model and
[apps/management-ui](apps/management-ui) for the console.

## Clients

Native client libraries live in [`clients/`](clients) and are covered by the `make test` smoke run:

| Language | Path | Notes |
|---|---|---|
| Python | [`clients/`](clients) (`src/kuttidb_client.py`) | TCP, shared-memory embed, in-process, and managed local transports |
| Node.js | [`clients/nodejs`](clients/nodejs) | Zero dependencies; managed Unix and loopback TCP |
| Go | [`clients/go`](clients/go) | Full native v1.8 protocol, pooled TCP/TLS, managed Unix and loopback TCP, and cgo embed |
| Java | [`clients/java`](clients/java) | Full native v1.8 protocol, pooled TCP/TLS/Unix, managed Unix and loopback TCP |
| Rust | [`clients/rust`](clients/rust) | Full native v1.8 protocol, verified TLS, pooling, and managed Unix/loopback TCP |
| C/C++ | `libkuttidb_embed.dylib` / `.so` | Shared-memory embedded mode |

Python, Node.js, and Rust clients are published to PyPI (`kuttidb`), npm
(`@kuttidb/client`), and crates.io (`kuttidb`); the Go module is fetched
directly from this repository. Release procedure:
[CLIENT_PUBLISHING.md](docs/operations/CLIENT_PUBLISHING.md).

## Run it with Docker

```sh
KUTTIDB_AUTH_TOKEN_FILE=./auth.token docker compose up --build
```

The compose file starts a non-root container with durable WALs, loopback-only ports, and a
Prometheus metrics listener (`--metrics-bind 127.0.0.1:9099`). Multi-architecture
(linux/amd64 + linux/arm64) images are built in CI. See [DOCKER.md](docs/operations/DOCKER.md) and
[KUBERNETES.md](docs/operations/KUBERNETES.md) for manifests and probes.

## Documentation

| Document | Contents |
|---|---|
| [GETTING_STARTED.md](docs/guides/GETTING_STARTED.md) | Simple first run: values, Queues, and Streams |
| [ARCHITECTURE.md](docs/design/ARCHITECTURE.md) | Engines, storage separation, durability model |
| [PROTOCOL.md](docs/design/PROTOCOL.md) | Binary wire protocol, CLI flags, limits |
| [QUEUES.md](docs/messaging/QUEUES.md) | Queue semantics, delivery and dead-letter rules |
| [EXCHANGES.md](docs/messaging/EXCHANGES.md) | Exchange types, routing rules, binding limits |
| [STREAMS.md](docs/messaging/STREAMS.md) | Partition ordering, offsets, retention, consumer groups |
| [DURABILITY.md](docs/design/DURABILITY.md) | Acknowledgement points, atomic operations, single-node limits |
| [SECURITY.md](docs/SECURITY.md) | Auth, TLS, permissions, threat model |
| [MANAGEMENT_API.md](docs/api/MANAGEMENT_API.md) | Admin API startup, resources, and security guidance |
| [DEPLOYMENT.md](docs/operations/DEPLOYMENT.md) | Docker/Kubernetes, metrics, probes, backup/restore |
| [RELEASE.md](docs/operations/RELEASE.md) | Release cycle, official binaries, tagging process |
| [BENCHMARKS.md](docs/operations/BENCHMARKS.md) | Recorded benchmark methodology and results |
| [MIGRATION.md](docs/guides/MIGRATION.md) | When to use Redis/RabbitMQ/Kafka/SQLite instead |
| [CLIENT_PUBLISHING.md](docs/operations/CLIENT_PUBLISHING.md) | Publishing the client SDKs to PyPI, npm, crates.io, and Go |
| [SINGLE_NODE_SAAS_IMPROVEMENT_INSTRUCTION.md](docs/plans/SINGLE_NODE_SAAS_IMPROVEMENT_INSTRUCTION.md) | Agent assignment: bounded resources, mixed workloads, disk-backed messaging, and single-server SaaS performance |
| [openapi/management-v1.yaml](openapi/management-v1.yaml) | Versioned Management API contract |

## Testing and benchmarks

```sh
make test          # core, platform, queues, exchanges, atomicity, streams, fuzz, embed
make bench-quick   # cache performance gates
make bench-exchange
```

The suite includes crash-recovery tests (`queue_crash_test`, `queue_failure_test`), concurrency
and TSAN builds (`make sanitize-tsan-server`, `sanitize-tsan-queue`), and ASLR-safe embedded-region
tests. Recorded numbers live in [BENCHMARKS.md](docs/operations/BENCHMARKS.md) and [ROADMAP.md](docs/plans/ROADMAP.md).

## What KuttiDB is not

Honesty about limits is part of the design:

- It is **not** Redis, RabbitMQ, or Kafka, and speaks none of their wire protocols. The native
  protocol comes first; compatibility adapters are a later, separate milestone.
- It does **not** guarantee "no data loss". Evictable cache entries may be lost by policy.
  Acknowledged durable queue messages survive process crashes and clean restarts on a healthy
  single node — but a single node does not protect against disk or machine loss. Replication is
  not yet implemented.
- There is **no exactly-once delivery claim** anywhere.
- Windows requires remaining IOCP/named-pipe/mapping work (tracked in the roadmap).

## Contributing

Issues and pull requests are welcome. For non-trivial changes, please open an issue first so we
can agree on the approach. Run `make test` and `pnpm lint && pnpm test` (for the console) before
submitting. Durable semantics are the contract: if a PR changes acknowledgement or recovery
behavior, it should come with a matching crash-test.

## License

Licensed under the [Apache License, Version 2.0](LICENSE) (`Apache-2.0`).

<div align="center">
<sub>Built in C. One binary. Warm out of the oven. 🥖</sub>
</div>
