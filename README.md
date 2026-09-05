<div align="center">

<img src="docs/logo.png" alt="KuttiDB’s smiling toast mascot" width="96" />

<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/wordmark-dark.svg" />
    <img src="docs/assets/wordmark-light.svg" alt="KuttiDB." width="350" />
  </picture>
</h1>

### One binary. A lot off your plate.

Cache, background jobs, and replayable events.<br />
One small C server for your SaaS.

<p>
  <a href="https://github.com/kuttidb/kuttidb/releases"><img src="https://img.shields.io/github/v/release/kuttidb/kuttidb?include_prereleases&amp;filter=v%2A&amp;label=release&amp;color=ed742f&amp;labelColor=27271f&amp;style=flat-square" alt="Latest server release, including prereleases" /></a>
  <a href="https://github.com/kuttidb/kuttidb/actions/workflows/release.yml"><img src="https://img.shields.io/github/actions/workflow/status/kuttidb/kuttidb/release.yml?label=release%20build&amp;labelColor=27271f&amp;style=flat-square" alt="Official binary release build status" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/kuttidb/kuttidb?color=c8daac&amp;labelColor=27271f&amp;style=flat-square" alt="Apache-2.0 license" /></a>
  <a href="docs/operations/RELEASE.md"><img src="https://img.shields.io/badge/platform-macOS%20%7C%20Linux-e6e8df?labelColor=27271f&amp;style=flat-square" alt="Platforms: macOS and Linux" /></a>
</p>

**[Website](https://kuttidb.com)** ·
**[Getting started](docs/guides/GETTING_STARTED.md)** ·
**[Releases](https://github.com/kuttidb/kuttidb/releases)** ·
**[Documentation](docs/README.md)**

<sub>Open source · Self-hosted · Currently in beta</sub>

</div>

---

KuttiDB brings a cache, durable work queues, and replayable event streams into
one executable, with one configuration and one data directory. It is built for
small and midsize SaaS teams running on a single server.

Local processes can share memory; clients on other machines use the native
network protocol. The plaintext build has no external dependencies. An optional
Management API and self-hosted console handle day-to-day operations.

[Try the demo](#try-all-three-in-60-seconds) · [Install](#install-and-run) ·
[Clients](#clients) · [What survives a restart](#what-kuttidb-is-not)

## Highlights

### Three everyday needs. One place to put them.

| Cache · keep it handy | Queues · get it done | Streams · play it back |
|---|---|---|
| Read a value by key. Give it a TTL. | Hand work to a worker. ACK when finished. | Append an event. Read it again by offset. |
| Batching, pipelining, bounded memory, and eviction. | Durable publish confirms, retry delays, visibility timeouts, and dead-letter queues. | Partitioned logs, retention controls, saved offsets, and leased consumer groups. |
| [Values & first steps →](docs/guides/GETTING_STARTED.md) | [Delivery & recovery →](docs/messaging/QUEUES.md) | [Partitions & replay →](docs/messaging/STREAMS.md) |

**The value and the job. One durable commit.** Atomic operations such as
`put_and_enqueue` and `put_and_publish` commit a cache mutation and message
delivery together: after recovery, both sides exist or neither does. They require
cache persistence and durable target queues.
[Read the commit contract →](docs/design/DURABILITY.md#atomic-cache-plus-message-operations)

Also included:

- **Exchanges and routing:** direct, fanout, and topic exchanges, durable bindings,
  alternate routing, and explicit unroutable results.
- **Cache-stampede protection:** leased single-flight claims, bounded waits,
  negative caching, stale-while-revalidate, and refresh-ahead.
- **Local shared memory:** optional offset-based, ASLR-safe embedded regions.
- **Operations:** an authenticated, audited Management API and web console.

## Try all three in 60 seconds

Generate a report, cache its status, finish a background job, and append its
completion event. Then watch the demo **kill and restart its own server** and
verify recovery.

```sh
curl -fsSL https://kuttidb.com/demo.sh | bash
```

**You need:** macOS or Linux, Python 3.10+, and curl. No account or pip install.
The script uses an installed binary or downloads a checksum-verified release,
runs on a private local socket, and removes its own server and temporary data
when finished. Download time varies; the demo itself takes seconds.

[![Recorded demo: cache, report worker, durable queue, event replay, and crash recovery](landing/demo.gif)](https://kuttidb.com/#demo)

<details>
<summary><strong>What this recording proves</strong></summary>

This is a real recorded run. Cache writes use `--durability always`. Restarting
checks process-crash recovery on the same healthy disk, not protection from disk
or machine loss. The memory value is sampled server RSS, not a benchmark or
peak-memory guarantee.

Already cloned the repository? Run `make && python3 examples/saas_demo.py`.

</details>

[Read the demo](examples/saas_demo.py) ·
[Follow the walkthrough](docs/guides/SAAS_DEMO.md) ·
[Replay the recording](https://kuttidb.com/#demo)

## Install and run

Prebuilt binaries are available for **macOS and Linux**, on **arm64 and x86_64**:

```sh
curl -fsSL https://kuttidb.github.io/kuttidb/install.sh | bash
kuttidb 7379 kuttidb.wal
```

Or build from a cloned repository:

```sh
make
./kuttidb 7379 kuttidb.wal
```

The server listens on `127.0.0.1:7379`. Keep its WAL files for recovery: the cache,
queues, and streams use their own logs. `make` also builds the benchmark tool and
embedded library.

[Release downloads](https://github.com/kuttidb/kuttidb/releases) ·
[Platform requirements](docs/operations/RELEASE.md) ·
[Full setup guide](docs/guides/GETTING_STARTED.md)

## Clients

Install the client for your language, then connect to your server:

| Language | Install | Source |
|---|---|---|
| **Python** | `python3 -m pip install --pre kuttidb` | [Python client](clients/python) |
| **Node.js** | `npm install @kuttidb/client@beta` | [Node.js client](clients/nodejs) |
| **Go** | `go get github.com/kuttidb/kuttidb/clients/go` | [Go module](clients/go) |
| **Rust** | `cargo add kuttidb@0.0.6-beta` | [Rust crate](clients/rust) |
| **Java** | Maven: `io.github.kuttidb:kuttidb-client:0.0.8-beta` | [Java client](clients/java) |
| **C / C++** | `make` builds `libkuttidb_embed.dylib` / `.so` | [Embedded API](src/embed.h) |

<details>
<summary><strong>Java: Maven and Gradle configuration</strong></summary>

Maven (`pom.xml`, Java 17+):

```xml
<dependency>
  <groupId>io.github.kuttidb</groupId>
  <artifactId>kuttidb-client</artifactId>
  <version>0.0.8-beta</version>
</dependency>
```

Gradle (Kotlin DSL):

```kotlin
implementation("io.github.kuttidb:kuttidb-client:0.0.8-beta")
```

</details>

Python, Node.js, Go, Java, and Rust cover the native cache, queue, exchange,
atomic-operation, and stream APIs, including managed local servers. The C/C++
embedded API provides shared-memory cache access; use the socket clients for
messaging. Each SDK versions independently.

[Browse usage examples](https://kuttidb.com/#workbench-title) ·
[Package publishing and releases](docs/operations/CLIENT_PUBLISHING.md)

## Connect from your application

With the Python package installed and the server running:

```python
from kuttidb import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    # Keep a value close.
    db.put("report:42", b"ready", ttl=60)
    print(db.get("report:42"))

    # Hand off a job; acknowledge after processing.
    db.queue_declare("reports", durable=True)
    db.queue_publish("reports", b"report:42")
    job = db.queue_consume("reports", visibility=30)
    if job:
        print(job["value"])
        db.queue_ack("reports", job["id"])

    # Keep an event for replay.
    db.stream_declare("events", partitions=1)
    db.stream_append("events", b"report.ready")
    for event in db.stream_fetch("events", partition=0, offset=0):
        print(event["value"])
```

For application work, acknowledge only after the work succeeds. Use NACK and
requeue for retryable failures; delivery is at-least-once.
[Queue patterns and dead-letter handling →](docs/messaging/QUEUES.md)

<details>
<summary><strong>Let Python manage a local server</strong></summary>

For a small single-host application, the opt-in factory manages one local
KuttiDB instance from a data directory:

```python
from kuttidb import KuttiDBClient

with KuttiDBClient.managed(data_dir="./data/kuttidb", idle_timeout=60) as db:
    db.put("greeting", b"hello")
```

It derives an owner-only Unix socket and data files, verifies the instance
identity after connecting, and requests graceful shutdown only after all native
client connections have closed for the idle grace period. The KuttiDB binary
must be installed locally.

Ordinary `KuttiDBClient(...)` construction only connects; it never starts or stops
a server. Use standalone mode for remote clients, containers, service managers,
and multi-machine deployments.

The default transport is Unix-only. Advanced integrations can explicitly choose
literal loopback TCP with `transport="tcp"`, `host="127.0.0.1"`, and `port=...`.
DNS names and non-loopback addresses are rejected for managed mode.

`ServerParams` provides typed settings for durability, fsync, memory/value/batch/
client/thread limits, an owner-only auth-token file, TCP TLS certificates,
queue/stream WAL paths, and optional metrics or admin listeners. Settings are
allowlisted by `kuttidb ensure`; token values are never passed to the launcher.

</details>

## Management API & Web console

The optional Management API (`/api/admin/v1`) and self-hosted console cover
keyspace entries, queues, routing, stream tails, consumer groups, atomic
operations, and maintenance jobs.

[![Recorded console tour: overview, queues, retained messages](landing/media/console-tour.gif)](docs/api/MANAGEMENT_API.md)

| Overview — real facts, no invented charts | Queue messages — browsing never consumes |
|---|---|
| ![Console overview with engine summaries, recent jobs, and persistence facts](landing/media/console-overview.png) | ![Queue detail listing retained messages with state, size, and delivery counts](landing/media/console-queue-messages.png) |
| ![Console overview in dark mode with olive surfaces](landing/media/console-overview-dark.png) | ![Keyspace entry inspector with encoding tabs](landing/media/console-keyspace-inspector.png) |

### Run the console

**1. Start KuttiDB with the Management API enabled:**

```sh
umask 077
printf '%s\n' 'replace-with-a-long-random-token' > admin.token
./kuttidb 7379 kuttidb.wal \
  --admin-bind 127.0.0.1:7380 \
  --admin-token-file admin.token \
  --admin-audit-log admin-audit.jsonl
```

**2. In a second terminal, run the console from the repository root
(Node 24+ and pnpm):**

```sh
pnpm install
ALLOW_LOOPBACK_HTTP=true pnpm ui:dev
```

**3. Open `http://localhost:5173`**, connect to `http://127.0.0.1:7380`, and use
the token from `admin.token`. The token is held in the console gateway's memory
for this browser session and is never saved with a profile.

The API is disabled by default. It requires a bearer-token file with `0600`
permissions, audits mutations before dispatch, and permits plaintext
administration only on loopback. The console gateway keeps administrator tokens
in bounded process memory; browser storage contains profile metadata only.

[Management API and security model](docs/api/MANAGEMENT_API.md) ·
[Console source](apps/management-ui)

## Run it with Docker

```sh
KUTTIDB_AUTH_TOKEN_FILE=./auth.token docker compose up --build
```

The compose file starts a non-root container with durable WALs, loopback-only
ports, and a Prometheus metrics listener. Multi-architecture images cover
`linux/amd64` and `linux/arm64`.

[Docker setup](docs/operations/DOCKER.md) ·
[Kubernetes manifests](docs/operations/KUBERNETES.md) ·
[Deployment and backups](docs/operations/DEPLOYMENT.md)

## Documentation

| Document | Contents |
|---|---|
| [GETTING_STARTED.md](docs/guides/GETTING_STARTED.md) | Simple first run: values, Queues, and Streams |
| [SAAS_DEMO.md](docs/guides/SAAS_DEMO.md) | One-command report demo: cache, background jobs, event replay, and crash recovery |
| [ARCHITECTURE.md](docs/design/ARCHITECTURE.md) | Engines, storage separation, durability model |
| [PROTOCOL.md](docs/design/PROTOCOL.md) | Binary wire protocol, CLI flags, limits |
| [QUEUES.md](docs/messaging/QUEUES.md) | Queue semantics, delivery and dead-letter rules |
| [EXCHANGES.md](docs/messaging/EXCHANGES.md) | Exchange types, routing rules, binding limits |
| [STREAMS.md](docs/messaging/STREAMS.md) | Partition ordering, offsets, retention, consumer groups |
| [DURABILITY.md](docs/design/DURABILITY.md) | Acknowledgement points, atomic operations, single-node limits |
| [SECURITY.md](docs/SECURITY.md) | Auth, TLS, permissions, threat model |
| [MANAGEMENT_API.md](docs/api/MANAGEMENT_API.md) | Admin API startup, resources, and security guidance |
| [MANAGEMENT_UI_DESIGN_SYSTEM.md](docs/design/MANAGEMENT_UI_DESIGN_SYSTEM.md) | Brand-based console design: tokens, components, layouts, and interaction states |
| [DEPLOYMENT.md](docs/operations/DEPLOYMENT.md) | Docker/Kubernetes, metrics, probes, backup/restore |
| [LANDING_PAGE.md](docs/operations/LANDING_PAGE.md) | Preview the landing page, maintain client examples, and serve it with GitHub Pages |
| [RELEASE.md](docs/operations/RELEASE.md) | Release cycle, official binaries, tagging process |
| [BENCHMARKS.md](docs/operations/BENCHMARKS.md) | Recorded benchmark methodology and results |
| [MIGRATION.md](docs/guides/MIGRATION.md) | When to use Redis/RabbitMQ/Kafka/SQLite instead |
| [CLIENT_PUBLISHING.md](docs/operations/CLIENT_PUBLISHING.md) | Publishing the client SDKs to PyPI, npm, crates.io, and Go |
| [openapi/management-v1.yaml](openapi/management-v1.yaml) | Versioned Management API contract |

## Testing and benchmarks

```sh
make test          # core, platform, messaging, recovery, clients, and embed
make bench-quick   # cache performance gates
make bench-exchange
```

The suite covers crash recovery, concurrency, protocol fuzzing, client smoke
runs, and ASLR-safe embedded regions. Changes to acknowledgement or recovery
behavior require a matching crash-test. Console changes also require
`pnpm lint && pnpm test` from `apps/management-ui`.

[Recorded benchmarks and methodology](docs/operations/BENCHMARKS.md)

## What KuttiDB is not

A single server, on purpose. The limits are part of the contract:

| Boundary | What to expect |
|---|---|
| **Single-node durability** | Durable modes protect process crashes and clean restarts on a healthy node. They do not protect disk or machine loss. Replication is not implemented. |
| **At-least-once delivery** | Unfinished work can be redelivered. There is no exactly-once processing guarantee. |
| **Evictable cache** | Entries may be lost by policy. Cache durability depends on the selected mode. |
| **Native protocol** | KuttiDB is not a drop-in Redis, RabbitMQ, or Kafka replacement and does not speak their wire protocols. |
| **Platform support** | macOS and Linux. Windows still requires IOCP, named-pipe, and mapping work. |

[Durability guarantees](docs/design/DURABILITY.md) ·
[When another tool is a better fit](docs/guides/MIGRATION.md)

## Contributing

Issues and pull requests are welcome. For non-trivial changes, open an issue
first to agree on the approach. Follow [AGENTS.md](AGENTS.md) for contribution
rules and documentation placement, and run the relevant checks before submitting.

[Report an issue](https://github.com/kuttidb/kuttidb/issues) ·
[Security policy](docs/SECURITY.md)

## License

[Apache License, Version 2.0](LICENSE). Self-host it, use it, and build on it.

---

<div align="center">

<img src="docs/logo.png" alt="" width="40" />

**Less to run. More to build.**<br />
<sub>Built in C. Served warm.</sub>

</div>
