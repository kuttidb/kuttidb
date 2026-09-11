# KuttiDB Documentation

All project documentation lives in this folder. The repository root keeps only
`README.md` (entry point), `LICENSE`, and `AGENTS.md` (agent/contributor rules).
See [../AGENTS.md](../AGENTS.md) for the placement rules.

## Layout

| Folder | Contents |
|---|---|
| [`guides/`](guides) | First-run guides and migration comparisons |
| [`design/`](design) | Architecture, wire protocol, durability model |
| [`messaging/`](messaging) | Queue, exchange, and stream semantics |
| [`operations/`](operations) | Deployment, Docker, Kubernetes, benchmarks |
| [`api/`](api) | Management API reference |
| [`adr/`](adr) | Architecture decision records |
| [`SECURITY.md`](SECURITY.md) | Security policy (kept directly under `docs/` so GitHub still recognizes it) |

## Index

| Document | Contents |
|---|---|
| [guides/GETTING_STARTED.md](guides/GETTING_STARTED.md) | Simple first run: values, Queues, and Streams |
| [guides/TELEMETRY.md](guides/TELEMETRY.md) | Optional privacy-preserving telemetry and public aggregate statistics |
| [guides/SAAS_DEMO.md](guides/SAAS_DEMO.md) | One-command report demo: cache, background jobs, event replay, and crash recovery |
| [guides/ATOMIC_JOB_COMPLETION.md](guides/ATOMIC_JOB_COMPLETION.md) | Atomic job completion: durable state, ACK, and the next message in one commit |
| [guides/CLIENT_FEATURE_MATRIX.md](guides/CLIENT_FEATURE_MATRIX.md) | Per-client feature coverage, method mapping, and transports |
| [guides/GO_CLIENT.md](guides/GO_CLIENT.md) | Go client: context APIs, affinity, embedding opt-in, and replay cursors |
| [design/ARCHITECTURE.md](design/ARCHITECTURE.md) | Engines, storage separation, durability model |
| [design/PROTOCOL.md](design/PROTOCOL.md) | Binary wire protocol, CLI flags, limits |
| [messaging/QUEUES.md](messaging/QUEUES.md) | Queue semantics, delivery and dead-letter rules |
| [messaging/EXCHANGES.md](messaging/EXCHANGES.md) | Exchange types, routing rules, binding limits |
| [messaging/STREAMS.md](messaging/STREAMS.md) | Partition ordering, offsets, retention, consumer groups |
| [design/DURABILITY.md](design/DURABILITY.md) | Acknowledgement points, atomic operations, single-node limits |
| [design/ATOMIC_JOB_COMPLETION.md](design/ATOMIC_JOB_COMPLETION.md) | Completion commit authority, receipts, recovery, and checkpoints |
| [SECURITY.md](SECURITY.md) | Auth, TLS, permissions, threat model |
| [api/MANAGEMENT_API.md](api/MANAGEMENT_API.md) | Admin API startup, resources, and security guidance |
| [design/MANAGEMENT_UI_DESIGN_SYSTEM.md](design/MANAGEMENT_UI_DESIGN_SYSTEM.md) | Brand-based console design: tokens, components, layouts, and interaction states |
| [operations/DEPLOYMENT.md](operations/DEPLOYMENT.md) | Docker/Kubernetes, metrics, probes, backup/restore |
| [operations/RELEASE.md](operations/RELEASE.md) | Release cycle, official binaries, tagging process |
| [operations/CLIENT_PUBLISHING.md](operations/CLIENT_PUBLISHING.md) | Client SDK releases: PyPI, npm, crates.io, Go module |
| [operations/DOCKER.md](operations/DOCKER.md) | Container image, compose setup, runtime flags |
| [operations/KUBERNETES.md](operations/KUBERNETES.md) | Manifests, probes, and production notes |
| [operations/BENCHMARKS.md](operations/BENCHMARKS.md) | Recorded benchmark methodology and results |
| [guides/MIGRATION.md](guides/MIGRATION.md) | When to use Redis/RabbitMQ/Kafka/SQLite instead |
| [../openapi/management-v1.yaml](../openapi/management-v1.yaml) | Versioned Management API contract |
