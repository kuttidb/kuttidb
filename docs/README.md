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
| [guides/SAAS_DEMO.md](guides/SAAS_DEMO.md) | One-command report demo: cache, background jobs, event replay, and crash recovery |
| [design/ARCHITECTURE.md](design/ARCHITECTURE.md) | Engines, storage separation, durability model |
| [design/PROTOCOL.md](design/PROTOCOL.md) | Binary wire protocol, CLI flags, limits |
| [messaging/QUEUES.md](messaging/QUEUES.md) | Queue semantics, delivery and dead-letter rules |
| [messaging/EXCHANGES.md](messaging/EXCHANGES.md) | Exchange types, routing rules, binding limits |
| [messaging/STREAMS.md](messaging/STREAMS.md) | Partition ordering, offsets, retention, consumer groups |
| [design/DURABILITY.md](design/DURABILITY.md) | Acknowledgement points, atomic operations, single-node limits |
| [SECURITY.md](SECURITY.md) | Auth, TLS, permissions, threat model |
| [api/MANAGEMENT_API.md](api/MANAGEMENT_API.md) | Admin API startup, resources, and security guidance |
| [design/MANAGEMENT_UI_DESIGN_SYSTEM.md](design/MANAGEMENT_UI_DESIGN_SYSTEM.md) | Brand-based console design: tokens, components, layouts, and interaction states |
| [operations/DEPLOYMENT.md](operations/DEPLOYMENT.md) | Docker/Kubernetes, metrics, probes, backup/restore |
| [operations/LANDING_PAGE.md](operations/LANDING_PAGE.md) | Preview the landing page, maintain client examples, and serve it with GitHub Pages |
| [operations/RELEASE.md](operations/RELEASE.md) | Release cycle, official binaries, tagging process |
| [operations/CLIENT_PUBLISHING.md](operations/CLIENT_PUBLISHING.md) | Client SDK releases: PyPI, npm, crates.io, Go module |
| [operations/DOCKER.md](operations/DOCKER.md) | Container image, compose setup, runtime flags |
| [operations/KUBERNETES.md](operations/KUBERNETES.md) | Manifests, probes, and production notes |
| [operations/BENCHMARKS.md](operations/BENCHMARKS.md) | Recorded benchmark methodology and results |
| [guides/MIGRATION.md](guides/MIGRATION.md) | When to use Redis/RabbitMQ/Kafka/SQLite instead |
| [../openapi/management-v1.yaml](../openapi/management-v1.yaml) | Versioned Management API contract |
