# AGENTS.md — Rules for AI Agents and Contributors

KuttiDB is a C database/server with a TypeScript management console. This file
defines the conventions every agent and human contributor must follow. It is
the single source of truth for where documentation lives.

## Documentation policy (mandatory)

**All project documentation lives in `docs/`. Never create `.md` files at the
repository root.**

The root keeps exactly three markdown-visible files:

- `README.md` — project entry point (the only documentation file allowed at root)
- `AGENTS.md` — this file
- `LICENSE` — not markdown, listed for completeness

Anything else — guides, references, specs, implementation plans, working
instructions, notes — goes under `docs/` in the matching subfolder.

### Where new documents go

| Path | Put here | Naming |
|---|---|---|
| `docs/guides/` | First-run guides, how-tos, migration comparisons | `UPPER_SNAKE.md` |
| `docs/design/` | Architecture, wire protocol, durability model | `UPPER_SNAKE.md` |
| `docs/messaging/` | Queue, exchange, and stream semantics | `UPPER_SNAKE.md` |
| `docs/operations/` | Deployment, Docker, Kubernetes, benchmarks, releases | `UPPER_SNAKE.md` |
| `docs/api/` | Management API reference material | `UPPER_SNAKE.md` |
| `docs/plans/` | Roadmap plus implementation plans and instruction documents used for development (by humans or agents) | `UPPER_SNAKE.md`, suffix `_PLAN` or `_INSTRUCTION` |
| `docs/adr/` | Architecture decision records | `NNNN-short-title.md`, sequential number |
| `docs/SECURITY.md` | Security policy (must stay directly under `docs/` so GitHub recognizes it) | fixed |

If a document spans categories, pick the primary audience: operators →
`operations/`, application developers → `guides/`, protocol/storage internals →
`design/`.

### Rules when writing or moving documentation

1. Never add, leave, or re-create `.md` files at the repository root except
   `README.md` and `AGENTS.md`.
2. When you add a user-facing document, add a row to the index tables in
   `README.md` (Documentation section) and `docs/README.md`.
3. Use relative links between documents. After moving a document, fix:
   its outbound links, links pointing at it from other docs, the
   `README.md` tables, and the GitHub `blob/main/...` links in
   `landing/index.html`.
4. Source comments may reference documents — use the repo-root path
   (e.g. `docs/design/ARCHITECTURE.md`), never a bare filename.
5. Do not rename documents casually; agents and CI reference them by path.
   If a rename is required, update every reference and keep the move in its
   own commit.
6. Transient working notes for agent-driven tasks belong in `docs/plans/`,
   not the root, not `docs/` directly, and never in commit messages alone.

## Build and test

```sh
make              # builds kuttidb, kuttidb-bench, and the embedded library
make test         # core, platform, queues, exchanges, atomicity, streams, fuzz, embed
make bench-quick  # cache performance gates
pnpm lint && pnpm test   # management console (apps/management-ui)
```

- Durable semantics are the contract: a PR that changes acknowledgement or
  recovery behavior must come with a matching crash-test.
- Run `make test` before submitting; run the console checks when `apps/`
  or `packages/` change.
