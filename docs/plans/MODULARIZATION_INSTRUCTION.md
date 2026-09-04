# Implementation instruction: performance-safe modularization of KuttiDB

## Objective

Refactor KuttiDB into small, coherent modules so a maintainer or coding agent
can change one cache, queue/exchange, stream, protocol, networking, persistence,
or operational feature without loading unrelated implementation files.

Preserve the completed performance work. This is a structural refactor, not a
new optimization program and not a protocol or storage-format rewrite.

The finished result must retain:

- native protocol 1.6, including queue batches `0x2D`-`0x2F`, stream commit
  batch `0x6B`, and capability bits 12 and 13;
- the current cache hot path and direct response-vector GET behavior;
- queue message indexes, ready hints, depth-independent common operations,
  grouped fsync, batches, per-queue concurrency, and WAL checkpoints;
- stream incremental accounting, coalesced retention, grouped fsync, batches,
  and WAL checkpoints;
- existing durability acknowledgement points, crash recovery, resource bounds,
  and public APIs;
- the corrected multiprocess benchmark methodology.

The working tree may contain unrelated user changes. Never reset, overwrite,
or broadly reformat changes outside the active module.

## Re-evaluation baseline (2026-08-30, commit `5f36dd3`)

The performance program materially changed the source since the first review.
The context hotspots are now:

| File | Current size | Main responsibilities mixed together |
|---|---:|---|
| `src/server.c` | 4,489 lines / 188,949 bytes | configuration, auth/TLS, byte codecs, cache WAL/snapshots, atomic coordination, maintenance, connection state, singleflight/SWR, all protocol families including new batches, event loops, listeners, metrics HTTP, and `main` |
| `src/queue.c` | 2,830 lines / 118,137 bytes | queue state, intrusive indexes, delivery/retry/DLQ, three lock domains, grouped fsync, WAL/replay/checkpoint, exchanges, transactions, batches, durable consumers, reaping, inventory, and metrics |
| `src/stream.c` | 140 physical lines / 31,494 bytes | topics, partitions, retention, incremental accounting, grouped fsync, WAL/replay/compaction, batches, consumer groups, and metrics, with lines up to 1,969 characters |
| `clients/nodejs/kuttidb_client.js` | 770 lines / 28,850 bytes | transport/pooling, codecs, and every public feature API |

The largest single context hotspot remains `conn_process` in `src/server.c`,
now approximately 1,165 lines (`server.c:2160` through `server.c:3324`). It
parses, validates, executes, and responds to every native protocol family.

`src/kuttidb.c`, `src/embed.c`, `src/embed_kuttidb.c`, and `src/platform.c`
remain comparatively bounded. Do not split them for symmetry. The cache engine
was not a measured server bottleneck, so cache micro-optimization was correctly
skipped in the performance program.

## Mandatory pre-refactor synchronization audit

Do not begin mechanical queue extraction from the current commit until the
queue synchronization implementation, tests, and documentation agree.

The declared design in `src/queue.c` is:

```text
metadata store->lock -> queue->lock -> store->wal_lock
```

Queue objects are never freed while the store is live, fanout locks target
queues in creation order, DLQ routing uses trylock to avoid queue-lock cycles,
and atomic transactions deliberately keep the metadata lock across the
prepare/cache-marker/commit window.

The current committed implementation does not consistently follow that design:

- ordinary queue publish/consume/ACK/NACK and batch paths resolve a queue under
  `store->lock`, then acquire `store->lock` again for message state instead of
  acquiring `queue->lock`;
- checkpoint, maintenance, owner cleanup, and aggregate stats contain nested
  `store->lock` calls where their comments require queue locks;
- `queue_consume_for_owner` calls `q_wait_durable_locked` without first holding
  `wal_lock`, although that helper performs condition waits on `wal_lock` and
  documents it as a caller requirement;
- `QUEUES.md` still says per-queue lock sharding is future work while
  `ARCHITECTURE.md`, `ROADMAP.md`, the `Queue` structure, and benchmark claims
  say it is implemented.

The metadata and WAL mutexes use `PTHREAD_MUTEX_ERRORCHECK`. A recursive
metadata-lock attempt therefore returns an error instead of necessarily
blocking, but the return values are ignored. Passing the ordinary queue test is
not proof that lock ownership is correct.

Repair this in a separate correctness change before modularization:

1. Declare lock ownership on every internal helper (`metadata held`, `queue
   held`, `WAL held`, or no lock) and enforce it consistently.
2. Replace mistaken message-state metadata locks with the owning queue lock.
3. Ensure every `q_wait_durable_locked` call enters with `wal_lock` held and no
   metadata or queue lock held, as its contract requires.
4. Preserve the atomic transaction exception explicitly rather than weakening
   the general order.
5. Check and handle mutex-operation errors in debug/test builds.
6. Add deterministic concurrency tests proving that independent queues can
   mutate in parallel and grouped-fsync waiters cannot be lost.
7. Run queue failure, crash, exchange, atomic, checkpoint, batch, ASan/UBSan,
   and TSan gates.
8. Rerun `make bench-queue`; only results from the repaired committed source
   become the modularization performance baseline.
9. Reconcile `QUEUES.md`, `ARCHITECTURE.md`, `ROADMAP.md`, and code comments.

Do not hide this audit inside file moves. Review the synchronization correction
as logic, then perform module extraction from a known-correct baseline.

## Performance invariants to protect

### Queue invariants

- Message removal is O(1) through the doubly linked list.
- Delivery-tag lookup uses the per-queue intrusive hash index and remains
  proportional to live in-flight state, not retained depth.
- `ready_hint` prevents repeated scans through the in-flight prefix. Any
  requeue that may precede the hint resets it safely; a delayed ready message
  is never skipped permanently.
- TTL/visibility scans run only when `ttl_count` or the earliest visibility
  deadline says work may be due.
- `live_bytes` and each message's `wal_footprint` are updated at every shared
  mutation point and remain equal to a full checkpoint recount.
- Queue, metadata, and WAL lock domains remain distinct after the audit. Queue
  pointers remain stable after metadata lookup because queues are not freed
  before store shutdown.
- WAL byte order equals WAL sequence order. `synced_seq` advances only for the
  high-water mark actually covered by a completed fsync.
- Additive operations may apply before waiting for grouped durability;
  destructive transforms such as ACK, NACK/requeue, and dead-letter routing
  preserve fsync-before-mutate ordering.
- A sole durable writer still fsyncs immediately; low-load latency must not be
  traded away accidentally.
- Batch operations retain their all-input validation/capacity checks and one
  durability wait per batch. ACK/NACK durability remains per processed record.
- Fanout locks queues in stable creation order, performs the all-target capacity
  check before mutation, and deduplicates targets.
- DLQ routing stays durable-before-source-removal and fail closed. A busy DLQ
  may defer routing but must never cause loss or a lock cycle.
- Queue checkpointing holds a consistent metadata and all-queue snapshot,
  emits existing record types, fsyncs the temporary file, atomically renames,
  fsyncs the parent directory, and safely swaps the append file.
- The atomic cache-plus-message path preserves metadata -> queue -> WAL/cache
  ordering and its prepare/marker/commit recovery rules.

### Stream invariants

- `live_bytes` remains an incrementally maintained O(1) compaction estimate;
  do not reintroduce a retained-record traversal on append or commit.
- Per-topic record counts remain incremental so metrics do not scan retained
  partitions while holding the store mutex.
- Retention writes one coalesced trim boundary per affected partition per pass,
  not one fsync per removed record.
- Appends, batch appends, trims, declarations, and group commits update
  accounting at their shared mutation points.
- `wal_seq`, `synced_seq`, and `syncer` keep grouped-fsync acknowledgement
  semantics. Failure wakes all waiters and makes durable operations fail closed.
- Compaction waits for an in-flight syncer, writes and syncs a complete retained
  checkpoint, atomically publishes it, and then marks checkpointed sequences
  durable.
- Batch append stays one atomic WAL record. Batch group commit applies entries
  in order and shares one durability wait.
- Fetch continues to enforce both record-count and response-byte limits.

### Server and cache invariants

- Cache GET appends directly to the connection response vector while holding
  only the relevant shard lock; do not add an intermediate value allocation.
- Batched cache paths keep streaming/incremental input parsing and bounded
  opportunistic output draining.
- Moving protocol code must not add a second parse, full-frame copy, heap
  allocation per request, virtual dispatch, or a global dispatcher mutex.
- Event-loop and kqueue/epoll behavior is not an optimization target for this
  refactor. The multiprocess matrix showed the server scaling to eight clients;
  the old eight-thread cliff was in the benchmark client.
- Snapshot staging, WAL folding, and the atomic commit marker retain their
  existing critical sections and acknowledgement points.

## Target module layout

Use this as the intended ownership map. Do not create empty or one-function
modules solely to match the tree.

```text
src/
  app/
    config.c/.h                 parse and validate configuration
    runtime.c/.h                ServerContext lifecycle and maintenance
    main.c                      startup, signals, thread ownership, shutdown

  common/
    bytes.c/.h                  LE codecs and bounded read cursors
    crc32.c/.h                  CRC primitives with format-specific wrappers
    file_io.c/.h                exact I/O, private open, parent-directory fsync
    time_util.c/.h              wall and monotonic clocks

  cache/
    cache.c                     existing in-memory cache core
    cache_wal.c/.h              append, flush, and valid-prefix replay
    cache_snapshot.c/.h         load, stage, publish, and WAL fold
    cache_persist.c/.h          mutation acknowledgement policy

  queue/
    queue.c                     public facade and lifecycle
    queue_internal.h            private structs and lock ownership contracts
    queue_message.c             list/index/hint/accounting primitives
    queue_delivery.c            publish/consume/ACK/NACK/retry/DLQ/reaping
    queue_batch.c               publish/consume/ACK/NACK batch orchestration
    queue_consumer.c            durable named consumers and owner cleanup
    exchange.c                  declare/bind/route/topic matching/fanout locks
    queue_tx.c                  atomic prepare/commit/abort/reconciliation
    queue_wal.c                 records, replay, sequence, and grouped fsync
    queue_checkpoint.c          live recount and crash-safe checkpoint publish
    queue_stats.c               bounded snapshots and aggregate counters

  stream/
    stream.c                    public facade and lifecycle
    stream_internal.h           private structs and mutex/WAL contracts
    stream_state.c              shared mutation and incremental accounting
    stream_topic.c              declare, append, batch append, fetch
    stream_retention.c          trim selection and coalesced persistence
    stream_group.c              membership, assignment, commits, lag, reaping
    stream_wal.c                records, replay, grouped fsync, compaction
    stream_stats.c              bounded snapshots and diagnostic recount

  protocol/
    opcodes.h                   C authority for protocol 1.6 constants
    protocol.c/.h               incremental dispatcher and result contract
    response.c/.h               bounded response encoders
    cache_handler.c
    queue_handler.c             includes queue batches
    exchange_handler.c
    stream_handler.c            includes append/commit batches
    atomic_handler.c
    singleflight_handler.c
    admin_handler.c             AUTH/HEALTH/CAPABILITIES/STATS/inventory

  server/
    server_internal.h           private Conn/Loop/ServerContext types
    connection.c                I/O, buffering, backpressure, guard timer
    event_loop.c                accept/read/write/timer callbacks and workers
    listener.c                  TCP and Unix listeners
    auth.c/.h                   token loading/wiping/comparison
    tls.c/.h                    TLS context and handshake
    singleflight.c/.h           claims, waiters, negative entries, SWR
    metrics.c/.h                bounded Prometheus/HTTP endpoint
    atomic_commit.c/.h          cross-engine commit orchestration
```

Keep `QueueStore`, `QueueTx`, and `StreamStore` opaque in public headers.
Private engine definitions live in exactly one internal header per engine.

## Dependency rules

```text
app -> server + protocol + public engine APIs
server -> protocol + public engine APIs + platform + common
protocol handlers -> public engine APIs + common
cache/queue/stream -> common
engine internals -X-> Conn, Loop, HTTP, TLS, or protocol handlers
```

Additional rules:

- `queue_internal.h` is visible only to `src/queue/*`; `stream_internal.h` is
  visible only to `src/stream/*`.
- Only `queue_wal.c` owns `wal_seq`, `synced_seq`, `syncer`, `sync_cond`, and
  WAL-failure transitions. It exposes narrow internal append/wait/drain APIs
  whose lock preconditions are asserted in tests.
- Only `queue_message.c` mutates list links, tag-index membership, ready hints,
  per-message WAL footprint, and depth/inflight/TTL counters.
- Only `stream_state.c` mutates retained-list links and incremental live-state
  accounting. WAL and retention modules request state transitions through it.
- Only `atomic_commit.c` coordinates both cache persistence and queue
  transactions.
- Engine metric APIs return bounded snapshots or invoke bounded callbacks;
  engines never format JSON, Prometheus, or HTTP.
- Build manifests list each production source exactly once. Unit tests link the
  same module libraries as production instead of compiling alternative source
  combinations.

## Context and file-size guardrails

- Aim for one responsibility and roughly 150-500 formatted lines per `.c`
  file.
- Treat 600 lines or 25 KB as a soft review threshold. Exceeding either needs a
  written cohesion reason or a further split.
- Keep functions normally below 80 lines. State machines may exceed this only
  when splitting would obscure transitions.
- One function definition per normally formatted block. Handwritten minified
  or multi-function lines are forbidden.
- Do not split a tight per-message loop across exported calls merely to satisfy
  a line budget. Hot-loop cohesion and measured performance take priority.
- Prefer explicit context parameters over new process globals.
- Avoid vague modules named `util`, `helpers`, or `manager`.

Add a source-size report to CI. Initially allowlist the current hotspots and
remove each exception as its split lands. Exclude generated, vendored, and
benchmark fixture data.

## Incremental protocol dispatcher

Replace `conn_process` family by family without assuming one read equals one
frame. Use a result contract equivalent to:

```c
typedef enum {
    PROTO_HANDLED,
    PROTO_NEED_MORE,
    PROTO_DEFERRED,
    PROTO_CLOSE
} ProtocolAction;

typedef struct {
    ProtocolAction action;
    size_t consumed;
} ProtocolResult;
```

Every family handler must:

1. inspect incomplete input without consuming it;
2. validate its opcode-specific shapes and configured limits;
3. execute only when the complete request is available;
4. report exactly how many bytes were consumed;
5. append through bounded response helpers without copying keys/values merely
   to cross a module boundary;
6. preserve deferred singleflight responses;
7. leave socket flushing, event interest, and backpressure to the connection
   layer.

Move batch state into a dedicated protocol state object. Preserve streamed GET
batch responses, queue batch all-or-bounded-partial semantics as documented,
and the 256-entry protocol cap. Do not create one module per opcode; group by
shared engine invariants.

## Storage-format warning

The cache, queue, and stream checksum and record layouts resemble each other
but are not automatically interchangeable. Before moving shared byte or CRC
code:

- add byte-for-byte golden fixtures for every current record type, including
  queue batches' individual records and stream append batches;
- preserve concatenated versus format-specific checksum composition;
- preserve legacy replay, valid-prefix truncation, and stale checkpoint-temp
  cleanup;
- compare WAL and snapshot bytes before and after each extraction;
- cover torn headers, torn payloads, corrupt checksums, invalid lengths, and
  interruption at checkpoint rename/fsync boundaries.

Do not standardize disk formats during modularization. A format change needs a
new version, migration and rollback plan, and separate review.

## Required migration sequence

### Phase -1 — reconcile queue synchronization

Complete the mandatory audit above. Establish the corrected committed source
as the only valid performance and correctness baseline.

Exit gate: implementation, lock comments, architecture docs, queue docs, TSan,
concurrency tests, and benchmarks agree.

### Phase 0 — freeze equivalence and performance gates

1. Run the full Make and CMake test matrices.
2. Capture queue, stream, cache, exchange, recovery, binary-size, and memory
   baselines from the corrected source.
3. Use independent client processes for cache scaling; never compare against
   the known-distorted multi-threaded client result.
4. Add golden wire/WAL/snapshot fixtures and incremental-accounting recount
   assertions before moving persistence code.
5. Record lock ownership and startup/shutdown order in module contracts.

The existing benchmark policy remains: a like-for-like regression greater than
10% blocks acceptance unless profiling gives a measured justification. For
queue and stream work, also protect performance shape: costs must remain flat
with retained depth where the completed tuning made them flat.

### Phase 1 — format and split the stream engine

1. Reformat `src/stream.c` with no logic changes. Review ignoring whitespace.
2. Introduce `stream_internal.h` and annotate mutex ownership.
3. Extract stats and group membership first.
4. Extract retained-state mutation/accounting as one cohesive unit.
5. Extract retention, then WAL/group-fsync/compaction. Keep their sequencing
   interface narrow and explicit.
6. Keep the current single stream-store mutex during extraction; concurrency
   redesign is out of scope.

Exit gate: stream/fuzz/protocol/recovery/fault-injection/ASan/TSan tests pass;
append and commit stay flat at depth; recount equals incremental accounting;
group-fsync and single-writer latency remain within the gate.

### Phase 2 — split the queue engine without undoing tuning

1. Introduce `queue_internal.h` only after the synchronization audit.
2. Extract cold stats and consumer code first.
3. Extract exchange routing while preserving stable target ordering and pointer
   lifetime.
4. Extract queue message primitives as the sole owner of list/index/hint and
   live-byte invariants.
5. Move single and batch delivery operations onto those primitives.
6. Extract atomic transactions as one unit; never split the commit window
   across ambiguous ownership.
7. Extract WAL/group-fsync, then checkpoint/replay, only after golden fixtures
   cover every record and interruption boundary.
8. Keep `queue.h` source-compatible.

Exit gate: focused tests plus queue failure/crash/exchange/atomic/fuzz/protocol,
checkpoint SIGKILL, ASan/UBSan, and TSan pass. Depth-scaling, ready-hint,
group-fsync, batch, and independent-queue benchmark rows remain within the
regression gate and preserve their tuned complexity.

### Phase 3 — extract cold server services

Move low-risk leaves before protocol dispatch:

1. auth and secret-file validation;
2. TLS context and handshake support;
3. metrics rendering and HTTP endpoint;
4. cache WAL/replay/snapshot/persistence policy;
5. singleflight/SWR behind an opaque service interface;
6. cross-engine atomic orchestration.

Introduce `ServerContext` gradually. Do not migrate all globals in one patch.

Exit gate: `server.c` no longer owns security, metrics HTTP, cache persistence,
singleflight storage, or atomic implementation details; focused security,
persistence, stampede, and atomic suites pass.

### Phase 4 — split protocol families

1. Add bounded request cursors and response builders.
2. Move one family at a time: admin, cache, queue, exchange, stream,
   singleflight, atomic.
3. Move each family's batch opcodes with the same family, not into a generic
   batch module.
4. Run that family's wire/fuzz tests and the appropriate benchmark before the
   next move.
5. Reduce `conn_process` to incremental dispatch, consumed-byte handling,
   deferred-response handling, and connection-close policy.

Exit gate: adding a normal opcode touches `opcodes.h`, one handler, an engine
API if required, tests, and `PROTOCOL.md`; it does not edit an all-feature
switch.

### Phase 5 — isolate networking and lifecycle

1. Move connection buffering/backpressure/TLS I/O to `connection.c`.
2. Move event callbacks and worker loops to `event_loop.c`.
3. Move TCP/Unix listener creation to `listener.c`.
4. Reduce `main.c` to configuration, validated startup order, signal handling,
   thread ownership, and reverse-order shutdown.
5. Keep `platform.c` as the only kqueue/epoll boundary.

Do not change event-loop scheduling or dispatch budgets without new profiling
showing a server bottleneck.

### Phase 6 — client cleanup

Client splits are lower priority and do not block C modularization.

- Node.js: constants/codecs, connection pool/transport, and feature APIs while
  preserving the exported `Client`.
- Go: transport/pool, cache operations, and batch operations in the same
  package.
- Rust and Java remain below the urgent context threshold; split only as their
  feature surface grows.

## Build and test structure

Create production source groups that mirror ownership, for example:

```text
kuttidb_common
kuttidb_cache
kuttidb_queue
kuttidb_stream
kuttidb_protocol
kuttidb_server_runtime
```

The exact number may be smaller. CMake and Make must share the same source
groupings, preferably through central lists or included fragments. Sanitizer,
test, benchmark, shared-library, and production targets must not silently omit
a moved source.

Move tests gradually toward:

```text
tests/unit/cache/
tests/unit/queue/
tests/unit/stream/
tests/unit/protocol/
tests/concurrency/
tests/integration/
tests/fixtures/wal/
tests/fixtures/protocol/
```

A module is not complete if it can only be tested by compiling a private `.c`
file through a test-only source combination different from production.

## Coding-agent navigation contract

After the real paths exist, add a short root `AGENTS.md` and
`docs/CODE_MAP.md`. Do not publish path guidance before the paths exist.

The code map must identify:

- ownership for cache, queue, exchange, stream, atomic, singleflight,
  persistence, networking, security, and metrics changes;
- public and private headers for each module;
- lock ownership and cross-module order;
- incremental accounting owners and required recount tests;
- minimum focused tests and performance rows for each module;
- wire and disk-format documentation.

The root agent instruction should require agents to read the code map and
relevant public header first, open only direct module dependencies and focused
tests, search call sites before contract changes, preserve hot-path invariants,
and update the map whenever ownership moves.

## Per-change checklist

Every modularization change must state:

- responsibility moved and old/new ownership;
- public API impact (normally none);
- wire/disk byte impact (must be none during extraction);
- locks required on entry and held/released internally;
- grouped-fsync sequence and acknowledgement impact;
- index/hint/incremental-accounting impact;
- focused, recovery, sanitizer, and concurrency tests run;
- relevant before/after benchmark rows;
- source-size exception removed or remaining.

Mechanical moves and behavioral fixes belong in separate commits.

## Definition of done

The refactor is complete when:

- the queue synchronization audit is resolved and documentation matches code;
- `server.c` is gone or is a small facade, and no parser handles all families;
- queue message/index, delivery/batch, exchange, transaction, WAL/fsync,
  checkpoint, consumer, and stats ownership is explicit;
- stream source is normally formatted and split by state/accounting, topic,
  retention, group, WAL/fsync/compaction, and stats responsibilities;
- cache persistence and snapshots are outside networking/protocol code;
- engine internals do not depend on connection or HTTP types;
- no handwritten source exceeds the context budget without a documented
  cohesion reason;
- Make, CMake, unit, integration, crash/recovery, fault-injection, sanitizer,
  TSan, and benchmark gates pass;
- protocol 1.6, WAL/snapshot/embedded formats, durability, client APIs, resource
  bounds, tuned performance shapes, and acknowledgement points are unchanged;
- a coding agent can locate, change, and verify one normal feature without
  loading unrelated engines.
