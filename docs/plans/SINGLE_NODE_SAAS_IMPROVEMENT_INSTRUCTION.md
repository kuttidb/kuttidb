# Agent instruction: make KuttiDB the small-server cache, queue, and event log

## Mission

Improve KuttiDB for small and midsize SaaS builders who want cache access,
background jobs, and replayable events from one lightweight server. Optimize
for a developer running one application on a modest VPS: one executable, one
configuration, one data directory, predictable resource use, and useful
performance while all three workloads run together.

The product ambition is **the SQLite-like operational simplicity of cache,
queues, and streams combined**. This describes ease of adoption and operation;
it does not imply SQLite's storage format, SQL, transaction model, or that the
network server is an in-process library. Preserve the existing managed local
and embedded options and their distinct contracts.

Aim to surpass a same-host Redis + RabbitMQ + Kafka deployment in total
resource efficiency, application latency, and operational effort for these
workloads. Earn narrower per-engine performance claims with measurements.
Feature parity with three distributed platforms is not the acceptance test.

This document is an implementation assignment for a future agent. Its review
baseline is commit `74a40c3`, inspected on 2026-09-05. Proposed targets below
are engineering goals, **not measured results or current product guarantees**.

## Read first and reconcile existing work

Follow [AGENTS.md](../../AGENTS.md). Read the
[architecture](../design/ARCHITECTURE.md),
[durability contract](../design/DURABILITY.md),
[native protocol](../design/PROTOCOL.md),
[queue](../messaging/QUEUES.md), [exchange](../messaging/EXCHANGES.md),
[stream](../messaging/STREAMS.md), and
[benchmark](../operations/BENCHMARKS.md) documents before changing behavior.

Use [ROADMAP.md](ROADMAP.md) as historical implementation evidence. This
instruction defines the priorities for the single-server SaaS effort; it
does not make every unchecked historical milestone a prerequisite.

Reconcile these existing assignments with the checked-out source:

- [MODULARIZATION_INSTRUCTION.md](MODULARIZATION_INSTRUCTION.md): extract
  modules when needed for a specific change. Its historical queue-lock
  findings must be rechecked; do not assume they remain unfixed.
- [COMPATIBILITY_GATEWAYS_INSTRUCTION.md](COMPATIBILITY_GATEWAYS_INSTRUCTION.md):
  retain as a separate, optional adoption track. The core product must deliver
  all three capabilities without requiring that gateway.
- [MANAGED_LOCAL_SERVER_IMPLEMENTATION_PLAN.md](MANAGED_LOCAL_SERVER_IMPLEMENTATION_PLAN.md):
  build on the existing launcher and SDK lifecycle support.
- [MANAGEMENT_UI_IMPLEMENTATION_PLAN.md](MANAGEMENT_UI_IMPLEMENTATION_PLAN.md)
  and [FULL_MANAGEMENT_API_INSTRUCTION.md](FULL_MANAGEMENT_API_INSTRUCTION.md):
  extend existing operational surfaces instead of creating another console.

The protocol reference currently says v1.8, while older plans mention v1.2,
v1.5, or v1.6. Verify source capabilities and compatibility tests; do not
change wire versions to match stale prose. Keep new documentation under
`docs/`, link user-facing additions from both documentation indexes, and
preserve unrelated working-tree changes.

## What the review found

These are source-level observations, not a comprehensive correctness audit.
Revalidate them against the revision on which implementation starts.

| Area | Existing implementation and evidence | Implication for this assignment |
|---|---|---|
| Cache | `src/kuttidb.c` and `src/server.c`: sharding, TTL, batches, pipelining, eviction, WAL/snapshots, singleflight and stale refresh | Preserve the fast path. Do not spend the first milestone rebuilding cache primitives that already work. |
| Queues and routing | `src/queue.c`: durable ACK/NACK, retry/DLQ, routing, batches, tag indexes, ready hints, per-queue locks, grouped fsync, checkpoints | Extend proven semantics. Historical group-commit and index improvements already exist. |
| Queue memory | `Message` retains the payload in memory; `Queue` has `max_depth` | A message-count limit does not bound bytes across queues, in-flight copies, and fanout. Large backlogs need explicit byte admission and eventually disk-backed payloads. |
| Streams | `src/stream.c`: every `StreamRecord` contains its payload; `StreamPart` is a linked list; `stream_fetch` starts at the retained head | Retained history consumes RAM, and reads near the tail traverse older records. Disk segments and offset indexes are the main storage opportunity. |
| Stream retention accounting | `record_bytes` counts body and key bytes; `enforce` uses those bytes and optional age limits | `max_bytes` is not a process-memory ceiling. Record metadata, empty records, topics, groups, and temporary allocations require independent limits. |
| Stream maintenance | `stream_compact_locked` rewrites retained state while holding the store mutex | Correct checkpointing exists, but maintenance can interfere with latency. Measure pauses under realistic retained history. |
| Resource budget | `src/server.c` passes `--max-memory-mb` to `kuttidb_set_budget(g_cache, ...)` | The flag currently budgets cache, not the whole process. Add a clearly named total budget with compatible configuration semantics. |
| Combined commits | Existing atomic operations coordinate cache mutations and queue/exchange delivery using prepare/commit records | This is a useful differentiator. It does not currently establish a transaction across cache, queues, and streams, or across an application's SQL database. |
| Operations | Managed startup, ownership locks, TLS/auth, metrics, an admin API, SDKs, and a console already exist | Make deployment and recovery easier using these foundations. The optional console currently requires its own JavaScript runtime/gateway. |
| Backup | `docs/operations/DEPLOYMENT.md` recommends a clean stop and copying the complete persistence set | Turn this into a verified workflow first; consistent online backup needs a coordinated design. |
| Performance evidence | Existing tables mostly measure cache or individual messaging engines and contain historical before/after runs | They do not establish superiority over competitors or combined-workload behavior on constrained Linux hosts. |

The native queue concurrency tests and checked queue mutex helpers are present.
Do not repeat the old global-queue-lock diagnosis as a current finding without
a reproducer. Likewise, the documented historical cache client-scaling cliff
was attributed to the load generator; use independent client processes.

Review validation: `make test` completed successfully on the local macOS
checkout, including the C/Python suites and Go, Java, Rust, and Node.js client
smoke tests. The first sandboxed attempt could not bind local sockets; the
rerun with local socket access passed. No new constrained-Linux, soak, or
competitor benchmark was run for this documentation review. Changes from
other work appeared during the review; the implementing agent must capture
a fresh revision and working-tree baseline rather than treating this as a
release certification.

## Product boundary and invariants

1. Keep the default data service one C executable and one process. Internal
   files and bounded worker threads are allowed. Do not require a JVM, Erlang,
   Node.js, Redis, another database server, or a control-plane daemon to run
   cache, jobs, and streams. TLS dependencies must be disclosed separately
   from the dependency-free plaintext build.
2. Prioritize Linux amd64/arm64 VPS and container deployments; preserve macOS
   development support. Replication, clustering, Windows completion, Kafka
   transactions, SQL, Lua, and rich Redis data structures are outside this
   effort unless a measured customer requirement changes the scope.
3. Preserve durable acknowledgement points and at-least-once delivery. Group
   fsync and batching may amortize work, never acknowledge before the promised
   durability point. Separate process-crash, OS/power-failure, and machine-loss
   guarantees. SIGKILL alone does not test loss of the OS page cache.
4. Cache eviction must never evict durable jobs. Stream retention may remove
   events according to the explicit policy, independently of consumer lag.
   Administrative purge/delete and expiry remain explicit loss policies.
5. Reject or backpressure overload with bounded waits and clear errors.
   Preserve room for ACKs, draining, recovery, and health inspection. Never
   accept durable work and silently discard it to meet a memory target.
6. Preserve native wire layouts, capability negotiation, SDK behavior, WAL
   recovery, binary keys/values, and existing managed directory ownership.
   Version storage changes, provide migration, and explicitly reject newer
   unsupported formats. Never let an older binary reinterpret new files.
7. Separate speculative observations from reproduced defects. Fix correctness
   failures before tuning the affected path. Every changed acknowledgement or
   recovery behavior must have a matching crash test.

## Workload contract and proposed release targets

Use three deployment profiles. Memory is the **whole service cgroup limit**,
including charged filesystem cache, not just configured cache capacity.
Load generators run outside that budget and are measured separately.

| Profile | Host allocation for KuttiDB | Purpose |
|---|---|---|
| Small | 1 vCPU, 256 MiB RAM, local SSD | Minimum supported mixed-workload profile |
| Standard | 2 vCPU, 512 MiB RAM, local SSD | Primary SaaS performance and release gate |
| Growth | 4 vCPU, 2 GiB RAM, local SSD | Scaling shape and larger working sets |

Use fixed manifests recording CPU model, throttling, kernel, filesystem,
storage, architecture, binary revision, TLS, limits, workload, and durability.
Retain at least 20% memory headroom at the steady-state target. Short-lived
allocation peaks, recovery, and maintenance must still fit the hard limit.
Do not certify a profile based only on macOS or an unconstrained workstation.

Freeze the following initial target workload before optimization; baseline
measurement may justify a documented revision before the implementation
gate is locked. Never lower it afterward simply to make a failing run pass.

- Standard profile: 20,000 cache operations/s (90% GET, 10% PUT), 1,000
  complete job publish/deliver/ACK cycles/s, and 2,000 durable stream appends/s
  with a live consumer committing offsets in documented batches, all together.
- Use 20,000 resident cache keys, 32-byte keys and 1 KiB bodies, 8 queues,
  4 topics with 4 partitions each, and 32 total native client connections.
  Job payloads and stream values are also 1 KiB. Cache uses the documented
  periodic mode; durable jobs and stream appends retain fsync-before-success.
- Run an unbatched producer baseline and a separate bounded-batch profile
  (up to 64 records, maximum 2 ms client batching delay). Count completed
  jobs and records, not batches, as throughput. Report each mode separately.
- Proposed Standard p99 targets: cache round trip at most 2 ms; durable
  publish/append confirmation at most 20 ms; job publication to durable ACK
  at most 50 ms with a no-op worker. Include batching delay and client queues.
  Slow-business-worker scenarios measure queue age separately.
- Small must sustain at least half these arrival rates within the same
  latency targets. Growth must show increased sustainable mixed-workload
  capacity; investigate flat scaling before adding threads or locks.

| Gate | Proposed acceptance requirement |
|---|---|
| Idle footprint | Plaintext core RSS at most 10 MiB and idle CPU below 1% of one core over 5 minutes; record TLS and admin-enabled costs separately. Preserve any tighter verified baseline. |
| Loaded footprint | No OOM, unbounded buffers, or steadily growing live allocations in a 24-hour Standard soak. Report RSS/PSS, cgroup current/peak, page cache, and engine accounting. |
| Durable history beyond RAM | Standard retains and replays at least 4 GiB of stream payload under its 512 MiB cgroup limit, with cache and jobs active, after segmented storage lands. |
| Job backlog beyond RAM | Standard retains at least 1 GiB of durable queued payload, then drains it correctly under the same limit after queue paging lands. |
| Isolation | At the fixed target arrival rates, normal maintenance and a controlled slow-consumer scenario keep p99 within 2x the corresponding normal mixed-run baseline and the absolute SLOs. Beyond admitted capacity, return bounded overload responses instead of hanging. |
| Recovery | For the 4 GiB stream + 1 GiB queue fixture, proposed cold recovery to usable service within 30 seconds on Standard; preserve all acknowledged unexpired data and offsets. Record cold and warm filesystem-cache runs separately. |
| Regression | No unexplained regression over 10% in comparable existing release benchmarks. Use repeat-run distributions, not one short sample. |
| Packaging | Publish stripped binary size, dynamic dependencies, total install size, startup time, threads, and open files for plaintext and TLS builds; additions require measured justification. |

These targets establish usefulness, not competitor wins. Report every missed
target, why it missed, and the next bounded action. Do not claim production
readiness from throughput alone.

In the slow-consumer scenario, apply latency SLOs to unaffected cache traffic,
publisher confirmations, and actively draining queues. Report backlog age and
lag for the deliberately paused consumer; do not require its jobs to finish
within the no-op worker latency target while it is paused.

## Execution order

Deliver each phase in reviewable changes with its own evidence. Do not
combine a wholesale refactor, storage migration, and performance rewrite.
Ship useful increments: trustworthy limits before larger disk backlogs.

### Phase 0 — establish a trustworthy baseline

1. Record commit, dirty-tree state, build flags, existing feature capabilities,
   and supported toolchains. Run `make test` before implementation. Distinguish
   environment restrictions from product failures using captured server logs.
2. Add a reproducible combined-workload harness using existing clients and
   benchmark patterns. Keep traffic, payloads, seeds, and settings versioned.
   Produce machine-readable measurements and a concise human report.
3. Sweep offered load until SLOs fail. Track per-operation p50/p95/p99/p99.9,
   completed goodput, errors/timeouts, queue age, stream lag, CPU, memory,
   disk space, bytes written, fsyncs, and maintenance pauses. Do not hide
   timeouts by dropping them from latency accounting.
4. Profile the server and load generator separately. Determine whether
   synchronous durability waits inside native request dispatch stall an event
   loop's unrelated clients. Measure checkpoint pauses and stream fetch cost
   as retained offsets grow. Record findings before selecting an optimization.
5. Refresh the current limitations in the roadmap with source-backed facts.
   Do not reuse historical benchmark values as measurements of this commit.

Exit: baseline manifests and commands are reproducible on the three profiles;
the top bottlenecks have evidence; current test failures are fixed or clearly
recorded as blockers before the affected phase proceeds.

### Phase 1 — put the whole service on a budget

Implement shared admission accounting with separate cache, queue, stream,
connection, coordination, metadata, and maintenance categories. Keep cache
eviction separate from durable-data admission. Preserve the meaning of the
existing cache memory flag; expose the effective total and per-engine limits.

- Bound allocated bytes, object counts, outstanding operations, response
  buffers, and temporary copies. Include slab/allocator overhead, record
  headers, empty messages, queue/topic/group declarations, durable consumers,
  deleted-object tombstones, retry state, and future deduplication indexes.
- Reserve capacity before WAL writes and visible mutation. Reserve every
  fanout destination and batch cost; release reservations on all error paths.
  Define normal fanout/batch partial-success behavior explicitly and preserve
  atomic-operation all-or-nothing behavior.
- Add per-queue byte limits, per-topic byte/count limits, total durable-storage
  limits, and a disk free-space reserve. Account for checkpoint double space,
  segment rolling, temporary files, logs, snapshots, and retained backups.
  Free-space checks are advisory; handle actual ENOSPC/EIO safely too.
- Keep bounded emergency capacity for ACKs, deletes, DLQ transitions and
  maintenance so a full store can make progress. A full DLQ must not erase its
  source message. Document when operator space reclamation is required.
- Add stable quota, overload, and persistence-failure responses with bounded
  client backoff. Surface used/reserved/limit values and rejection counts in
  the existing metrics and admin API without unbounded label cardinality.

Exit: concurrent variable-size publish, fanout, tiny/empty-record floods,
slow sockets, stalled consumers, declaration churn, allocation failures, and
disk exhaustion stay within the declared envelope. No acknowledged durable
record disappears except under its explicit expiry/retention/delete policy.
Recovery admission must stream or refuse safely without rewriting good data
when an existing dataset exceeds a newly lowered limit.

### Phase 2 — retain history on disk with bounded RAM

Start with streams, then use measured lessons for queue payload storage.
Write an ADR describing format, durability, recovery, indexes, file lifetime,
and migration before implementing the new storage format.

For streams:

- Add append-only segment files with checksummed, versioned records; bounded
  active buffers; sparse offset indexes; and a bounded read cache. Index and
  segment metadata must not grow without a declared bound as history grows.
- Seek near the requested offset; avoid traversing all earlier records. Keep
  per-partition ordering, exact binary keys, existing offsets, batches,
  retention, consumer commits, and capability-negotiated responses intact.
- Make group fsync cover the actual segment and metadata dependencies before
  confirmation. Coordinate roll, manifest publication, index recovery,
  rename, directory sync, and reader references before deleting old files.
- Retention should usually remove expired whole segments. Bound the excess
  retention space to a documented segment allowance; handle partially live
  boundary segments without rewriting the entire retained set on every pass.
- Recover from validated metadata and bounded active tails without loading
  every payload. Rebuild damaged derivative indexes safely; distinguish an
  incomplete tail from corruption within previously committed history.

For queues:

- Page cold durable payloads to disk; keep bounded ready/in-flight windows and
  indexes. A full per-message RAM index is not enough for the backlog gate.
- Preserve delivery identity, lease ownership, ACK/NACK, retries, expiry,
  DLQ transfers, routed copies, checkpoints, and atomic cache-event recovery.
- Keep hot queues fast. Measure ready lookup, delayed retries, large in-flight
  sets, reconnect storms, and backlog drains. Add timer/ready indexes only
  where the measured path warrants them.

Exit: the beyond-RAM and recovery gates pass; retained history growth does
not create proportional process memory growth or linear prefix fetch work.
Test crashes at every persistence boundary, disk-full rolls, truncated
records, index loss, interrupted migration, and restoration of old fixtures.
Publish an upgrade/rollback matrix; rollback may require restoring a backup,
and must never silently start an old binary on an incompatible directory.

### Phase 3 — keep mixed workloads responsive

Optimize measured interference after the accounting and storage foundations.
If durable calls block event loops, introduce a bounded completion mechanism
or a small bounded I/O worker pool, preserving response ordering, connection
lifetime, cancellation, durability waits, and shutdown draining. Never use a
thread per connection or an unbounded request queue.

Budget work per event-loop turn, batch, and maintenance slice. Limit pending
responses for slow readers. Keep metrics and administrative browsing bounded.
Make fsync coalescing adapt to actual concurrent work without inserting an
unconditional delay into low-load operations. Only split stream locks further
when profiling shows contention; preserve a documented lock order.

Exit: mixed workload and isolation gates pass during segment cleanup,
checkpoints, metrics scrapes, a hot queue, and a lagging stream consumer.
The no-op worker benchmark verifies the complete durable job lifecycle,
not publish-only speed. Applicable race and sanitizer checks pass.

### Phase 4 — make the three capabilities useful together

Build a small SaaS example around cached account settings, background report
jobs with retries, and a replayable activity feed. Use one KuttiDB instance
and the existing Python and Node.js clients. Show restart and worker-failure
behavior, not just successful requests.

Prioritize bounded improvements that remove application glue:

- Reliable SDK consumer loops: deadlines, batching, prefetch, jittered retry,
  graceful drain, heartbeat/lease renewal, partition loss, and visible lag.
  Async APIs must not block the application's event loop. Keep SDK behavior
  consistent and capability-gated across supported languages.
- Safe retry semantics: a connection timeout can mean an operation committed.
  Do not automatically retry non-idempotent writes as though they failed.
  For producer deduplication, specify identity scope, payload mismatch,
  concurrent retries, persistence, expiry horizon, storage bounds, and the
  original result returned on retry. Do not label deduplication exactly-once
  processing or store its authority in evictable cache entries.
- Small cache primitives with demonstrated SaaS use: conditional put,
  compare-and-swap, increment, and TTL inspection, after verifying which
  already exist. Define atomicity, numeric overflow, TTL, eviction, and
  persistence behavior. Avoid a large Redis command-compatibility project.
- App/namespace resource limits and scoped credentials if multiple apps share
  an instance. A key prefix alone is not an authorization boundary. Keep
  admin credentials separate from application credentials and test cross-scope
  cache, queue, routing, stream, and inventory access.

Demonstrate existing atomic cache-plus-queue operations first. Their guarantee
is atomic commit/recovery, followed by normal cache eviction/expiry and queue
delivery policies; it does not make cache state a permanent system of record.
Do not imply that a PostgreSQL/MySQL update commits with a KuttiDB operation.
Provide an application outbox/idempotent-handler example for that boundary.

Only after the earlier phases pass, evaluate one bounded cross-engine
extension, such as cache-plus-stream append or job-ACK-plus-event append.
Require a concrete application case, a commit-coordinator ADR, bounded
reservations, in-doubt retry semantics, recovery and checkpoint coordination,
and crash tests at every boundary. Do not assemble a transaction from a series
of independent API calls or expand into a general transaction language.

Exit: the example uses all three capabilities from one endpoint, processes
retries safely, demonstrates replay, and recovers from a killed worker/server.
Each added primitive has a documented use case and compatibility coverage.

### Phase 5 — make deployment, recovery, and upgrades ordinary

Improve the existing CLI and managed local lifecycle instead of adding a
required orchestrator. Keep a minimal default configuration with explicit
cache durability, durable messaging, total memory, disk reserve, and retention
settings. Display effective configuration and actionable startup errors.

- Make verified cold backup/restore a supported command or packaged workflow.
  Capture the full cache snapshot/WAL, queue WAL, stream files, and transaction
  metadata set, including configured paths outside the default directory.
  Write checksums and a format/version manifest; restore into a fresh directory
  and validate before use. Define instance identity and credential handling.
- Online backup is a separate increment: pin a coordinated commit/checkpoint
  boundary and required files, then copy without unbounded pauses. Arbitrary
  copies of three live WALs are not a consistent backup. Preserve bounded
  disk/memory use and clean up failed backup jobs.
- Add a read-only integrity/doctor workflow, tested upgrade fixtures, and
  useful readiness states for recovery, disk pressure, and persistence failure.
  Destructive salvage must be explicit and preserve originals; do not silently
  truncate mid-log corruption and advertise complete recovery.
- Add reproducible Linux release binaries, checksums, and resource-profile
  examples to the existing release process. Test restart, ownership conflicts,
  backup restore, and upgrade using the packaged artifact.
- Keep the console optional. A core-only install must support the complete
  example and operational workflow. If a browser UI becomes part of the
  single-binary package, embed built static assets and preserve the existing
  authentication boundary; do not make Node.js a runtime prerequisite.

Exit: a fresh user can install, start, exercise all three capabilities, back
up, restore, and upgrade using a short reproducible guide. A restore test
checks actual values, message identities, offsets, and transaction outcomes,
not just whether the server starts.

### Phase 6 — demonstrate the competitive advantage

Compare both individual primitives and the combined application. Pin actual
Redis, RabbitMQ, Kafka, client, and runtime versions and publish configuration
files. Use maintained single-node configurations, local SSD storage, equal
hardware budgets, comparable payloads, and equivalent supported features.

1. Cache: match read/write mix, key distribution, hit rate, TTL, value size,
   batching, transport, and persistence. Keep volatile, periodic/AOF, and
   fsync-before-response cases in separate tables.
2. Jobs: use durable queues/messages, publisher confirms, manual consumer
   acknowledgements, matching prefetch, and equivalent retry/expiry policy.
   Distinguish the publisher confirmation from the consumer ACK semantics;
   document differences that prevent exact durability equivalence.
3. Streams: compare durable append, live consumption, consumer commits, and
   cold replay with matching partitions, retention, compression settings,
   batch sizes, and supported acknowledgement guarantees. Kafka `acks=all`
   with replication factor one is not proof of per-record fsync-before-ACK.
   Inspect the pinned broker's flush behavior and mark unmatched guarantees
   explicitly. Never silently weaken KuttiDB's durability for a graph.
4. Whole application: compare one KuttiDB process with all three competitor
   services concurrently under the same total host allocation. Include the
   Kafka controller role/runtime and all mandatory supporting processes.
   Report both same-budget results and resources each setup needs to meet the
   same SLO. If a stack cannot run in a profile, publish the configuration and
   startup failure; do not manufacture a zero-throughput speedup ratio.

Run 100 B, 1 KiB, and 16 KiB payloads; small/large backlogs; uniform/hot keys;
fast/slow consumers; TLS off/on; low load, target load, and overload. Use
warm-up and at least five independent steady-state runs, plus the 24-hour
soak. Separate warm/cold cache states and run order effects. Measure offered
load with a bounded open-loop driver or corrected latency accounting so
stalling the driver cannot make server tail latency appear better.

Publish successful SLO-compliant operations per CPU-second, peak total
memory, disk/write amplification, startup/recovery, and deployment steps.
For the headline combined-workload target, aim for at least 50% less peak
service memory and at least equal SLO-compliant goodput versus the three
service stack. This is a hypothesis to test, not a required conclusion.
Report losses and non-equivalent cases alongside wins.

Reference semantics checked during this review:

- Redis supports distinct persistence policies; select and disclose the
  policy used in every comparison. [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/).
- Redis's benchmark guidance discusses client connections, pipelining, and
  persistence as comparison variables. [Redis benchmark guidance](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/).
- RabbitMQ distinguishes publisher confirms from consumer acknowledgements.
  [RabbitMQ acknowledgement documentation](https://www.rabbitmq.com/docs/confirms).
- Resource pressure is part of broker operation, not just an idle-RSS result.
  [RabbitMQ memory accounting](https://www.rabbitmq.com/docs/memory-use).
- Kafka's persistence and replication design requires care when comparing a
  single broker with local fsync durability. Recheck against the tested
  version. [Kafka 4.1 design](https://kafka.apache.org/41/design/design/).

## Validation and handoff requirements

Run `make test` before submitting every implementation increment, as required
by the repository. Run `make bench-quick` for affected performance paths and
`make bench-queue`, `make bench-stream`, or `make bench-exchange` when their
engines change. Use the relevant existing ASan/UBSan/TSan targets for storage,
allocation, and synchronization changes. Run console lint/tests when `apps/`
or `packages/` change; run affected SDK suites and managed lifecycle coverage
for client or protocol changes.

Extend failure coverage for post-WAL allocation failure, fsync failure,
short writes, ENOSPC, concurrent checkpoint/roll, client disconnect after
commit but before response, clock changes, stalled consumers, and restarts.
Persist acknowledged operation IDs in the test driver outside the killed
server, reconcile them after recovery, and tolerate unconfirmed operations
that legitimately committed. Test both cache-event commit boundaries and
ordinary messaging; never assert exactly-once external side effects.

Keep artifacts under the existing benchmark/test structure and documentation
under the correct `docs/` category. For every phase, record:

- What changed, the customer problem it resolves, and affected source paths.
- Baseline and resulting measurements with reproduction commands/manifests.
- Correctness/crash-test results, resource limits, and any environment gaps.
- Wire/storage compatibility, upgrade/rollback implications, and remaining
  limitations.
- The next unfinished acceptance gate and the smallest useful next change.

Start with Phase 0 and then Phase 1. Complete the currently selected phase
through its validation and documentation rather than stopping after another
plan. Work in small increments; do not treat this entire program as one PR.
The overall effort is complete only when the resource, correctness,
mixed-workload, recovery, developer-workflow, and comparison evidence is
published with honest outcomes. A feature list or a peak cache benchmark
alone does not establish the intended single-server advantage.
