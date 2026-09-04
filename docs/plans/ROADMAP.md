# KuttiDB roadmap

This roadmap is ordered by correctness and containment.  A milestone is not
complete merely because code exists: it must keep the repository buildable,
pass its stated tests, and document the actual guarantees.

## Baseline record — 2026-08-28

Environment: macOS 15.3 (Darwin 24.3), ARM64, Apple Clang 17, OpenSSL-enabled
build.  `make -B all` completed with `-Wall -Wextra` and no warnings.  The full
existing `make test` suite passed after rebuilding.

| Measurement | Result |
|---|---:|
| Python sequential cache operations | 65,310 ops/s |
| Python 200k batched cache operations | 1,454,284 ops/s |
| Embedded single puts (ctypes included) | 323,274 ops/s |
| Embedded single gets | 892,427 ops/s |
| C benchmark, 1 client, 256-item batches, 100 B values | 1,302,389 ops/s; p50 161 us; p99 354 us |
| C benchmark, 2 clients, same workload | 3,273,430 ops/s; p50 126 us; p99 264 us |
| C benchmark, 4 clients, same workload | 3,324,358 ops/s; p50 244 us; p99 750 us |
| C benchmark, 8 clients, same workload | 1,950,049 ops/s; p50 419 us; p99 2,388 us |
| Idle RSS in benchmark matrix | 1,760–1,792 KiB |
| Loaded RSS in benchmark matrix | 7,408–37,088 KiB |

The recorded pre-refactor matrix reports p50 and p99 batch latency.  The
benchmark now also reports p95, but it still does not isolate PUT, GET, and
DELETE latency.  Queue and stream baselines do not yet exist because those
engines do not exist.  Future benchmark work must add these measurements before
accepting a major milestone.

Progress sample — 2026-08-29, after the stream generation, metrics endpoint,
and fuzz additions (`make bench-quick`, same workload as the baseline row):

| Clients | ops/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| 1 | 904,077 | 272 us | 454 us | 523 us |
| 2 | 2,598,685 | 204 us | 295 us | 314 us |
| 4 | 3,462,964 | 232 us | 528 us | 685 us |
| 8 | 1,869,718 | 390 us | 2,301 us | 2,415 us |

The four-client sample sits ~4% above the recorded baseline, within the
short-run noise the earlier note describes; there is no sign of the >10%
regression gate being crossed.

Verified existing behavior includes CRC-checked WAL recovery, absolute TTL
across restart, snapshot recovery, bounded cache eviction, runtime WAL failure
fail-closed behavior, auth/TLS checks, embedded-region recovery, and Python,
Go, Java, and Rust client smoke tests.

Known baseline constraints:

- Server networking is kqueue-specific, so Linux and Windows cannot currently
  build or run it unchanged.
- The Makefile emits a macOS `.dylib`; there is no cross-platform build system.
- Embedded `CEMBv2` was pointer-based and incompatible; the active `CEMBv3`
  format is offset-based and ASLR-safe.  Automatic socket/pipe/TCP fallback
  and Windows shared memory remain incomplete.
- Queue, exchange, stream, consumer-group, atomic cache-event, stampede,
  replication, Docker, and Kubernetes capabilities are not implemented.

## Milestone 0 — baseline and architecture

Status: **complete**.

- Inspected protocol, security model, Makefile, cache/server/embed sources,
  current tests, benchmarks, and Python, Go, Java, and Rust clients.
- Captured the build/test and quick benchmark baseline above.
- Added [ARCHITECTURE.md](../design/ARCHITECTURE.md) and this roadmap because no
  equivalent documents existed.
- Documented current cache durability and the required single-node limitation.

Exit gate: existing cache build and tests pass, and no current behavior was
changed.  Result: passed on the recorded macOS/ARM64 environment.

## Milestone 1 — portability foundation

Status: **in progress**.

- Implemented an internal event-polling boundary (`src/platform.c`): kqueue on
  macOS and epoll/timerfd/eventfd on Linux.  Server connection logic no longer
  directly references kqueue.  A direct platform test covers read watches,
  one-shot timers, and shutdown wakeups.
- Introduced CMake while retaining Make.  CMake produces native macOS dylibs
  and Linux shared libraries, and the native CI workflow builds/tests both
  macOS and Ubuntu. The local Alpine container build now compiles the server
  successfully on Linux; the full Linux runtime suite remains CI evidence to
  collect, rather than a claim based only on this build check.
- Preserve kqueue on macOS; add an IOCP-equivalent backend on Windows.
- Continue isolating POSIX-specific files, locks, clocks, liveness, mappings,
  Unix sockets, and shutdown.  Produce Windows DLL artifacts after those
  boundaries exist.
- Replaced `CEMBv2` native pointers with a versioned, offset-only `CEMBv3`
  region. The cross-process ASLR test reserves the creator address before
  attaching, then verifies read/write visibility at a different address.
- Added Python `LocalKuttiDB`: it selects CEMBv3 when available, otherwise a
  configured Unix socket or TCP/TLS, with a test covering both paths. Named
  pipes and equivalent selector APIs for other native clients remain pending.
- Add macOS/Linux CI; document and exercise Windows compilation as soon as the
  abstraction is in place.

Exit gate: macOS and Linux cache tests pass; embedded attach is ASLR-safe;
existing v1.1 cache clients remain compatible; no cache-only regression over
10% without an explicit measured justification.  The event refactor passes the
full macOS cache suite and sanitizer checks.  A representative four-client
post-refactor sample is 3,487,176 ops/s versus the original 3,324,358 ops/s;
the short benchmark is noisy, so this is evidence of no obvious regression,
not a release-grade comparison.  Native Linux result evidence, automatic
transport fallback, and Windows work remain required before this milestone can
complete.

Status update — 2026-08-29: the native Linux runtime evidence now exists.  A
complete forced rebuild plus the full C suite (core, platform, queue, queue
failures, queue crash, exchange, atomic, stream, WAL fuzz, embed ASLR) and all
twelve Python protocol/recovery/security/fuzz suites pass inside an Alpine
3.21 (musl, epoll) container.  Revalidated — 2026-08-29 (round 2): the same forced Linux rebuild plus the
full C suite, all twelve Python protocol/recovery/security/fuzz suites, and a
CMake/CTest build now cover the code as of the durable named consumers and
labeled metrics work: `LINUX C SUITE OK`, `LINUX PYTHON SUITE OK`, CTest
10/10.  Running that suite originally caught a real Linux-only
defect the macOS sanitizers could not: the metrics renderer used a 128 KiB
stack buffer on a thread with musl's 128 KiB default stack, crashing the
server on first scrape.  The buffer is now heap-allocated and the metrics
thread gets an explicit 1 MiB stack.  The Makefile now builds
`libkuttidb_embed.so` on Linux (previously dylib-only), which also made the
automatic shared-memory → socket/TCP transport fallback pass on Linux.
Remaining for Milestone 1: Windows (documented blocker, see Milestone 7) and
CI-collected Linux evidence for the client toolchain smoke tests.

## Milestone 2 — durable queue MVP

Status: **complete**.

- Added a separate queue engine and CRC-checked queue WAL. Durable declaration,
  publish confirmation, durable delivery/ACK records, crash recovery, manual
  ACK/NACK/requeue, visibility leases, redelivery indicators, and bounded queue
  depth are available through the native Python client and protocol.
- Each consume response supplies both a stable message ID and a one-use,
  connection-bound delivery tag. A stale or other-connection ACK/NACK cannot
  affect the delivery, and closing its consumer connection requeues it
  immediately.
- Added core and protocol tests for crash recovery, visibility, delivery-token
  ownership, and connection-close requeue. Queue bytes are separate from cache
  accounting and cannot be evicted by cache memory pressure.
- Publish TTLs and durable delayed NACK retries are exposed. The queue
  persists the eligibility deadline, exposes each message's delivery count,
  and reclaims expired messages before bounded-depth capacity checks.
- Queue recovery tests inject both a torn tail and a CRC-corrupted final
  record, confirming recovery retains the valid durable prefix and truncates
  only the invalid tail.
- Native `QUEUE_STATS` returns depth and in-flight delivery counts for a named
  queue. Per-connection prefetch and explicit consumer cancellation are
  available; cancellation requeues the connection's outstanding deliveries
  immediately.
- Dead-letter queues: a declaration may name a dead-letter queue and an
  optional delivery limit. Rejected messages (`NACK requeue=false`), expired
  messages, and messages whose next delivery would exceed the limit are routed
  there. The routed copy is durable before the source removal record, so a
  crash between them re-routes rather than drops; expiry-routed copies lose
  their expiry to prevent dead-letter ping-pong; a full dead-letter queue
  fails closed and keeps the message. `STATS` reports `queue_deadlettered`.
- Failure tests added: SIGKILL during publish (five rounds of pipe-confirmed
  publishes; every confirmed ID survives recovery), SIGKILL between delivery
  and ACK (redelivery with count 2 after restart), disk exhaustion via
  `RLIMIT_FSIZE` (durable mutations fail closed, non-durable queues keep
  working, the valid prefix survives), and 4x4 concurrent publishers/consumers
  with full coverage and drain verification.
- Persistent consumer registration/cancellation remains future work; the
  per-connection prefetch and cancellation cover the MVP contract.
- Fixed a concurrent ACK-loss defect found during the production stress gate:
  removing a queue tail with one older in-flight message left the tail pointer
  null, so a subsequent publish overwrote the head and orphaned that delivery.
  Tail recomputation now retains the remaining node, and a focused regression
  test verifies its ACK remains valid across the publish. Queue visibility
  leases now use a monotonic clock, while persisted retry and expiry times
  remain wall-clock based for restart correctness.

- Durable named consumers close the persistent-registration gap noted when
  the queue MVP completed: `0x29`/`0x2A`/`queue_consumer_register`/
  `queue_consumer_unregister` plus `0x2B`/`queue_consume_as` deliver under a
  stable owner token (new CRC-checked WAL record types `LOG_CONSUMER`/
  `LOG_CONSUMER_DEL`, replayed with fail-closed shape validation). A dropped
  connection's in-flight deliveries follow their visibility deadlines
  instead of being requeued immediately, reconnecting with the same name
  keeps ownership and prefetch accounting, graceful unregister requeues
  immediately, and registration survives restart. Delivery tags stay
  one-use per process and are documented as such. `STATS` exposes
  `queue_consumers`; capability bit 9 advertises the feature. Covered by
  core tests (owner stability across restart, durable registry replay,
  requeue-on-unregister) and wire tests (prefetch under the consumer token,
  visibility-held redelivery after disconnect, crash-restart registry
  recovery), including under ASan/UBSan and TSan.

Exit gate: at-least-once semantics are precise, verified, bounded, and never
acknowledge successful durable work after a persistence failure.  Result:
passed on macOS/ARM64 — `queue_test`, `queue_failure_test`, `queue_crash_test`,
and the queue protocol/recovery suite all pass, including DLQ routing,
fail-closed dead-lettering, and restart recovery.

## Milestone 3 — exchanges and routing

Status: **complete**.

- Added a routing layer above the queue engine in `src/queue.c`: direct,
  fanout, and topic exchanges plus the unnamed default exchange that routes
  by queue name. Topic patterns use `*` (exactly one word) and `#` (zero or
  more words) with a bounded dynamic-programming matcher (no catastrophic
  backtracking, at most 128 pattern words; longer patterns are refused at
  bind time).
- Bindings require an existing queue, are idempotent, and are capped at 1024
  per exchange, which bounds the fanout of a single publish. A queue bound
  through several bindings still receives one copy per publish.
- Routing is fail-closed: all targets are expiry-reaped and capacity-checked
  before anything is appended; a full target queue or a persistence failure
  fails the whole publish with no confirmation. Durable copies are appended
  per target and the queue WAL is fsynced once before the publisher confirm.
- Unroutable publishes report `routed = 0` (MISS), increment the
  `exchange_unroutable` stat, and may route once into a declared alternate
  exchange; AE chains are capped at one hop so cycles cannot loop, and a
  missing alternate degrades to unroutable.
- Durable exchange declarations, bindings, and unbinds are CRC-checked queue
  WAL records recovered across restart; non-durable exchange state is
  in-memory only. The queue WAL replay validates the new record shapes and
  truncates a corrupt tail exactly as before.
- Native protocol opcodes `0x30`–`0x33`, `STATS` counters (`exchanges`,
  `exchange_bindings`, `exchange_unroutable`), and Python client methods
  (`exchange_declare`, `exchange_bind`, `exchange_unbind`, `exchange_publish`)
  are documented in [EXCHANGES.md](../messaging/EXCHANGES.md).

Exit gate: routing rules and error paths are covered by integration tests and
cannot create unbounded fanout or response buffering. Result: passed on
macOS/ARM64 — `exchange_test` (patterns, routing, unroutable/AE, capacity,
binding cap, WAL replay and torn-tail recovery, in-memory durability policy)
passes, including under ASan/UBSan; `test_exchange_protocol.py` covers the
wire protocol, malformed-input rejection, restart recovery of durable
exchange state, and redelivery, and also passes against the sanitized server
binary. `make -B test` passes end to end with all client smoke tests.

Exchange routing benchmark (`src/bench_exchange.py`, durable queues, single
Python client, 100-byte values): plain durable queue publish 22,100 ops/s;
direct exchange with one durable binding 23,283 ops/s (routing overhead
within noise of the baseline); fanout to eight durable bindings 11,540 ops/s
(≈92k durable copies/s); topic exchange with 100 bindings 23,695 ops/s with
one match and 65,818 ops/s unroutable. The full cache benchmark remains the
release gate: 4-client 3,267,066 ops/s versus the recorded baseline
3,324,358 ops/s (−1.7%, within the short-run noise the baseline note
describes; a 2-client rerun sampled 2,527,550 ops/s against the recorded
3,273,430, so short runs vary by more than the 10% gate and the multi-client
sample is the recorded comparison).

## Milestone 4 — atomic cache plus messaging

Status: **complete**.

- Added `PUT_AND_PUBLISH` (`0x40`), `PUT_AND_ENQUEUE` (`0x41`), and
  `DELETE_AND_PUBLISH` (`0x42`) with a durable, exposed commit id per
  operation. The cache WAL transaction record (`0x08`) is the commit marker:
  the queue WAL first fsyncs a `TX_PREPARE` (transaction id, pre-allocated
  per-target message ids, payload), then the cache record is flushed and
  fsynced, then a queue `TX_COMMIT` record materializes the reserved
  messages. The store lock is held across the window so the resolved target
  set and capacity checks stay authoritative.
- Recovery is correct at every commit boundary: prepare without the marker is
  discarded (neither side); marker without `TX_COMMIT` is finished by startup
  reconciliation, which materializes the messages and writes the missing
  commit record; both records replay directly, idempotently. WAL folding
  defers while a transaction is inside its commit window so a marker cannot
  be folded away before the queue side is self-contained.
- Atomic operations are refused (not degraded) when cache persistence or the
  queue WAL is disabled or any target queue is non-durable. Unroutable
  publishes commit nothing. Cache mutations are applied in memory only after
  the marker is durable, so a failed commit leaves memory untouched.
- DELETE_AND_PUBLISH is an idempotent delete (missing keys still emit the
  event); the published message may be the cache value (`0x40`/`0x41`) or an
  explicit payload (`0x42`).
- Python client: `put_and_publish`, `put_and_enqueue`, `delete_and_publish`.

- Added `UPDATE_AND_EMIT` (`0x43`, protocol v1.4, capability bit 10), the
  conditional atomic operation: it commits only when the cache key already
  exists, answers MISS and writes nothing otherwise, and publishes the same
  bytes it stores (optional TTL). Its commit marker carries new conditional
  sub-operations (`8`/`9`); replay applies a conditional marker only when the
  key exists at that replay point, and a crafted marker for a missing key
  stays uncommitted so reconciliation discards the prepared event — recovery
  keeps both sides or neither. Covered by live hit/miss tests, crafted
  marker boundaries (existing key, missing key, idempotent second restart),
  a dedicated live SIGKILL loop (3,877–4,418 acknowledged updates all fully
  present in recorded runs), and the whole suite under ASan/UBSan; the
  protocol fuzzer now also targets `0x40`–`0x43` envelopes.

Exit gate: recovery shows both the cache mutation and message or neither,
with no event describing a cache state that was not committed. Result: passed
on macOS/ARM64. `test_atomic.c` covers the queue-side transaction machinery
(materialization, discard, reconciliation, idempotency, torn tails) including
under ASan/UBSan; `test_atomic_protocol.py` crafts both WALs at each commit
boundary against the real binary (prepare-only, marker-only, both records,
torn marker, corrupt marker, fanout recovery) and runs a live SIGKILL loop —
in the recorded runs 4,000–5,300 acknowledged transactions were all fully
present after restart, and every recovered message had its committed cache
counterpart. The suite also passes against the sanitized server binary. A
CRC-ordering bug in the first transaction-record writer was caught by exactly
this live test and fixed.

## Milestone 5 — anti-cache-stampede

Status: **complete**.

- Added native single-flight with opcodes `0x50`–`0x53` and a Python client
  API (`get_or_claim`, `wait_for_key`, `put_and_release`, `release_claim`,
  and the `get_or_load` cache-aside helper from the product spec). Exactly
  one concurrent requester claims a missing key; the rest wait server-side
  and receive the loaded value (or a negative answer) without re-loading.
- Claims are leased (≤ 60 s): a crashed loader cannot strand a key, and a
  restart clears all ephemeral coordination state. Wait deadlines are
  enforced by a server-side sweep; waits never block an event loop — a
  completed load pushes wake events to each waiter's own loop through a new
  non-stopping `platform_loop_notify` channel, and responses are built on the
  waiter's loop. Limits: one pending wait per connection, ≤ 256 waiters per
  key, waiter total bounded by the connection limit, lease/timeout caps of
  60 s.
- Negative caching: `PUT_AND_RELEASE(negative=1)` records a bounded,
  TTL-expiring negative answer that is invisible to plain `GET`.
- Any successful `PUT` of a key wakes its waiters, so plain writers
  participate. `STATS` exposes `claims`, `singleflight_waiters`, and
  `negatives`.
- Stale-while-revalidate and refresh-ahead are now native: `PUT_SWR` (`0x0b`,
  protocol v1.5, capability bit 11) stores a value exactly like `PUT_TTL` and
  additionally retains a bounded in-memory stale copy until `ttl + stale`;
  `GET_OR_REFRESH` (`0x54`) answers fresh hits immediately, serves expired
  keys from the retained copy (state `stale`, no waiting) while the
  `holder=1` caller revalidates, and flags refresh-ahead-due values (state
  `refresh`). Plain `GET` semantics are untouched: expired keys still miss,
  the stale copy is never visible to plain reads, a successful plain
  put/delete supersedes it, and a restart clears the registry — the same
  bounded, memory-only coordination category as claims and negatives
  (4,096 entries / 32 MiB, 7-day window cap, full registry degrades to
  claim/wait). Python client: `put_swr`, `get_or_refresh` (with a `holder`
  flag), and the `get_or_load_swr` cache-aside helper. `STATS` exposes
  `stale_entries`, `stale_serves`, `refresh_serves`; the metrics endpoint
  mirrors them. Covered by wire tests (fresh/stale/refresh/holder, restart
  clearing, fallback, helper loader-once, negative answers, refusal shapes)
  and the TSan server suite.

Exit gate: crashed owners cannot strand a key, and waits never block an event
loop or bypass configured timeout/backpressure limits. Result: passed on
macOS/ARM64. `test_stampede_protocol.py` verifies: one claim among 8 racers;
a crashed loader's lease expiring so the key is claimable again; `get_or_load`
calling the expensive loader exactly once across 10 concurrent threads;
negative answers cached, invisible to `GET`, and expiring; server-side wait
timeouts; release-without-value waking waiters into the `released` state;
protocol refusals (lease/timeout caps, payload shapes, duplicate wait per
connection); and claims cleared by a restart. The full suite passes after the
supporting event-loop fix below.

Supporting fix: a long-latent event-loop hazard surfaced by this milestone —
when a closing connection and a new accept shared one kqueue batch, the stale
event for the old connection could close the new connection's reused fd.
Connection events are now processed before listener accepts within a batch,
and a bare hangup no longer skips a readable event.

## Milestone 6 — partitioned streams

Status: **in progress**.

- Implement topics, bounded partition counts, keyed selection, append-only
  segments, offsets, batch append/fetch, replay, consumer groups, offset
  commits, retention, and lag metrics.
- Add segment rolling/cleanup and interruption-safe recovery.  Log compaction
  follows only after the basic retention model is proven.

- Added a separate CRC-checked stream WAL, durable immutable topic
  declarations, 1–256 partitions, deterministic keyed partition selection,
  monotonic per-partition offsets, replay fetches, bounded fetches, retention
  by age and retained bytes, and durable per-group offset commits. The native
  Python client and `STREAMS.md` document the `0x60`–`0x64` protocol.
- `stream_test` covers ordering, torn-tail recovery, committed offsets, byte
  retention, and WAL compaction; `test_stream_protocol.py` covers the live
  wire API and restart recovery. These are a working vertical slice, not a
  full Kafka replacement.
- Added crash-safe WAL checkpoint compaction: the compactor serializes only
  retained records and the latest group offsets to a synced temporary WAL,
  then atomically renames it and fsyncs the parent directory. A test forces
  multi-megabyte turnover, confirms the compacted file stays bounded, then
  verifies retained offsets and commits after recovery.
- Added lease-based native consumer groups. Group joins/heartbeats return a
  deterministic round-robin partition assignment, disconnects remove members
  immediately, expired leases are reaped, and offset commits are refused for
  members that do not own the partition. Core and protocol tests cover join,
  rebalance, disconnect, and assignment enforcement.
- Removed the accidental O(N) stream work (measured, `BENCHMARKS.md` stream
  table): compaction eligibility now uses an incrementally maintained
  live-checkpoint byte estimate instead of walking every retained record on
  each durable mutation, metrics report retained-record counters instead of
  traversing partitions under the lock, and retention persists one coalesced
  trim boundary per affected partition instead of one fsynced trim record per
  evicted record. Median durable append at 100k retained records went from
  3,815 to 46,911 ops/s and no longer degrades with history; commits went
  from 3,658 to 47,750 ops/s; the metrics scrape dropped from ~488 µs to
  sub-microsecond. WAL format, recovery, and acknowledgement points are
  unchanged. `stream_test` cross-checks the incremental accounting against a
  full traversal recount after appends, batches, trims, group changes, and
  recovery, and covers durable age-based retention (which also fixed a latent
  stack-buffer overflow in the age-trim WAL logging path).
- Added stream group fsync (measured, `BENCHMARKS.md` group-commit table):
  durable stream operations write their record, apply the in-memory mutation
  under the store lock, and wait with the lock released until an fsync covers
  their sequence; one syncer at a time fsyncs the high-water mark outside the
  lock, so one fsync completes every writer that arrived during the previous
  round. Because the WAL is a single sequential log, a record's durability
  implies all earlier records', so acknowledgement points are unchanged while
  fsync count per record drops with concurrency: 8-thread durable append went
  from 44,230 to 76,958 ops/s (median of five runs) and single-thread latency
  is unchanged. Write or fsync failure sets the store failed and wakes all
  waiters with errors (fail closed); compaction drains an in-flight fsync
  before rewriting and publishes the synced sequence for everything it
  checkpoints. `stream_test` gained an 8-thread shared-store concurrency test
  (per-partition offset contiguity, recount after reopen) and the server-level
  TSan protocol suites pass.
- Added queue group fsync (measured, `BENCHMARKS.md` queue group-commit
  table): publish, delivery, and declaration records are written and applied
  under the store lock, then their callers wait with the lock released until
  an fsync covers their sequence, while ACK, NACK/requeue, and dead-letter
  routing keep the strict fsync-before-mutate ordering inside the lock. The
  shared-queue publish+consume+ACK pipeline went from 20,998 to 39,589
  cycles/s (median of five runs); single-thread latency is unchanged and
  acknowledgement points are unchanged. The 8-thread per-queue publish case
  is now store-mutex-bound rather than fsync-bound, which moves per-queue
  lock scope up the priority list. Queue failure, crash, exchange, atomic,
  and fuzz suites pass; queue TSan/ASan and the server-level TSan protocol
  suites are clean.
- Added native batch operations behind capability bits 12/13 (protocol
  1.6): queue publish batch (`0x2D`), consume batch (`0x2E`), ACK/NACK batch
  (`0x2F`), and stream offset commit batch (`0x6B`), each bounded at 256
  entries and sharing one group fsync per batch. Measured (BENCHMARKS.md):
  durable publish batches of 256 run at 316k msgs/s versus 33k singles
  (9.5×), with the per-message durability contract unchanged (capacity is
  pre-checked for the whole publish batch; ACK removal still follows its own
  record's durability; dead-letter routing stays fail closed). The Python
  client and protocol suites cover every batch op including crash restart;
  existing opcodes and clients are unaffected.
- Removed the measured queue linear scans (BENCHMARKS.md queue index table):
  messages are doubly linked with O(1) removal (the old removal recomputed
  the tail by walking the queue), delivery tags resolve through a per-queue
  intrusive hash index proportional to in-flight count, and the
  retention/visibility pass now runs only when a TTL message exists or the
  earliest in-flight visibility deadline is actually due. Durable publish no
  longer degrades with depth (was 2.2× from 5k to 20k; now flat at ~49k
  ops/s), NACK requeue at depth went from 31,158 to 50,826 ops/s, and single
  durable publishes at depth went from 33,263 to 49,702 ops/s. An A/B run on
  the same machine state shows the retention skip alone lifts the shared
  publish/consume/ACK pipeline by 1.36×. Queue TSan/ASan and the full test
  matrix pass. Remaining linear path: consume behind a large in-flight wall
  (ready-pointer work), message-ID lookups during replay (bounded by live
  depth), and owner-indexed disconnect cleanup.
- Completed the mandatory Phase -1 synchronization audit: message state is
  guarded by the per-queue lock (metadata only for declarations and name
  lookups), every `q_wait_durable_locked` caller enters holding `wal_lock`
  and no other lock, and checked mutex operations abort on error in test
  builds (the ERRORCHECK mutexes had been silently swallowing recursive
  acquisitions). Dead-letter routing resolves its metadata with trylock and
  the DLQ lock with trylock; fanout locks targets in creation order;
  checkpoint, reap, requeue, and stats walk metadata plus per-queue locks in
  the documented order. Deterministic concurrency stress runs at 2+ threads
  verify independent queues mutate in parallel and grouped-fsync waiters
  cannot be lost. TSan, ASan, and the full suite pass; the earlier
  "attempted and reverted" note described a revert that the audit has now
  superseded with a correct implementation.
- Verified the checkpoint under the live server (protocol suite): ~2.9 MB of
  drained queue history plus 10 retained messages compacted to 873 bytes
  during one maintenance pass, and a SIGKILL restart recovered every live
  message. This closes the queue half of the recovery-scaling gate; the
  stream engine's existing checkpoint compaction already provides the same
  bound for streams.
- Added the queue WAL checkpoint (crash-safe, format-compatible): when the
  log outgrows twice the live state plus 1 MiB, maintenance rewrites the
  full live state using only existing record types, fsyncs and atomically
  renames the new log, and continues appending. Verified in `queue_test`
  with 12k drained triples plus mixed live state (retained messages,
  delivery counts, delayed retry, exchange, consumer): the WAL compacted
  from 2.4 MB to ~17 KB and every element survived a clean restart. The
  measured recovery cost of a 20k-triple history (64 ms) now stops growing
  once checkpoints fire, meeting the recovery-scaling gate for queues
  ahead of the Phase 6 server integration work.
- Added the consume ready-hint (BENCHMARKS.md consume-scan-hint table):
  consume scans start at the first message that is not known in-flight, so
  deliveries stop re-walking the growing in-flight wall. Durable consume
  went from 11,826 to 31,798 ops/s clean and from 10,155 to 21,274 ops/s
  behind a 10k in-flight wall; delayed retries are preserved (a delayed
  message is never jumped, and the protocol suite covers the
  delay-expires-after-later-deliveries case). Two candidates were measured
  and rejected on evidence: an owner index for disconnect cleanup (27-59 µs
  once per connection across 8 queues × 8k depth) and a replay message-ID
  index (recovery is linear at 64 ms per 20k publish+deliver+ACK triples;
  the historical-WAL growth fix is the Phase 6 checkpoint).
- Split the queue store lock into three domains (metadata, per-queue, WAL)
  with a single documented order (`ARCHITECTURE.md`): independent queues now
  mutate in parallel, record writes serialize only on the WAL lock with
  sequence order equal to byte order, dead-letter routing uses trylock
  against the DLQ (busy = keep and retry, matching the documented failure
  behavior), fanout locks all targets in creation order before the
  all-targets capacity check, and aggregate counters became C11 atomics.
  This removes the structural ceiling where every queue blocked on one lock
  during another queue's WAL write. Shared-queue pipelines gained ~12%
  (27.4k -> 30.7k cycles/s median); other concurrent shapes are within
  run-to-run noise because fsync rounds and wakeup latency now dominate.
  Thread sanitizer is clean on the queue, failure, crash, and exchange
  suites; the full C/Python matrix passes. The remaining known
  serialization is the atomic transaction protocol, which intentionally
  holds the metadata lock across prepare/cache-marker/commit.
- Remaining release blockers: durable segment storage, cooperative rebalance
  notifications/draining, fault injection for stream disk-full/corruption,
  and retention-cleanup interruption tests.

- Hardened replay fetches with a byte budget in addition to the 1,024-record
  cap. The stream engine stops before allocation when the response would
  exceed the configured server batch limit; an oversized first record is
  explicitly refused rather than silently producing unbounded memory use.
- Added stream observability: `STREAM_GROUP_LAG` reports per-partition
  consumer lag, while `STATS` exposes the durable group count and current
  live membership count. Both are covered in core and protocol tests.
- Added native `STREAM_APPEND_BATCH` (`0x67`): up to 1,024 keyed records are
  prevalidated, emitted under one fsynced CRC-checked WAL record, linked into
  memory only after that durable point, and acknowledged together. Core and
  wire tests cover offsets and restart recovery.
- Cooperative rebalance notifications/draining: every membership change
  (join, graceful leave, disconnect, lease expiry) advances a group
  generation that the `0x65` join/heartbeat response reports after the
  assignment (capability bit 8, protocol v1.3); heartbeats do not move it.
  New `0x68`/`stream_group_leave` releases one membership immediately so a
  drained consumer's partitions reassign without lease delay, and commits
  from members that lost a partition remain refused. Core tests assert the
  generation transitions and cross-group leave isolation; wire tests assert
  generation movement on join/disconnect/leave, drain-then-leave, and the
  refusal of leaving an unjoined group.
- Stream fault injection: `stream_test` now covers RLIMIT_FSIZE disk
  exhaustion (fail-closed appends and declarations, `stream_persistence_failed`
  set, acknowledged prefix readable and recoverable, fresh fd resumes), a
  bit-flipped final WAL record (checksum rejection, prefix and offset
  continuity kept), and a torn record after retention trims (interruption
  during cleanup never loses the valid prefix). All pass under ASan/UBSan.
- Retention/cleanup interruption hygiene: startup now removes stale
  `.compact.*` siblings left by a crashed compactor (the pre-rename WAL was
  still valid), with a test forcing the litter and asserting both recovery
  and cleanup.

Exit gate: order holds within a partition, records and offsets recover after a
process crash, and consumption is documented and tested as at-least-once.
Status: the correctness exit gate passes; the remaining item, segment-file
storage, is a documented scalability optimization rather than a correctness
gap (see [STREAMS.md](../messaging/STREAMS.md)): the in-memory retained set plus a single
crash-safe WAL with interruption-safe checkpoint compaction already provides
the tested recovery contract, with `max_bytes` bounding retained memory and
capacity planning required for compaction.

- Profiled the macOS "8-client cliff" required by the performance gate
  (BENCHMARKS.md client-scaling table): the server scales linearly from 4 to
  8 client connections when measured with independent client processes
  (4.71M -> 9.75M ops/s, p99 under 850 µs), and the historical cliff is an
  artifact of the benchmark client's own in-process thread contention. No
  kqueue change is warranted on this evidence; the event-loop dispatch
  budget work stays available if a server-side limit appears under real
  multiprocess load.

## Performance program — consolidated status (2026-08-29)

All phases of the performance program are complete or resolved with
measurements (`BENCHMARKS.md` holds the tables):

1. Baselines: engine-level stream and queue harnesses (`make bench-stream`,
   `make bench-queue`), server matrix with multiprocess clients.
2. Stream O(N) removal: incremental live-checkpoint accounting, coalesced
   retention, incremental metrics — append flat across 10x history
   (12.3x at 100k retained records).
3. Group fsync on both engines: stream 1.74x at 8 threads; queue shared
   pipeline 1.89x; low-load latency unchanged; acknowledgement points
   unchanged.
4. Batch protocol operations (0x2D/0x2E/0x2F/0x6B, capability bits 12/13,
   protocol 1.6): durable publish batches 9.5x.
5. Queue indexes: delivery-tag hash, O(1) removal, ready hint, retention
   skip — publish/consume/ACK/NACK cost no longer grows with depth;
   owner-index and replay-index candidates measured and rejected.
6. Queue WAL checkpoint: crash-safe, format-compatible, verified under the
   live server with SIGKILL restart (2.9 MB -> 873 bytes, all live messages
   recovered); recovery now scales with live state + tail.
7. Client-scaling cliff: profiled; the server scales linearly to 8 clients
   (10.7M ops/s, p99 462 us) with independent client processes — the
   historical cliff was a benchmark-client artifact; the acceptance gate is
   met. No kqueue change was warranted.
8. Cache micro-optimizations: precondition (a profiled server bottleneck)
   not met; correctly skipped.

Resource gates: binary 185 KB; no threads added beyond the existing
maintenance thread; engine idle allocations ~131 KB (STATS); all buffers,
batches, and indexes bounded and proportional to live state; WAL sizes
bounded by checkpoints on both engines; idle RSS is not directly measurable
in this sandbox (`ps` denied) — the recorded Linux matrix shows 6.4 MB idle,
within the 10 MiB gate. Remaining documented limitations: stream segment
files (deeper than checkpoint compaction), cooperative rebalance
notifications, the atomic-transaction metadata-lock window, and Windows
native build.

## Milestone 7 — deployment and Windows delivery

Status: **in progress**.

- Added a multi-stage, non-root Linux Docker image, local Compose deployment,
  an ephemeral cache Deployment, and a durable single-node StatefulSet. The
  durable path uses a private copied auth secret, `always` durability, a PVC,
  explicit resource limits, and TCP lifecycle probes. It documents that a
  single persistent volume is not replication or node-loss protection.
- Added an authenticated `HEALTH` command and `kuttidb-cli health`. It reports
  ready only when cache, queue, and stream persistence remain writable; the
  deployment guide distinguishes this durability-aware check from a lightweight
  TCP listener probe.
- Added a Prometheus scrape endpoint: `--metrics-bind IPv4:PORT` enables a
  bounded, single-thread admin listener serving `/metrics` (exposition format
  v0.0.4, all STATS families), `/ready` (503 under the same fail-closed
  durability contract as `HEALTH`), and `/live`. Non-loopback binds require
  `--metrics-token-file` (0600, owner-only, symlink-safe) and answer 401
  without a constant-time-compared `Authorization: Bearer` header; request
  size and socket timeouts are bounded and one request is served per
  connection. Covered by the security suite (policy refusal, 401/404 paths,
  header parsing, oversized-request drop, metric families) and documented in
  [DEPLOYMENT.md](../operations/DEPLOYMENT.md).
- Wired authenticated probes into deployment: the durable StatefulSet copies a
  private metrics token from its secret, binds metrics on `0.0.0.0:9099`, and
  uses HTTP `/ready` and `/live` probes carrying the bearer header; the Compose
  example adds a loopback-only scrape. The endpoint also exposes
  `kuttidb_ready` so Prometheus can alert on persistence degradation.
- Protocol v1.2 adds authenticated capability negotiation. Clients provide a
  supported major/minor version and receive a stable feature bitset; a major
  mismatch returns MISS before a feature-specific command is attempted.
- Finish Windows service, mapping, locking, ACL, named-pipe, DLL, and C# work.
- Validate and publish multi-architecture images, then add a Helm chart if the
  simple manifests no longer cover deployment needs. A CI job now builds the
  runtime image for linux/amd64 and linux/arm64 via QEMU and smoke-tests the
  arm64 variant; image publication to a registry remains release work.
- Persistent-volume recovery in a container is now validated and enforced by
  CI (see the Milestone 8 entry); remaining Windows delivery work is
  (service, mapping, locking, ACLs, named pipes, DLL, C# client).
  Prometheus metrics and authenticated probes are now shipped; remaining
  observability work is per-queue/per-topic labeled metrics.
- Windows remains the one documented platform blocker, with its exact scope
  recorded in [ARCHITECTURE.md](../design/ARCHITECTURE.md): no Windows server build
  (IOCP event backend), no Windows shared-memory mapping or named pipes, and
  no DLL/C# client until the remaining POSIX-specific file locks, clocks, and
  process-liveness boundaries are abstracted.

Exit gate: images and manifests start, recover with persistent storage, honor
security contexts/resource limits, and clearly state single-node limitations.

## Milestone 8 — compatibility and release readiness

Status: **in progress**.

- Evaluate Redis/AMQP/Kafka adapters only after native semantics are stable.
- Publish reproducible macOS/Linux/Windows benchmarks, resource ceilings,
  recovery measurements, and upgrade/backup guidance.
- Run ASan, UBSan, ThreadSanitizer where supported, fuzz protocol/WAL parsers,
  and complete security/failure testing.

- Added WAL-reader fuzzing (`src/test_fuzz.c`, `make fuzz_test`): 2,000
  deterministic mutations per engine (byte flips, random truncations, swaps)
  of valid stream and queue WALs, with postcondition checks — a successful
  recovery open must yield contiguous partition records with a correct
  continuation offset, and a queue open must either accept the durable
  declaration or be in the documented fail-closed state. Passes under
  ASan/UBSan via `make sanitize-fuzz`.
- Added live protocol fuzzing (`src/test_protocol_fuzz.py`): every opcode with
  malformed envelopes, oversized length claims, truncated frames, pipelined
  garbage, and random byte storms; the server must keep serving well-formed
  clients with no leaked connections afterward. Also passes against the
  sanitized server binary.
- Benchmark refresh recorded in the baseline section: 4-client 3,462,964
  ops/s with p50/p95/p99 against the 3,324,358 ops/s baseline — no regression
  over the 10% gate.
- Remaining observability work closed: per-queue labeled metrics
  (`kuttidb_queue_depth{name}`, `kuttidb_queue_inflight{name}`, via a new
  `queue_foreach_stats` store-lock snapshot) and per-topic series
  (`kuttidb_topic_partitions`, `kuttidb_topic_retained_bytes`,
  `kuttidb_topic_records`, via `stream_foreach_stats`), bounded at 256
  labeled objects per engine with explicit `*_truncated` gauges and
  ASCII-safe label escaping; covered by the security suite.
- Reran ThreadSanitizer on the current tree through new `make
  sanitize-tsan-queue` (the 4x4 concurrent publisher/consumer suite) and
  `make sanitize-tsan-server` (queue, single-flight, and stream protocol
  suites against a TSan server): no race reports.
- Added backup/restore and upgrade/compatibility guidance to
  [DEPLOYMENT.md](../operations/DEPLOYMENT.md): cold backup of the one data directory,
  idempotent restore through tested WAL recovery, explicit live-copy and
  snapshot caveats, and the protocol/format versioning upgrade policy.
- Compatibility-adapter evaluation recorded in
  [ARCHITECTURE.md](../design/ARCHITECTURE.md): Redis RESP and AMQP gateways are
  deferred with written rationale (semantic subsets breed broken
  integrations; the native API already works in five client languages), and
  Kafka wire compatibility is ruled out for the core binary in favor of
  native streams plus documented bridges. The documented claim stays
  "native protocol v1.3, no Redis/AMQP/Kafka wire compatibility."
- Added [BENCHMARKS.md](../operations/BENCHMARKS.md) (methodology, recorded results, and
  the honest gaps: no isolated PUT/GET/DELETE latency series yet, no
  Linux/Windows-native performance tables) and [MIGRATION.md](../guides/MIGRATION.md)
  (when to use Redis/RabbitMQ/Kafka/SQLite instead, workload migration maps,
  and the upgrade policy), both linked from the README.
- CI now runs a dedicated safety job: ASan/UBSan core, stream, WAL-fuzz, and
  live-server suites plus both ThreadSanitizer targets, so the sanitizer and
  fuzz evidence is collected on every push, not only locally.
- Validated persistent-volume recovery in a container, closing the M7
  blocker: `src/test_container_recovery.py` seeds durable cache/queue/stream
  state into a Docker volume, SIGKILLs the container, restarts on the same
  volume, and verifies every acknowledged durable item survived — cache
  500/500, queue 50/50, streams 20/20 against the freshly built runtime
  image. CI now runs it as the `container-recovery` job. The test also
  re-confirmed the deployment pattern: both the client listener and the
  metrics listener require their token files on a non-loopback bind.
- Closed the isolated-latency gap in [BENCHMARKS.md](../operations/BENCHMARKS.md):
  `kuttidb-bench` gained a `single` mode (`make bench-single`) that measures PUT,
  GET, and DELETE as separate one-round-trip series with p50/p95/p99 —
  recorded at ~12 us p50 per op for one client and ~16–17 us for four.
- Added a Linux-native performance table to [BENCHMARKS.md](../operations/BENCHMARKS.md)
  (Alpine/musl/epoll in a container on the same ARM64 host, caveats
  documented): 7.2M ops/s at 4 clients, 8.4M ops/s at 8 clients, with the
  epoll backend avoiding the 8-client p99 cliff the kqueue sample shows.
- Error reporting documented as an explicit condition-to-wire-signal map in
  PROTOCOL.md (MISS for misses/unroutable, fail-closed ERROR for
  full/durability/shape failures, connection close for auth, wait states for
  lease conflicts): every condition the product spec names is
  client-distinguishable without widening the three-status wire contract.
- Operational inspection closed: `kuttidb-cli queues|topics|groups` plus
  read-only inventory opcodes `0x2c`/`0x69`/`0x6a` (bounded 256-entry
  snapshots under the owning engine's lock, documented in PROTOCOL.md) give
  CLI visibility into queue depth/in-flight, topic partitions/records/bytes,
  and consumer-group generations/membership.
- Client coverage completed to the practical set the product spec lists:
  a zero-dependency Node.js client (`clients/nodejs`, full protocol surface
  including queues, exchanges, atomic operations, single-flight/SWR, and
  streams) wired into `make test` with the other client smokes, and runnable
  Python/Node example walkthroughs (`examples/`) covering every documented
  usage pattern. Remaining optional: Swift ("where practical") and C#/
  Node-per-language packages beyond the shared Node client.
- Docker/Kubernetes delivery documented end to end: DOCKER.md (image layout,
  flags, Compose, healthcheck semantics, recovery verification) and
  KUBERNETES.md (manifests, authenticated probes, secret staging, graceful
  termination, single-node limits); the image now ships a TCP HEALTHCHECK,
  verified in a live container. Registry publication remains release work
  (needs credentials); Helm remains optional — the clean manifests satisfy
  the deployment gate.
- Remaining: Windows benchmark evidence and release packaging.

Exit gate: documentation makes no unsupported exactly-once, no-data-loss, or
Kafka/AMQP/Redis-compatibility claims; all supported-platform build, safety,
operational, and deployment gates pass.
