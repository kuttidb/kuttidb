# Benchmarks

All numbers come from the recorded environments below and are reproducible
with the commands shown. Short runs are noisy; the comparisons called out as
gates compare like-for-like runs on the same machine, and a regression greater
than 10% is treated as release-blocking unless a measured justification
exists. Numbers are evidence, not marketing: they describe the recorded runs,
not a guarantee on other hardware.

## Methodology

- `make bench-matrix` (or `make bench-quick`) drives the C benchmark
  (`src/kuttidb_bench.c`) and a Python matrix (`src/bench_matrix.py`) against a live
  server: N clients × 256-item batches of 100-byte values, reporting ops/s,
  p50/p95/p99 batch latency, idle and loaded RSS, and live/allocated bytes.
- `make bench-exchange` measures durable queue/exchange routing overhead
  (`src/bench_exchange.py`, single Python client, 100-byte values).
- `make bench-stream` measures the stream engine directly through the
  `StreamStore` API (`src/bench_stream.c`): durable append ops/s and
  p50/p95/p99 as retained history grows 10k → 100k records (8 partitions,
  100-byte values), head/tail/lagging fetch, the metrics scrape, offset
  commits, one retention burst that evicts ~900 records in a single call, and
  full-WAL reopen recovery.
- `make bench-queue` measures the queue engine directly through the
  `QueueStore` API (`src/bench_queue.c`): durable publish as retained depth
  grows 5k → 20k (100-byte values), consume from a clean head and from behind
  a 10k-deep wall of in-flight deliveries, ACK as the outstanding set shrinks
  10k → 5k, NACK/requeue, one visibility-expiry pass requeueing 2,000
  deliveries, the metrics scrape, and publish+consume+ACK steady state for
  in-memory and durable queues.
- Queue and stream baselines must be added to this file before any major
  milestone that touches those engines is accepted.

## Cache baseline — 2026-08-28 (macOS 15.3, ARM64, Apple Clang 17)

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

The short runs are noisy (the baseline note documents two-client samples
varying by more than the 10% gate); the four-client sample is the recorded
release-gate comparison.

## Progress sample — 2026-08-29 (`make bench-quick`, same workload)

After the stream consumer generations, Prometheus endpoint, and fuzz suites:

| Clients | ops/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| 1 | 904,077 | 272 us | 454 us | 523 us |
| 2 | 2,598,685 | 204 us | 295 us | 314 us |
| 4 | 3,462,964 | 232 us | 528 us | 685 us |
| 8 | 1,869,718 | 390 us | 2,301 us | 2,415 us |

The four-client sample sits ~4% above the recorded baseline — no regression
over the gate.

## Linux-native sample — 2026-08-29 (`make bench-quick` equivalent in a container)

Alpine 3.21 (musl, epoll, 4 event loops), forced `TLS=0` rebuild, same
workload, run inside Docker Desktop on the same ARM64 machine — so the Linux
kernel is native to the VM but there is a hypervisor layer; treat the shape
(scaling, latency) as the evidence, not absolute numbers:

| Clients | ops/s | p50 | p95 | p99 | loaded RSS |
|---|---:|---:|---:|---:|---:|
| 1 | 2,374,817 | 94 us | 148 us | 167 us | 6,432 KiB |
| 2 | 4,050,647 | 117 us | 163 us | 174 us | 11,152 KiB |
| 4 | 7,218,664 | 129 us | 178 us | 249 us | 20,564 KiB |
| 8 | 8,371,456 | 182 us | 371 us | 494 us | 39,220 KiB |

The epoll backend scales to 8 clients without the p99 cliff the macOS kqueue
matrix shows at 8 clients; memory per live byte is comparable. This is
correctness-first evidence, not a release-grade cross-platform comparison.

## Isolated single-op latency — 2026-08-29 (`make bench-single`)

One round trip per operation, 100-byte values, periodic durability, each
series measured separately (the gap noted above, now closed):

| Clients | Op | ops/s | p50 | p95 | p99 |
|---:|---|---:|---:|---:|---:|
| 1 | PUT | 25,248 | 12 us | 22 us | 43 us |
| 1 | GET | 25,248 | 13 us | 17 us | 19 us |
| 1 | DELETE | 25,248 | 12 us | 16 us | 18 us |
| 4 | PUT | 76,949 | 17 us | 27 us | 33 us |
| 4 | GET | 76,949 | 16 us | 27 us | 33 us |
| 4 | DELETE | 76,949 | 16 us | 27 us | 35 us |

Single-op throughput is dominated by per-request round trips; batched
operations (the table above) remain the recommended path for bulk work.

## Queue and exchange routing — 2026-08-29 (`src/bench_exchange.py`)

Durable queues, single Python client, 100-byte values:

| Scenario | ops/s |
|---|---:|
| Plain durable queue publish | 22,100 |
| Direct exchange, one durable binding | 23,283 |
| Fanout, eight durable bindings | 11,540 (≈92k durable copies/s) |
| Topic exchange, 100 bindings, one match | 23,695 |
| Topic exchange, unroutable | 65,818 |

Routing overhead is within noise of the plain-queue baseline; a fanout
publish multiplies durable copies, so capacity planning must multiply by
binding count.

## Queue engine, depth scaling — 2026-08-29 (`make bench-queue`)

Engine-level baseline before the queue index/group-commit milestones
(macOS 15.3, ARM64, single run; the publish depth trend repeated across the
runs performed while building the harness):

| Measurement (100-byte values) | Result |
|---|---:|
| Durable publish at depth 5k | 34,824 ops/s (p50 28 µs) |
| Durable publish at depth 10k | 26,721 ops/s (p50 36 µs) |
| Durable publish at depth 15k | 20,240 ops/s (p50 49 µs) |
| Durable publish at depth 20k | 15,606 ops/s (p50 62 µs) |
| Durable consume, clean head | 12,303 ops/s (81 µs avg) |
| Durable consume behind 10k in-flight deliveries | 10,705 ops/s (93 µs avg) |
| ACK with 10k outstanding deliveries | 47,928 ops/s (20 µs avg) |
| ACK with 5k outstanding deliveries | 53,798 ops/s (18 µs avg) |
| NACK requeue, ~8k outstanding | 31,158 ops/s (32 µs avg) |
| Visibility-expiry pass requeueing 2,000 deliveries | < 1 ms |
| Metrics scrape at 20k depth | < 1 µs |
| Publish+consume+ACK steady state, in-memory | 6,133,088 cycles/s |
| Publish+consume+ACK steady state, durable | 17,725 cycles/s (56 µs avg) |

Two depth-linear costs are visible and match the known issue list: durable
publish falls ~2.2× as retained depth grows 5k → 20k, and ACK/consume carry a
small but measurable linear component against the outstanding in-flight set
(queue-wide linked-list scans). Durable steady state is fsync-bound at three
durable records per cycle. These rows are the comparison point for the
queue-index and group-commit milestones; the engine-level in-memory cycle at
6.1M cycles/s confirms the scan costs, not the data path, dominate at depth.

## Queue engine, indexes and retention skip — 2026-08-29 (after group commit)

After the group-fsync milestones, the publish depth degradation was traced to
the retention/visibility pass that walked the whole queue on every publish
and consume, plus the O(depth) tail recomputation on removal. Phase 4 added:
a doubly linked message list with O(1) removal, an intrusive delivery-tag
hash index (proportional to in-flight count), and a retention pass that runs
only when a TTL message exists or a visibility deadline is actually due.
Engine-level, five runs, medians:

| Measurement | Before Phase 4 | After Phase 4 | Δ |
|---|---:|---:|---:|
| Durable publish at depth 5k | ~50,000 ops/s | ~49,000 ops/s | unchanged |
| Durable publish at depth 20k | degraded with depth | ~49,000 ops/s | flat |
| Publish, 8,192 singles at 8k depth | 33,263 ops/s | 49,702 ops/s | 1.49× |
| NACK requeue, ~8k outstanding | 31,158 ops/s | 50,826 ops/s | 1.63× |
| ACK, 10k outstanding | 47,928 ops/s | ~50,000 ops/s | fsync-bound |
| 256-message publish batch | 316,123 msgs/s | ~312,000 msgs/s | unchanged |
| Shared pipeline, retention skip A/B | 20,200 cycles/s | 27,400 cycles/s | 1.36× |

The headline is the shape change: per-operation cost no longer grows with
queue depth (the Phase 1 gate "queue ACK, consume, and timeout cost must not
grow linearly with queue depth" is now met for publish, ACK, and NACK; the
remaining linear path is the ready-scan for consume behind a large in-flight
wall, which the deferred ready-pointer work addresses). The A/B row compares
the same build with the retention skip disabled and enabled, run
back-to-back. Memory cost: two extra pointers per message (16 bytes on
64-bit, ~2.6% for 100-byte values) and a 512-byte initial tag table per
queue that grows only with in-flight count.

## Queue engine, consume scan hint — 2026-08-29

The consume scan started at the queue head, so every delivery walked past
all previously delivered (in-flight) messages — the probe itself was O(N²)
as its own deliveries accumulated into a wall. The scan now starts at a
maintained ready hint (every message before it is in-flight; any requeue
resets the hint to the head, and a ready-but-delayed message is never
jumped). Engine-level, three runs, medians:

| Measurement | Before hint | After hint | Δ |
|---|---:|---:|---:|
| Durable consume, clean head | 11,826 ops/s | 31,798 ops/s | 2.7× |
| Durable consume behind 10k in-flight deliveries | 10,155 ops/s | 21,274 ops/s | 2.1× |

Two related candidates were measured and rejected on cost/benefit evidence:
an owner-to-in-flight index for disconnect cleanup (a disconnect requeue
across 8 queues × 8k depth already costs 27–59 µs once per connection) and a
message-ID index for replay (recovery of 20k publish+deliver+ACK triples
measures 64 ms and scales with the interleaving already — the real fix for
historical WAL growth is the Phase 6 queue checkpoint).

## Queue engine, group commit — 2026-08-29 (`make bench-queue`, CONC phases)

Concurrent durable work before/after the queue group-fsync coordinator
(macOS 15.3, ARM64, engine-level harness, five comparable runs each,
medians). The coordinator lets publish and delivery records join group
fsyncs with the lock released; ACK/NACK/dead-letter keep fsync-before-mutate
inside the lock exactly as before.

| Measurement | Per-record fsync | Group fsync | Δ |
|---|---:|---:|---:|
| 4 producers + 4 consumers, one shared durable queue (publish+consume+ACK cycles) | 20,998 cycles/s | 39,589 cycles/s | 1.89× |
| 8-thread durable publish to per-thread queues, aggregate | 33,534 ops/s | 35,431 ops/s | 1.06× |
| 1-thread publish+consume+ACK cycle (interleaved A/B) | 15,066 cycles/s | 16,335 cycles/s | ~unchanged |

The shared-queue pipeline nearly doubles because its three durable records
per cycle (publish, delivery, ACK) no longer serialize behind three separate
fsyncs. The per-thread-queue publish case is now mutex-bound rather than
fsync-bound — the fsync left the critical path but the single store lock
caps aggregate throughput; per-queue lock scope is the recorded next step.
Single-thread latency is unchanged (a sole writer fsyncs under the lock as
before). Acknowledgement points are unchanged: publish, delivery, and ACK
each return only after an fsync covers their record.

## Queue engine, batch operations — 2026-08-29 (`make bench-queue`, BATCH phase)

Durable publish of 8,192 × 100-byte messages: one message per operation
versus the 256-message batch publish (protocol `0x2D`), engine level, five
runs, medians:

| Measurement | Singles | 256-message batch | Δ |
|---|---:|---:|---:|
| Durable publish throughput | 33,263 msgs/s | 316,123 msgs/s | 9.5× |
| Per-message durable cost | 30 µs | 3.2 µs (811 µs per batch) | 9.5× |

The batch amortizes one lock hold and one group fsync across the whole
batch; the per-message durability contract is unchanged. Consume and
ACK/NACK batches (`0x2E`/`0x2F`) provide the same round-trip and fsync
amortization on the consumer side and are exercised end-to-end by the
protocol suites, including restart recovery of acknowledged batches.

## Stream engine, retained-history scaling — 2026-08-29 (`make bench-stream`)

Before/after removing the accidental O(N) work from the stream engine
(macOS 15.3, ARM64, Apple Clang 17, engine-level harness, five comparable
runs each, medians reported). The "before" engine recomputed compaction
eligibility by walking every retained record on every durable mutation,
counted metrics by traversal, and fsynced one trim record per evicted
record; the "after" engine maintains the live-checkpoint estimate and
retained-record counters incrementally and persists one coalesced trim
boundary per affected partition.

| Measurement (100k retained records, 8 partitions) | Before | After | Δ |
|---|---:|---:|---:|
| Durable append at 10k retained records | 24,648 ops/s | 46,658 ops/s | 1.9× |
| Durable append at 100k retained records | 3,815 ops/s | 46,911 ops/s | 12.3× |
| Append p50 / p99 at 100k records | 254 / 381 µs | 21 / 27 µs | 12× |
| Offset commit at 100k records | 3,658 ops/s | 47,750 ops/s | 13.1× |
| Metrics scrape (per-topic stats) | 488 µs | < 1 µs | O(N) → O(1) |
| Retention burst (evict ~900 records in one call) | 17 ms | < 1 ms | ≥ 17× |
| WAL reopen recovery, 13.6 MB WAL | 93 ms | 93 ms | unchanged |

The headline result is the shape change: append throughput used to fall 6.6×
as retained history grew 10× (25k → 3.8k ops/s); it is now flat within ~7%
across the same growth (per-operation cost no longer depends on retained
history). Reopen recovery is replay-bound and deliberately unchanged —
segmented storage (roadmap) is the follow-up that addresses it. WAL format
and recovery behavior are unchanged.

## Stream engine, group commit — 2026-08-29 (`make bench-stream`, CONC phase)

Concurrent durable appends (8 threads sharing one store across 8 partitions,
10k records each, 100-byte values) before/after the group-fsync coordinator.
Same engine and harness otherwise; five runs per side, medians:

| Measurement | Per-record fsync | Group fsync | Δ |
|---|---:|---:|---:|
| 8-thread durable append, aggregate | 44,230 ops/s | 76,958 ops/s | 1.74× |
| 8-thread per-op p50 / p99 (includes durability wait) | 20 / 41 µs | 93 / 236 µs | see note |
| 1-thread durable append at 100k history | 44,624 ops/s | 49,126 ops/s | ~unchanged |

The old per-op latency hid the lock queueing behind the fsync; the reported
old wall time per operation at 8 threads is visible in the aggregate rate
(8/44,230 ≈ 181 µs), so the group-commit per-op p50 of 93 µs is a completion
time improvement, not a tail regression. The single-thread case is unchanged:
with no other writers the operation fsyncs immediately, so low-load p99 is
not affected. One fsync now covers every writer that arrived during the
previous round, cutting fsync count per acknowledged record roughly 4× at
8 threads. Acknowledgement points are unchanged: an operation returns only
after an fsync covers its record.

## Queue WAL checkpoint — 2026-08-29 (server-level, SIGKILL restart)

Live-server verification of the queue checkpoint (protocol test suite):
60×256 published+drained 100-byte messages (~2.9 MB of WAL history) plus 10
retained live messages, one maintenance pass (~1 s cadence), then SIGKILL
and restart:

| Measurement | Result |
|---|---:|
| WAL size before checkpoint | ~2.9 MB (all drained history) |
| WAL size after maintenance checkpoint | 873 bytes (live state only) |
| SIGKILL restart recovery | all 10 live messages present, in order |
| Recovery-time scaling | bounded by live state + post-checkpoint tail |

The engine-level harness (`make bench-queue`, RECOVERY phase) recorded 15 ms
to replay a 15k-record history and 64 ms for 20k triples before checkpoints;
after the trigger fires, replay cost stops growing with drained history
entirely. No extra threads were added (the maintenance thread already
existed) and the on-disk format is unchanged — the checkpoint re-emits
existing record types only.

## Cache client-scaling cliff — 2026-08-29 (`src/bench_matrix.py --quick`, this machine)

Phase 7 profiling baseline, macOS 15.3 ARM64, 256-item batches of 100-byte
values, server defaults (4 event loops, periodic durability):

| Clients | ops/s | p50 µs | p95 µs | p99 µs |
|---|---:|---:|---:|---:|
| 1 | 1,960,631 | 148 | 217 | 253 |
| 2 | 3,305,567 | 183 | 253 | 334 |
| 4 | 3,266,479 | 184 | 631 | 1,017 |
| 8 | 1,969,493 | 426 | 2,278 | 2,567 |

The documented cliff reproduces with the threaded benchmark client — but
profiling isolates it to the **client process**, not the server. Running the
same server against N *separate one-thread client processes* (identical
protocol, identical batch size):

| Measurement | 4 clients | 8 clients | 8 vs 4 |
|---|---:|---:|---:|
| 8/4 threads in one `kuttidb-bench` process | 3,470,000 ops/s | 1,955,000 ops/s | −44% |
| N separate one-thread client processes (aggregate) | 4,706,000 ops/s | 9,753,000 ops/s | **+107%** |
| p99, separate-process clients | — | 483–848 µs | low |

With multiprocess clients adopted into `bench_matrix.py --quick` itself
(each client is now an independent one-thread process; rates summed, worst
tail reported), the full matrix reads:

| Clients (multiprocess matrix) | ops/s | p50 µs | p95 µs | p99 µs |
|---|---:|---:|---:|---:|
| 1 | 1,855,701 | 163 | 211 | 256 |
| 2 | 3,949,150 | 125 | 200 | 243 |
| 4 | 7,634,769 | 126 | 198 | 231 |
| 8 | 10,675,351 | 138 | 396 | 462 |

Eight clients now measure 40% above four clients with p99 of 462 µs — the
acceptance gate is met with the corrected instrument.

Conclusions, from measurements on this machine:

- The **server scales linearly** from 4 to 8 (and 16; 9.75M at 8 with no
  cliff): the kqueue event loops, dispatch path, and shard locks are not the
  bottleneck at these client counts.
- The historical "8-client cliff" is an artifact of the measurement client:
  8 benchmark threads inside one process contend on client-side process
  resources (allocator, syscalls) and distort the server measurement.
- Per the acceptance gate, 8-client throughput is above 4-client throughput
  when measured with independent client processes; the threaded-benchmark
  number must not be used as evidence of a server scaling defect.

No kqueue or event-loop change was made on the strength of this table alone;
the event-loop dispatch budget work remains available if a server-side
limitation appears with real multiprocess workloads.

## Known gaps in this file

- No Windows-native benchmark tables yet (Windows server build remains the
  documented platform blocker).
- Queue publish/consume baselines beyond the single exchange benchmark, a
  SIGKILL-recovery cost table for streams (the reopen row above covers clean
  restart only), and consumer-lag behavior under slow consumers are not yet
  recorded.
