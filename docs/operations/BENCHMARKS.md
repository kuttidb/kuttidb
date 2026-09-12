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

## Cross-server competitor comparison — 2026-09-12 (Ubuntu 26.04 VPS, 1 vCPU)

A like-for-like single-node comparison of KuttiDB against Redis, RabbitMQ,
NATS JetStream, and Apache Kafka, all run on the same machine, one server at a
time, each stopped before the next was started.

Environment: Ubuntu 26.04.1 LTS (kernel 7.0.0-30-generic), AMD EPYC 9354P with
**1 vCPU**, 3.8 GiB RAM, local SSD (`rotational=0`), 48 GiB disk. Every server
and every benchmark client ran under `nice -n 15` on loopback, no TLS, no
compression. The production site served by the same host (Caddy on 80/443) was
checked before and after every phase and stayed at 200 responses in 14–40 ms
throughout. KuttiDB was built from commit `0ce9a31` with gcc 15.2 `-O2`
(`tls=off`, telemetry off). Absolute numbers on a 1-vCPU shared vHost are
conservative; the comparisons are same-machine, same-day, like-for-like.

> **Methodology correction (same day).** The first recording of this
> comparison ran every KuttiDB harness with its data directory under `/tmp`,
> which Ubuntu 26.04 mounts as **tmpfs** (a RAM disk). Fsync on tmpfs costs
> almost nothing, so KuttiDB's single-record publish (17.8k/s), exchange
> routing (17.5k/s), stream append (634k/s) and queue rows (207k publish /
> 128k consume+ACK) were inflated relative to every competitor, whose data
> directories were on the real SSD. Every KuttiDB row in this section has
> been re-measured with its WAL on the same SSD the competitors used
> (`TMPDIR` redirected); the corrected numbers replace the old ones below.
> The resource-usage section was measured on the SSD from the start and
> needed no correction. Two interim hypotheses are also withdrawn: queue
> publish is **depth-flat** (86–108k/s SSD at 20k and 200k messages; the
> 207k/s first reading was tmpfs, not depth), and the earlier footnote
> blaming queue depth for the publish gap was wrong. Consume+ACK does
> degrade with depth (63.5k/s at 20k → 32k/s at 200k on SSD) and is recorded
> at both scales.

Methodology per class:

- **Cache/KV**: KuttiDB `src/bench_matrix.py --quick` (256-item batches of
  100-byte values, independent one-thread client processes, PUT/GET/DELETE
  mix, WAL on the SSD). Redis `redis-benchmark` (`-d 100 -t set,get`,
  pipeline length as shown, 4 connections, `--threads 2`). Redis durability
  was AOF with `appendfsync everysec` — the closest analogue to KuttiDB
  `periodic` (`fsync-ms 100`) — plus `appendfsync always` and no-persistence
  extremes.
- **Durable queue** (publish → consume → ACK semantics): KuttiDB
  `src/bench_queue_net.py` (durable queue, 100-byte messages, batched 256 per
  round trip, single Python client, periodic durability, WAL on the SSD).
  RabbitMQ 4.0.5 with PerfTest 2.25.0 (`-s 100`, persistent messages,
  prefetch 100–250, classic durable and quorum queues). NATS 2.10.27
  JetStream with file storage (`nats bench js pub sync`, callback-based
  `js consume` with explicit acks). Redis list rows: `redis-benchmark -t
  lpush` (no acknowledgement semantics — listed for the ceiling only).
- **Durable partitioned stream**: KuttiDB `src/bench_stream_net.py`
  (stream, 40k × 100-byte records, 8 partitions, append/fetch batched 256,
  WAL on the SSD). Kafka 4.1.2 KRaft single broker (512 MiB heap,
  `acks=1`/`acks=all`, no compression, `batch.size=16384 linger.ms=0`) with
  the official producer/consumer perf tests.

### Results at a glance

One small VPS, one CPU, 100-byte messages, every server on the same SSD with
comparable durability. Numbers are thousands of operations per second;
**bold** is the best in the row; "—" means the product does not target that
workload.

| Workload | KuttiDB | Redis | RabbitMQ | NATS JetStream | Kafka |
|---|---:|---:|---:|---:|---:|
| Cache set/get, batched¹ | **651k** (813k @8c) mixed | 399k set / **798k** get | — | — | — |
| Durable queue: publish, 20k msgs | **108k** | 197k (no ack) | 16.6k | 12k–101k | 40k–47k² |
| Durable queue: publish, 200k msgs | 86k | 197k (no ack) | 7.3k | 85k | 37k² |
| Durable queue: consume + ack, 200k msgs | 32k | — | 7.3–13k | **145k** | 39k |
| Event stream: append (40k recs) | **135k** | — | 5.6k³ | 85–101k | 37k–47k² |
| Event stream: read back | **496k** | — | — | **148k**⁴ | 33k (163–255k raw) |
| One durable write at a time | 0.97k | — | **16.6k** | 12.2k | — |
| Server RAM, fresh idle⁵ | **2.9 MB** | 14 MB | 109 MB | 14 MB | 358 MB |
| Server RAM, 200k-message queue⁵ | 46 MB | **36 MB** | 161 MB | 41 MB | 385 MB |

Footnotes, in plain language:

1. KuttiDB's number is an interleaved put/get/delete mix with durability on
   (fsync batched every 100 ms). Redis was measured with AOF `everysec`; with
   fsync on every single write its set rate drops to 36k, with persistence
   fully off both set and get are ~399k.
2. Kafka's single-broker setup does not fsync — after a power loss the last
   messages can be gone — and its producer latency averaged ~1 second per
   record batch on this 1-vCPU box. It is the weakest-durability row.
3. That is RabbitMQ's quorum queue, its strongest-durability mode; its
   classic durable queue manages 9.6k combined and 13.1k consume+ack.
4. NATS JetStream consume was measured with 4 clients; every other number in
   its row and all KuttiDB numbers are 1 client. Core NATS without any
   persistence publishes 1,782k msgs/s — the in-memory ceiling, not a durable
   option.
5. Measured in the resource-usage section below (kernel VmHWM). KuttiDB holds
   live queue contents in RAM by design (cache-first engine), while NATS and
   Kafka stream to files — that is why KuttiDB wins idle RAM by 5× but Redis
   and NATS run lighter on a deep backlog. The ~8 MB figure from the cache
   matrix above is the cache-workload footprint at its small keyspace.
6. The single-record row is where batching stops helping: KuttiDB fsyncs
   before acknowledging each unbatched durable write (~970/s on this SSD),
   while RabbitMQ and NATS group their durability work internally
   (16.6k and 12.2k/s). KuttiDB's batched publish (256 per round trip) is the
   intended path for bulk work.

The one-line summary: **KuttiDB leads batched durable throughput (stream
append, queue publish) and idle RAM, ties NATS on deep-queue publish, and
loses on single-record durable writes, deep-queue consume+ACK, and pure cache
reads** — all of it measured on one shared vCPU with the production website
running on the same machine.

### Cache / KV throughput (100-byte values, WAL on SSD)

| Server (config) | Shape | Throughput | p50 | p99 |
|---|---|---:|---:|---:|
| KuttiDB `periodic` fsync 100 ms | 256-op batches, 1 client | 645,022 ops/s | 460 µs | 673 µs |
| KuttiDB `periodic` fsync 100 ms | 256-op batches, 4 clients | 651,120 ops/s | 1,125 µs | 4,978 µs |
| KuttiDB `periodic` fsync 100 ms | 256-op batches, 8 clients | 813,328 ops/s | 2,276 µs | 8,819 µs |
| KuttiDB durability off (ceiling) | 256-op batches, 8 clients | 859,272 ops/s | 1,183 µs | 8,727 µs |
| Redis 8.0.5 AOF everysec | pipeline 16, 4 connections | SET 266,312 / GET 398,406 ops/s | 199/127 µs | 423/351 µs |
| Redis 8.0.5 AOF everysec | pipeline 256, 4 connections | SET 398,789 / GET 797,578 ops/s | 831/495 µs | 1,967/1,687 µs |
| Redis 8.0.5 no persistence | pipeline 16, 4 connections | SET/GET 399,202 ops/s | 135 µs | 295 µs |
| Redis 8.0.5 AOF fsync always | pipeline 16, 4 connections | SET 36,258 ops/s (GET unaffected) | 1,311 µs | 5,183 µs |
| Redis 8.0.5 AOF everysec | 1 op per round trip, 1 connection | SET 24,888 / GET 22,124 ops/s | 31 µs | 79/159 µs |
| KuttiDB durability off (single-op harness) | 1 op per round trip, 1 client | 44,228 ops/s | 20 µs | 43 µs |

Reading: at matched 256-operation batching, KuttiDB's interleaved
PUT/GET/DELETE stream (645–813k ops/s with `periodic` durability) lands
between Redis's SET (399k) and GET (798k) rates on the same vCPU, and above
Redis's SET rate; the no-durability ceiling (859k) shows the SSD WAL costs
single-digit percent at this fsync cadence. On a single vCPU neither server
scales with clients in a straight line — the multi-client gains recorded on
the ARM64 machine above do not reproduce here, which is expected and not a
server regression.

### Durable queue: publish / consume+ACK (100-byte messages, SSD)

| Server (config) | Publish | Consume+ACK | Steady state (pub+sub) |
|---|---:|---:|---:|
| KuttiDB 1.9 durable queue, batched 256, 20k msgs | 108,132 msgs/s | 63,551 msgs/s (drain) | — |
| KuttiDB 1.9 durable queue, batched 256, 200k msgs | 86,137 msgs/s | 32,192 msgs/s | — |
| KuttiDB 1.9 durable queue, single-record writes | 965 msgs/s | — | — |
| RabbitMQ 4.0.5 classic durable, persistent, publish-only (20k) | 16,597 msg/s | — | — |
| RabbitMQ 4.0.5 classic durable, consume+ACK drain (30k) | — | 13,129 msg/s | — |
| RabbitMQ 4.0.5 classic durable, combined (30–50k) | — | — | 9,584 msg/s |
| RabbitMQ 4.0.5 quorum queue, combined (30k) | — | — | 5,586 msg/s |
| NATS 2.10.27 JetStream file, sync publish (per-msg ack, 50k) | 12,173 msgs/s (p50 75 µs) | — | — |
| NATS 2.10.27 JetStream file, async publish (batch 500, 200k) | 85,172–100,744 msgs/s | — | — |
| NATS 2.10.27 JetStream durable consumer (callback, explicit ack, 4 clients, 200k) | — | 144,605–147,612 msgs/s | — |
| Redis 8.0.5 LPUSH (AOF everysec, no ack, 200k) | 159,744–196,657 ops/s | — | — |
| NATS 2.10.27 Core NATS pub (no persistence) | 1,782,103 msgs/s | — | — |
| Kafka 4.1.2 single broker, `acks=1` / `acks=all` (200k) | 36,630–39,557 / 47,214 recs/s | 33,052 msg/s end-to-end (163–255k steady) | — |

Reading: the 256-message batch is what makes KuttiDB competitive — one group
fsync covers the batch, giving 86–108k msgs/s: level with NATS JetStream's
async publish, ~2.3× Kafka's produce rate, 5–12× RabbitMQ's classic rate,
behind only Redis's un-acknowledged O(1) list push. KuttiDB's
single-record durable path pays one fsync per operation (~965/s here) and is
10–17× behind RabbitMQ's and NATS's grouped durability — a real gap that
batching exists to avoid. Consume+ACK is the weakest measured area: behind
NATS at both scales and behind Kafka at 200k, consistent with the
depth-linked delivery costs documented in the engine baselines (the deferred
ready-pointer work is the known follow-up).

### Durable partitioned stream: append / fetch (100-byte records, SSD)

| Server (config) | Append (write side) | Read side |
|---|---:|---:|
| KuttiDB 1.9 stream, 8 partitions, batched 256, periodic, 40k recs | 135,543 recs/s | fetch 496,211 recs/s |
| KuttiDB stream, single-record appends | 839 recs/s | — |
| Kafka 4.1.2 single broker, `acks=1` / `acks=all`, no compression | 36,630–47,214 recs/s (p50 0.9–1.2 s) | 33,052 msg/s end-to-end incl. 5.3 s group setup; 163,399–254,777 msg/s steady fetch |

Two caveats keep this table honest. First, a single Kafka broker provides
durability only through the OS page cache (no fsync by default), so its row
is the weakest-durability configuration of the table — KuttiDB's batched
append is still ~3–4× faster while covering each batch with an fsync, and its
fetch is ~2–3× Kafka's steady consumer rate. Second, the Kafka numbers were
taken with the broker and client JVM sharing one vCPU; the producer's ~1 s
average latency reflects that starvation (real but machine-specific), and the
consumer end-to-end rate includes one-time group-rebalance setup that the
steady fetch rate does not.

### Exchange routing (KuttiDB server-level, single Python client, per-publish durable, SSD)

| Scenario | Result |
|---|---:|
| Plain durable queue publish (singles) | 824 ops/s |
| Direct exchange, one durable binding | 785 ops/s |
| Fanout, eight durable bindings | 525 ops/s (≈4.2k durable copies/s) |
| Topic exchange, 100 bindings, one match | 1,101 ops/s |
| Topic exchange, unroutable (no durable copy) | 16,282 ops/s |

Every durable copy on this path is fsynced before confirmation, so the SSD
rates are fsync-bound (~1 ms per copy); the unroutable row (no durable copy)
shows the CPU-bound ceiling of the same path (16.3k/s). Routing overhead
relative to the plain-queue baseline stays within noise, as it does on the
macOS baseline above. The earlier 17.5k/s recording of the same rows was the
tmpfs artifact described in the correction note.

### Resource usage under load — 2026-09-12 (same VPS)

The RAM row above, with the full picture behind it. Same machine, same one
product at a time, same `nice -n 15` loopback setup as the throughput runs.
Server peak RAM is the kernel-maintained VmHWM read at phase end; CPU% comes
from `/proc/<pid>/stat` tick deltas over each benchmark window, sampled
separately for the server and its load generator (both processes `nice -n
15`); disk is the `du` delta of each server's data directory during the
durable-write phase. Versions as in the throughput section; Kafka ran with its
heap capped at 512 MiB. KuttiDB's rates in this section were already measured
with the WAL on the SSD.

| Product | Scenario (100 B messages) | Rate | Server RAM idle → peak | Server CPU | Client CPU | Disk per message |
|---|---|---:|---:|---:|---:|---:|
| KuttiDB 1.9 | cache, batched 256, 400k live keys | 484k ops/s | 3 → 67 MiB | 41% | ~55% | — |
| Redis 8.0.5 | SET+GET, pipeline 16 | 263k / 319k ops/s | 14 → 16 MiB¹ | 49% | 39% | — |
| Redis 8.0.5 | 200k LPUSH, AOF everysec | 197k ops/s | 14 → 36 MiB | 55% | 26% | ~74 B/msg² |
| KuttiDB 1.9 | durable queue publish, 200k msgs, batched 256 | 80k msgs/s | 3 → 45 MiB | 43% | 9% | 132 B/msg³ |
| KuttiDB 1.9 | durable queue consume+ACK, 200k msgs | 28k msgs/s | 3 → 45 MiB | 56% | 7% | — |
| NATS 2.10.27 | JetStream async publish, 200k msgs | 85k msgs/s | 14 → 20 MiB | 48% | 36% | 138 B/msg |
| NATS 2.10.27 | JetStream consume+ack, 4 clients | 145k msgs/s | 14 → 41 MiB | 67% | 20% | — |
| RabbitMQ 4.0.5 | publish-only, 20k persistent, classic durable | 16.6k msgs/s | 109 → 159 MiB | 17% | 69%⁴ | 316 B/msg |
| RabbitMQ 4.0.5 | combined produce+consume, 30k msgs | 7.3k msgs/s | 109 → 161 MiB | 33% | 54% | — |
| Kafka 4.1.2 | produce 200k, acks=1 | 36.6k recs/s | 358 → 385 MiB⁵ | 30% | 55% | 110 B/msg⁶ |
| Kafka 4.1.2 | consume 200k msgs | 39.3k msg/s (163k steady) | 358 → 387 MiB | 10% | 43% | — |

Notes, in plain language:

1. redis-benchmark reuses one key per test by default, so the SET/GET phase
   barely grows Redis; the meaningful Redis loaded-RAM number is the
   200k-entry list phase.
2. Redis's AOF (`everysec`) is not fsync-acked per operation, and the
   recorded size is after Redis's automatic AOF rewrite compacted the log —
   the negative growth the rewrite produced is why no delta is quoted.
3. KuttiDB's publish WAL costs 132 bytes per 100-byte message while messages
   are in flight; the queue checkpoint compacts it after consumption, and the
   whole 200k-message run left ~1.2 MB on disk.
4. The PerfTest JVM consumed most of RabbitMQ's CPU; the Erlang server ran
   light on CPU but heavy in RAM (VM baseline ~109 MiB).
5. Kafka's peak includes the 512 MiB heap cap (RSS = heap + metaspace + mapped
   files); its durability remains page-cache-only on a single broker. During
   produce the data dir transiently preallocated ~1.15 GB of index files that
   were reclaimed after the run; the steady topic log is 110 B/msg and the
   whole data dir finished at 66 MB including metadata topics.
6. CPU columns do not sum past 100 because this is one shared core; durable
   publish paths are fsync-bound — the server CPU stays below 100% while
   blocked on disk I/O rather than compute-bound.

Combined server+client CPU cost per 1,000 msgs/s on the durable publish rows:
KuttiDB 0.6 CPU-points (durable, per-batch fsync coverage), Redis LPUSH 0.4
(no per-operation ack), NATS JetStream 1.0, Kafka 2.3, RabbitMQ 5.2. KuttiDB's
publish is disk-bound (server CPU 43% while fsync-waiting), not CPU-bound —
which is also why its throughput on this box is capped by the disk's group
fsync cadence rather than either process saturating the core.

### Summary

Honest scorecard on one vCPU, same SSD on all sides:

- **Wins**: batched durable stream append (135k vs Kafka 37–47k, NATS
  85–101k — 1.4–3.7×, while each batch is fsync-covered); stream fetch
  (496k vs Kafka's steady 163k); idle RAM (2.9 MB — 5× less than Redis,
  12× less than RabbitMQ, 123× less than Kafka); CPU cost per message on
  durable publish (0.6 CPU-points per 1,000 msgs/s).
- **Ties**: batched durable queue publish at 200k messages (86k vs NATS 85k;
  ~2.3× Kafka; 5–12× RabbitMQ), behind only Redis's un-acknowledged push.
- **Losses, recorded rather than hidden**: single-record durable writes
  (965/s — 12–17× behind RabbitMQ's and NATS's grouped durability; the
  batched API is the intended path); deep-queue consume+ACK (32k/s at 200k
  vs NATS 145k with 4 clients and Kafka 39k — the depth-linked delivery cost
  documented in the engine baselines); pure cache GETs (Redis 798k at
  pipeline 256); loaded RAM on deep backlogs (46 MB vs Redis 36 / NATS 41 —
  the in-memory queue is a design choice, and the checkpoint keeps the
  on-disk cost at ~1.2 MB for a fully drained 200k-message run).

Against the first recording of this comparison, the corrected numbers moved
every KuttiDB single-record and stream-append row down and left the batched
cache row essentially unchanged — the tmpfs correction is the difference
between marketing and measurement, and this file keeps the corrected ones.

Reproduce with: `make` (KuttiDB, commit `0ce9a31`, gcc 15.2), then
`TMPDIR=/root/resbench/tmp python3 src/bench_matrix.py --quick --durability
periodic`, `TMPDIR=... python3 src/bench_queue_net.py 7411 20000`,
`TMPDIR=... python3 src/bench_queue_net.py 7411 200000`,
`TMPDIR=... python3 src/bench_stream_net.py 7412 40000 8`,
`TMPDIR=... python3 src/bench_exchange.py 7406 5000` for KuttiDB (the
`TMPDIR` redirect is mandatory on hosts where `/tmp` is tmpfs — on macOS it
is not); `redis-server --appendonly yes --appendfsync everysec` +
`redis-benchmark -n 200000 -d 100 -t set,get,lpush,rpop -P {1,16,256} -c 4
--csv`; `rabbitmq-perf-test` with the flags above; `nats bench js pub
sync|async` + `nats bench js consume` with file storage;
`kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh` as above. The
raw tool outputs of the recorded runs are kept out of the published
repository per the documentation policy (transient working notes live in the
gitignored `docs/plans/`).

## Known gaps in this file

- No Windows-native benchmark tables yet (Windows server build remains the
  documented platform blocker).
- Queue publish/consume baselines beyond the single exchange benchmark, a
  SIGKILL-recovery cost table for streams (the reopen row above covers clean
  restart only), and consumer-lag behavior under slow consumers are not yet
  recorded.
