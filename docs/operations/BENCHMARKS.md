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
- `src/bench_queue_net.py` drives a live server over the protocol with one
  client: durable publish, consume+ACK and one-at-a-time durable publish,
  with exact message-ID, order, payload and ACK-count checks and a required
  full drain. It records server CPU, RSS and write counters from `/proc`
  alongside client CPU, and identifies the binary and the harness by SHA-256.
- `src/xbench.py` runs the same logical durable-queue workload against
  KuttiDB, Redis, NATS, RabbitMQ and Kafka from one client process at a
  matched durability tier, reading each server's cost from `/proc` for its
  whole process tree; `src/xsummary.py` reduces the JSONL to medians and
  lists failed trials rather than dropping them.
- `src/test_queue_wal_compat.py` checks that WALs written by two builds
  replay under each other after `SIGKILL`, which is the gate for any change
  to how records reach the disk.
- On a host whose storage latency drifts between sessions, a before/after
  comparison must interleave the two sides trial by trial and report the
  ratio of medians; a run of one side followed by a run of the other is not
  evidence.
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

## Queue and stream WAL write path — 2026-09-13 (Linux VPS, interleaved A/B)

Two rounds of changes to how records reach the disk. The first round shipped
the parts with no on-disk consequence; the second shipped the two larger wins
once each had the mechanism that makes it safe. Measured against the revision
immediately before each round on the reference VPS (Ubuntu 26.04.1, AMD EPYC
9354P, one shared vCPU, 3.8 GiB RAM, ext4 on `/dev/sda1`). This host's storage
latency drifts between sessions, so every comparison alternates baseline and
candidate trial by trial (medians over eight trials each); the ratio is the
trustworthy figure, not the absolute rate.

### Round 1 — fewer syscalls per record

1. **One syscall per record instead of two or three.** A record's header, name
   and payload are assembled in a staging buffer and issued as a single
   `pwrite` rather than a `write` per field.
2. **Slice-by-8 CRC-32**, same reflected polynomial, output verified
   bit-identical over 68,800 length and split combinations.
3. **The queue checkpoint stages its writes.** `ckpt_emit` issued up to three
   write syscalls per record while holding the metadata lock and every queue
   lock. A profile attributed 66% of server CPU during a drain to it.

| Measurement | Before | After | Δ |
|---|---:|---:|---:|
| Consume+ACK, batches of 256 | 49,996 msgs/s | 66,884 msgs/s | 1.34× |
| Consume+ACK, server CPU per million | 8.8 s | 4.8 s | 1.8× less |
| Durable publish, batches of 256 | 107,748 msgs/s | 118,742 msgs/s | 1.10× |

### Round 2 — batch coalescing and a WAL space reservation

Both were measured in round 1 and held back, each for a specific correctness
reason. Both now ship with that reason addressed.

**Batch coalescing — one write per batch instead of one per record.** The
obstacle was ordering: both engines applied a record to memory as soon as the
write returned, so deferring the write to the barrier moved write-time errors
past the mutation, and a failed append could leave a record readable that no
barrier would ever cover (the stream engine's disk-full test caught exactly
this). The fix is to restructure the batch paths so nothing is applied to
memory until every record of the batch is staged and written:

- `queue_publish_batch` validates the whole batch, allocates the IDs, stages
  all records, writes once, and only then links the messages.
- `queue_consume_batch` selects its messages into an array during the scan,
  writes every delivery record in one call, and publishes the in-flight
  state afterwards — under the queue lock throughout, so no reader observes a
  message as in flight before its record is in the file.
- `queue_ack_batch` / `queue_nack_batch` already used a log pass followed by
  an apply pass; their log pass now stages, and the `sync_log` between the
  passes performs the single write.

The single-record path is unchanged: it still writes before returning,
because its caller applies the record immediately afterwards.

**A WAL space reservation, committed with `fdatasync`.** Writes address an
offset inside a span the file has already been grown to with `fallocate`, so
the inode is untouched and the commit skips the size-update journal
transaction. The obstacle was that a process exiting without a clean close
leaves a zero tail, so "the records run to the end of the file" stops holding
until the next open — and `job_crash_matrix` showed a record appended past
such a gap being silently truncated instead of refusing the open. Silently
discarding a committed record is the wrong failure mode for this format, so
replay now proves the tail is safe before truncating: it scans from the
truncation point, truncates if the remainder is all zeros (a reservation) or
holds nothing that parses, and otherwise refuses the open with
`QUEUE_OPEN_TRAILING_RECORDS` and every byte preserved. A torn tail still
truncates as before — matching a CRC-32 by chance is a 2^-32 event — and the
cost is paid only when a tail exists. `src/test_queue_crash.c` covers both
outcomes.

Cumulative effect of round 2, 100,000 × 100-byte messages, batches of 256,
one client, eight interleaved trials per side:

| Measurement | Before | After | Δ |
|---|---:|---:|---:|
| Durable publish, server CPU per million | 2.5 s | 0.9 s | **2.9× less** |
| Consume+ACK, server CPU per million | 5.2 s | 1.4 s | **3.7× less** |
| Durable publish, one message per call | 781 msgs/s | 1,414 msgs/s | **1.81×** |
| Single publish p50 / p99 | 949 / 4,683 µs | 476 / 2,893 µs | 0.50× / 0.62× |
| Consume+ACK, batches of 256 | 53,065 msgs/s | 69,768 msgs/s | 1.31× |
| Consume+ACK, batch p50 / p99 | 4,094 / 14,218 µs | 2,966 / 11,532 µs | 0.72× / 0.81× |
| Durable publish, batches of 256 | 110,435 msgs/s | 115,173 msgs/s | 1.04× |
| WAL bytes written | identical | identical | 1.00 |
| Idle RSS | 2,928 KiB | 2,938 KiB | unchanged |

The CPU reductions are much larger than the throughput gains because client
and server share one vCPU here: work removed from the server is immediately
consumed by the client process. On a host where the server does not compete
with its own load generator, that freed CPU is available for throughput.
Batched publish moves 4% — it is bound by the client and the device on this
host, not by server CPU.

The acknowledgement contract is unchanged throughout: publish, delivery and
ACK replies return only after a barrier covers their bytes in the file, and
the bytes written per message are identical before and after.

### Device ceiling for reference

Writing a 160-byte record and committing it, 400 times, on this VPS:

| Method | p50 | p95 | commits/s at p50 |
|---|---:|---:|---:|
| Append and grow the file, `fsync` | 841 µs | 2,836 µs | 1,190 |
| Append and grow the file, `fdatasync` | 829 µs | 2,889 µs | 1,207 |
| `fallocate`d span, `fsync` | 613 µs | 2,137 µs | 1,632 |
| `fallocate`d span, `fdatasync` | 297–391 µs | 630–2,035 µs | 2,553–3,370 |
| `fallocate(KEEP_SIZE)` span, `fdatasync` | 1,059–1,094 µs | 3,480–3,760 µs | 914–944 |
| Zero-filled span, `fdatasync` | 215–270 µs | 285–467 µs | 3,701–4,652 |

`FALLOC_FL_KEEP_SIZE` keeps the file length honest and would have avoided the
tail question entirely, but it is not viable: every write still updates the
inode, so it gives no benefit at all. Zero-filling is marginally faster than
a reservation per commit but stalls the writer for the length of the fill.
The reservation is 8 MiB ahead of the cursor; a clean close returns the
unused tail, so the WAL on disk is exactly the records it holds. The
reservation is Linux-only (`fallocate`); other platforms fall back to
extending writes and keep the round-1 behaviour.

### Cross-version WAL compatibility

Verified with the writer killed by `SIGKILL`, so the reader must replay a WAL
that was never cleanly closed — including, for the new writer, one carrying a
reservation tail (8,388,642 bytes of file for 672,714 bytes of records):

| Writer → reader | Messages | Replayed depth | Read back in order | Drained |
|---|---:|---:|---|---|
| before → after | 5,020 | 5,020 | yes | yes |
| after → before | 5,020 | 5,020 | yes | yes |
| after → after | 5,020 | 5,020 | yes | yes |
| before → before | 5,020 | 5,020 | yes | yes |

Run with `src/test_queue_wal_compat.py OLD_BINARY NEW_BINARY`.

## Cache comparison, matched periodic durability — 2026-09-13

The cache equivalent of the queue comparison, built to answer the three
objections the audit raised against the earlier cache numbers.

**Workload.** Write 200,000 distinct keys with 100-byte values, then read all
200,000 back and verify every value. Batches of 256. Three trials; medians.
Harness: `src/xbench_kv.py`.

**Same operations on both sides.** The earlier comparison ranked KuttiDB's
interleaved PUT/GET/DELETE mix against Redis's separate SET and GET runs.
Here both systems run the same two phases in the same order, and both use a
single command carrying the whole batch — KuttiDB `put_many`/`get_many`
against Redis `MSET`/`MGET`. Using a Redis pipeline of individual `SET`s
instead of `MSET` measured 80k writes/s, but that number is the cost of
`redis-py` assembling 256 commands (9.5 client CPU seconds per million), not
of the Redis server; `MSET` is the like-for-like operation and is what the
table below uses.

**Same keyspace.** The earlier resource comparison gave KuttiDB 400k distinct
live keys and Redis one repeatedly overwritten key, so the memory columns
described different amounts of stored data. Here both hold the same 200,000
distinct keys, which is what makes the RSS row meaningful.

**Same durability window.** KuttiDB's cache WAL at `--fsync-ms 1000` against
Redis `appendfsync everysec`: both nominally a one-second loss window. (The
earlier comparison put KuttiDB's 100 ms setting against Redis's one second.)


> **Build note.** KuttiDB's rows were re-measured on the shipped build (both
> rounds of the WAL write-path work above). Competitor rows were collected in
> an earlier session and are unchanged, since their configuration did not
> change — but on a host with this much storage drift that means the two
> columns carry independent run-to-run spread. Treat the ordering as the
> result, not the exact ratios.

| Measurement | KuttiDB | Redis 8.0.5 |
|---|---:|---:|
| Write, ops/s | **502,332** | 234,867 |
| Read, ops/s | **424,069** | 359,362 |
| Write, server CPU s per million | **1.1** | 1.6 |
| Read, server CPU s per million | **0.6** | 0.7 |
| Write, client CPU s per million | **0.8** | 2.4 |
| Write, disk bytes per operation | 133 | 137 |
| Idle RSS | **2.9 MiB** | 14.5 MiB |
| RSS holding 200,000 keys | **35.8 MiB** | 54.0 MiB |
| Data directory after the run | 26.6 MB | 27.4 MB |

Reading this fairly: KuttiDB is about 2.1× on the write phase and about 1.2×
on the read phase, with roughly a third less server CPU per operation. The
memory rows are the larger result — 5× smaller idle footprint, and 1.5× less
resident memory holding the identical 200,000 keys, with essentially the same
bytes on disk. What this does not cover: Redis's data structures, scripting,
replication, cluster mode, or eviction behaviour under memory pressure. It is
a comparison of the plain key/value path only.

```sh
python3 src/xbench_kv.py --systems kuttidb,redis --count 200000 --batch 256 \
  --fsync-ms 1000 --repeats 3 --kuttidb-binary /absolute/path/to/kuttidb \
  --jsonl /absolute/path/to/new-results.jsonl
```

## Stream comparison, matched durability — 2026-09-13

**Workload.** Append 100,000 100-byte records to one topic with 8 partitions
in batches of 256, then read all 100,000 back from offset zero in batches of
256, committing the group offset after each batch. Payloads and the total
count are verified. Three trials; medians. Harness: `src/xbench_stream.py`.

**Durability tier.** Every append is on the device before it is
acknowledged: KuttiDB stream appends wait for an fsync covering their record;
Kafka runs `acks=all` with `log.flush.interval.messages=1`; Redis Streams runs
`appendonly yes` with `appendfsync always`. Server cost is read from `/proc`
for each server's whole process tree.


> **Build note.** KuttiDB's rows were re-measured on the shipped build (both
> rounds of the WAL write-path work above). Competitor rows were collected in
> an earlier session and are unchanged, since their configuration did not
> change — but on a host with this much storage drift that means the two
> columns carry independent run-to-run spread. Treat the ordering as the
> result, not the exact ratios.

| Measurement | KuttiDB | Redis Streams | Kafka 4.1.2 |
|---|---:|---:|---:|
| Append, records/s | **117,061** | 43,586 | 27,126 |
| Read+commit, records/s | **157,686** | 87,914 | 12,755 |
| Append, server CPU s per million | **0.9** | 3.3 | 25.6 |
| Read, server CPU s per million | **0.9** | 1.0 | 16.1 |
| Append, client CPU s per million | **1.0** | 9.1 | 2.3 |
| Append, disk bytes per record | **140** | 203 | 164 |
| Idle RSS | **2.9 MiB** | 14.5 MiB | 347.7 MiB |
| Loaded RSS | **16.8 MiB** | 26.0 MiB | 365.3 MiB |
| Startup to first accepted request | 0.2 s | **0.0 s** | 11.9 s |
| Data directory after the run | 16.8 MB | **16.5 MB** | 1,248.8 MB |

Reading this fairly:

- **Kafka's numbers are what `flush.messages=1` costs it.** That setting is
  not how Kafka is normally run; its default leaves flushing to the operating
  system and relies on replication across brokers for durability instead.
  This comparison is single-node and fsync-per-record by construction, which
  is the tier KuttiDB targets and the one Kafka is least suited to. A
  replicated multi-broker Kafka is a different system answering a different
  question, and nothing here speaks to it.
- **Kafka's 1.2 GB data directory** against 16.8 MB is mostly segment and
  index preallocation plus the `__consumer_offsets` topic, not payload. It is
  a real disk-footprint difference on a small single node, not write
  amplification of the same magnitude.
- **Redis Streams' read path** is `XRANGE` plus a durable offset key, which is
  the closest available analogue to a group commit; it is not an identical
  operation to either of the other two.
- **Not measured:** replication, multi-consumer group rebalancing under load,
  compaction, or retention enforcement while writing.

```sh
python3 src/xbench_stream.py --systems kuttidb,redis,kafka --count 100000 \
  --batch 256 --partitions 8 --repeats 3 \
  --kuttidb-binary /absolute/path/to/kuttidb \
  --jsonl /absolute/path/to/new-results.jsonl
```

## Cross-server comparison, matched durability — 2026-09-12

A like-for-like rerun of the comparison the audit below withdrew. The
objections that made the earlier numbers unusable are addressed by
construction: one workload, one client process, one durability tier, and
server cost read from the kernel rather than inferred.

**Workload, identical for every system.** Publish 100,000 100-byte messages
into one durable queue, then drain all 100,000 with an explicit consumer
acknowledgement, checking the payload of every message and requiring the
count to come back exactly. Work is submitted in batches of 256 so each
system gets the same opportunity to amortise round trips. Three trials per
system; medians reported. Harness: `src/xbench.py` with `src/xbench_core.py`.

**One client.** Every system is driven from the same single Python process
using its own maintained client library (`kuttidb_client`, `redis-py`,
`nats-py`, `pika`, `confluent-kafka`), so no system is measured through a
faster or slower benchmark tool than another. Client CPU is reported
separately because the libraries differ in efficiency.

**One durability tier.** Every system is configured so that a publish is not
acknowledged until its bytes are on the device:

| System | Version | Configuration |
|---|---|---|
| KuttiDB | this revision | durable queue; publish, delivery and ACK replies each wait for an fsync covering their record |
| Redis | 8.0.5 | Streams with a consumer group; `appendonly yes`, `appendfsync always` |
| NATS | 2.10.27 | JetStream file store, `sync_interval: always`; publish waits for its PubAck, consumer uses `ack_sync` |
| RabbitMQ | 4.0.5 | durable classic queue, persistent messages, publisher confirms; consumer acknowledges |
| Kafka | 4.1.2 | single broker, `acks=all`, `log.flush.interval.messages=1`; consumer commits offsets synchronously |

**Server cost from the kernel.** CPU seconds, RSS and bytes written are read
from `/proc` for the server's whole process tree (so the JVM's and the BEAM's
threads are included), sampled before and after each phase, and divided by a
fixed message count. A measurement that could not be read is reported as
`n/a`, never as zero.

**Environment.** Ubuntu 26.04.1, kernel 7.0.0-30-generic, AMD EPYC 9354P, one
shared vCPU, 3.8 GiB RAM, ext4 on `/dev/sda1`. Client and server share that
one vCPU over loopback, without TLS or compression, and the host also serves
a website. This measures that deployment, not isolated server capacity.

**KuttiDB's rows are from the shipped build.** An earlier draft recorded
197,553 publish and 93,224 consume+ACK from a build that deferred writes past
the in-memory mutation; that build was withdrawn as unsafe and those figures
do not stand. Publish medians between 109,343 and 157,385 have been observed
across sessions on this host, which is the scale of its storage drift.

### Results

> **Build note.** KuttiDB's rows were re-measured on the shipped build (both
> rounds of the WAL write-path work above). Competitor rows were collected in
> an earlier session and are unchanged, since their configuration did not
> change — but on a host with this much storage drift that means the two
> columns carry independent run-to-run spread. Treat the ordering as the
> result, not the exact ratios.


| Measurement | KuttiDB | Redis | NATS | RabbitMQ | Kafka |
|---|---:|---:|---:|---:|---:|
| Publish, msgs/s | **142,346** | 42,536 | 607 | 1,651 | 27,013 |
| Consume+ACK, msgs/s | **85,409** | 50,917 | 600 | 29,596 | 12,257 |
| Publish, server CPU s per million | **1.1** | 3.5 | 125.3 | 428.1 | 28.4 |
| Consume+ACK, server CPU s per million | **1.3** | 4.1 | 183.1 | 18.2 | 22.1 |
| Publish, client CPU s per million | **0.9** | 9.6 | 26.0 | 170.2 | 2.4 |
| Publish, disk bytes per message | **149** | 201 | 4,230 | 303 | 164 |
| Consume+ACK, disk bytes per message | 216 | 240 | 4,199 | **9** | 18 |
| Idle RSS | **2.9 MiB** | 14.5 MiB | 15.6 MiB | 128.8 MiB | 343.8 MiB |
| Loaded RSS after drain | **21.4 MiB** | 26.5 MiB | 36.2 MiB | 160.5 MiB | 369.5 MiB |
| Startup to first accepted request | 0.2 s | **0.0 s** | 0.2 s | 3.2 s | 12.2 s |
| Data directory after drain | 9.2 MB | 37.1 MB | **0.0 MB** | 7.9 MB | 1,102 MB |

### What these numbers do and do not say

- **The publish gap is a batching gap, and it should be read as one.** KuttiDB's
  batch protocol puts 256 messages under one barrier; Redis's pipeline gets
  the same amortisation from `appendfsync always`, which fsyncs once per
  event-loop iteration. NATS with `sync_interval: always` and RabbitMQ with
  per-publish confirms do not group-commit here, so they pay roughly one
  device barrier per message and land near this device's fsync ceiling. That
  is a real architectural difference at this durability setting, not a
  measurement artifact — but it is a statement about amortisation, not about
  how fast each system can make one message durable.
- **At one message per barrier, nobody wins by much, and KuttiDB does not
  win.** Publishing one at a time and waiting for durability, KuttiDB
  measures 1,042–1,281 msgs/s across runs on this host (p50 495–610 µs),
  against RabbitMQ's 1,651 and NATS's ~1,200. All of these sit within a
  factor of two of the raw device ceiling (about 1,200–1,700 commits/s for an
  extending write, see the device table above), because at that point the
  storage, not the server, is the limit. KuttiDB's advantage is in amortised
  throughput, CPU and memory — not in single-message commit latency, where it
  currently trails RabbitMQ.

  One candidate explanation was tested and rejected: that the cache WAL's
  periodic fsync (100 ms by default) interferes with queue commits through
  the shared ext4 journal. Running the same binary and workload with the
  cache interval at 100 ms and at 60 s, six interleaved trials each, moved
  single durable publish by 1.08× — inside this host's run-to-run spread —
  so the cost is the device barrier itself, not cross-WAL interference.
  Reproduce with `src/bench_queue_net.py --fsync-ms`.
- **RabbitMQ's low consume-side write volume is real.** Its 9 bytes per
  message on the drain reflects that acknowledgements for already-persisted
  messages need very little new durable state. KuttiDB writes a delivery and
  an acknowledgement record per message; that is a design difference with a
  measurable cost, and it is the largest remaining write-amplification item
  on the queue path.
- **NATS's 4,230 bytes written per 100-byte message** is the cost of
  `sync_interval: always` in its file store, which rewrites index and
  metadata blocks per commit. NATS is not normally run this way; its default
  is a two-minute interval. That default is a different durability promise
  and is not comparable to the other rows here.
- **RSS is not total memory.** It excludes the kernel page cache these
  disk-backed brokers rely on. The idle-RSS column is a fair comparison of
  process footprint; it is not a claim about total machine memory.
- **One vCPU, shared with the client.** Server and client CPU compete. This
  is representative of a small single-node deployment and it is the same
  constraint for every system, but it compresses the differences between
  systems whose clients are expensive (RabbitMQ via `pika`: 170 client CPU
  seconds per million) and those whose clients are cheap.
- **Not measured here:** clustering, replication, multi-consumer fan-out,
  crash-consistency under power loss, or any workload other than this one.
  Kafka and RabbitMQ are built for guarantees this single-node comparison
  does not exercise.

### Reproducing

```sh
python3 src/xbench.py --systems kuttidb,redis,nats,rabbitmq,kafka \
  --count 100000 --batch 256 --repeats 3 \
  --kuttidb-binary /absolute/path/to/kuttidb \
  --jsonl /absolute/path/to/new-results.jsonl
python3 src/xsummary.py /absolute/path/to/new-results.jsonl
```

Failed trials are written to the JSONL with `verified: false`, their phase
and a traceback, and are listed alongside the successful results rather than
dropped. The JSONL is appended, never overwritten.

## Cross-server VPS measurements — 2026-09-12: comparison audit

The original scorecard is withdrawn as a like-for-like ranking. The recorded
measurements remain useful observations, but the workloads, concurrency,
retention and acknowledgement guarantees were not equivalent. Do not use them
to claim either overall leadership or an equivalent-durability loss.

Environment recorded for the original runs: Ubuntu 26.04.1, kernel
7.0.0-30-generic, AMD EPYC 9354P, one shared vCPU, 3.8 GiB RAM, ext4 on
`/dev/sda1`. Client and server share that CPU over loopback, without TLS or
compression. The host also serves the production website. These are
measurements of that deployment, not isolated server capacity or a guarantee
for other VPS providers.

### Findings verified against commands, source and the VPS

- **Storage:** `/tmp` is tmpfs. The earliest KuttiDB runs therefore did not
  measure disk durability. The historical KuttiDB numbers below are the
  subsequently corrected SSD measurements. New queue runs refuse tmpfs and
  require an explicit disk-backed `--data-dir` on this VPS.
- **Cache:** KuttiDB's interleaved PUT/GET/DELETE mix cannot rank against
  separate Redis SET and GET tests. The resource comparison also used 400k
  live keys for KuttiDB versus a repeatedly overwritten key for Redis.
  Redis AOF `everysec` and KuttiDB cache `periodic` at 100 ms have different
  potential loss windows. A pipeline and a protocol batch can also have
  different latency boundaries.
- **Queue durability:** KuttiDB durable queue publish, delivery and ACK
  replies wait for fsync, including one durability wait per explicit batch.
  The cache `--durability periodic` setting does not turn queue replies into
  periodic acknowledgements. The queue already has a ready hint and tag
  index; references to those features being absent were stale.
- **NATS:** the installed 2.10.27 source defaults `sync_interval` to two
  minutes. A synchronous JetStream publish call waits for its server reply,
  which does not itself imply an fsync with that setting. That version also
  supports `sync_interval: always`. The earlier consumer command used four
  clients and reported `double-acked=false`; KuttiDB used one client and
  waited for ACK replies. See the version-pinned
  [file store](https://github.com/nats-io/nats-server/blob/v2.10.27/server/filestore.go)
  and [configuration parser](https://github.com/nats-io/nats-server/blob/v2.10.27/server/opts.go).
- **RabbitMQ:** the retained resource script used persistent messages for
  its publish-only phase but omitted `--confirm`. Its combined phase also
  omitted the persistent-message flag. Neither command establishes a
  throughput rate for fsync-confirmed individual publishes. PerfTest's
  [publisher-confirm option](https://perftest.rabbitmq.com/) is separate from
  persistence. The earlier “12–17 times slower durable singles” claim was
  therefore unsupported.
- **Kafka:** the recorded single-broker producer used `acks=1` or `acks=all`
  without a per-ack fsync requirement. End-to-end consumer timing included
  startup and group establishment, while another figure excluded them.
  A raw stream fetch is also a different operation from queue consume+ACK.
- **Resources:** CPU percentage during different-duration, different-rate
  tests does not measure efficiency on its own. Use server and client CPU
  seconds per fixed number of verified messages. VmHWM is process-lifetime
  peak RSS, not a separate peak for every phase. Directory size change is
  not bytes written; rewriting, preallocation and reclamation affect it.
  RSS also excludes much of the kernel page cache used by disk-backed
  brokers, so RSS alone cannot establish total machine memory cost.

### Historical SSD observations, not a ranking

These values preserve the previously recorded reference points. They are
single-run or short-run observations from different harnesses; the fresh
repeated measurements below supersede them for evaluating the changes here.

| Product and workload | Earlier observed rate | Limitation |
|---|---:|---|
| KuttiDB mixed cache, batch 256, 1 / 4 / 8 clients | 645k / 651k / 813k ops/s | Mixed operations; not comparable to pure SET/GET |
| Redis 8.0.5, pipeline 256, AOF everysec | SET 399k; GET 798k ops/s | Different operations, keyspace and loss window |
| KuttiDB durable queue, 20k messages, batch 256 | publish 108k; consume+ACK 63.6k msgs/s | One client; fsync-covered batches |
| KuttiDB durable queue, 200k messages, batch 256 | publish 86.1k; consume+ACK 32.2k msgs/s | One client; fsync-covered batches |
| KuttiDB durable queue, individual publish | 965 msgs/s | One outstanding fsync-covered publish |
| NATS 2.10.27 JetStream, default sync, async publish | 85k–101k msgs/s | Batch 500; periodic fsync |
| NATS JetStream, default sync, synchronous publish | 12.2k msgs/s | Reply does not establish per-message fsync |
| NATS JetStream consumer | 145k–148k msgs/s | Four clients; no double ACK |
| RabbitMQ 4.0.5 classic, persistent publish | 16.6k msgs/s | Publisher confirms absent in saved command |
| RabbitMQ classic drain | 13.1k msgs/s | Different client/protocol/ACK timing |
| Redis list LPUSH, AOF everysec | 197k ops/s | No visibility lease or consumer ACK |
| KuttiDB stream, 40k records, 8 partitions | append 135.5k; fetch 496.2k records/s | Explicit batches of 256 |
| Kafka 4.1.2, single broker | produce 36.6k–47.2k records/s | No per-ack fsync requirement |
| Kafka consumer | 33k end-to-end; 163k–255k steady records/s | Different timing boundaries |

Earlier idle / loaded server memory observations were approximately:
KuttiDB 3 / 45 MiB; Redis 14 / 36 MiB for a list backlog; NATS 14 / 20 MiB
at publish and 41 MiB at consume; RabbitMQ 109 / 161 MiB; Kafka 358 / 387 MiB.
These are RSS figures under different workloads. They do not establish a
matched total-memory winner. The retained raw logs remain in the local,
gitignored working-results directory; none of the discarded tmpfs numbers
should be republished as disk-durable results.

### Reproducing the verified queue workload

Build each revision separately, then use the **same** harness for both:

```sh
python3 src/bench_queue_net.py 17411 200000 \
  --binary /absolute/path/to/kuttidb --data-dir /absolute/path/on/ssd \
  --threads 1 --batch 256 --repeats 5 --single-count 0 \
  --jsonl /absolute/path/to/new-results.jsonl
```

The harness uses fresh queues, one client, 100-byte values, exact message-ID,
order, payload and ACK-count checks, and requires a complete drain. Latencies
are client batch durations; consume+ACK latency includes both round trips and
validation. Every result identifies the binary and harness by SHA-256. Linux
server CPU/RSS/write counters and client CPU time are recorded separately;
unavailable measurements are not replaced by zero. The JSONL file must be new
so previous evidence is never overwritten. Failed runs exit nonzero, record
`verified=false`, and retain their log and WAL directory for investigation.
Successful temporary data is removed after the process has stopped.

For a comparison, alternate baseline/candidate order, preserve every trial,
and report failures with the successful results. Run correctness tests outside
the benchmark window. Do not run competing load generators simultaneously on
this one-vCPU host. Process-kill recovery tests validate that failure model;
they do not simulate a hypervisor or storage device losing power.

## Known gaps in this file

- No Windows-native benchmark tables yet (Windows server build remains the
  documented platform blocker).
- Queue publish/consume baselines beyond the single exchange benchmark, a
  SIGKILL-recovery cost table for streams (the reopen row above covers clean
  restart only), and consumer-lag behavior under slow consumers are not yet
  recorded.
- Single-message durable publish now measures ~1,414 msgs/s against
  RabbitMQ's ~1,651 on the reference VPS, having been ~950 before the space
  reservation. The remaining gap has not been attributed; cross-WAL
  interference was tested and ruled out.
- The consume path writes a delivery and an acknowledgement record per
  message (216 bytes per 100-byte message against RabbitMQ's 9 on the drain).
  A coalesced batch record for deliveries and acknowledgements is the
  identified follow-up and is not yet implemented.
- The cross-server comparisons cover one single-node workload per engine.
  Clustering, replication, multi-consumer fan-out and power-loss crash
  consistency are not measured, and the systems compared are built for
  guarantees these comparisons do not exercise.
- The stream engine's batch paths were not restructured for coalescing:
  `stream_append_batch` already writes one record for a whole batch, so it
  had nothing to gain, but `stream_commit_batch_if_generation` and the group
  offset reset still write one record per commit.
- Batched publish throughput is bound by the client and the device on this
  one-vCPU host, so the 2.9× reduction in server CPU per message converts to
  only 4% more throughput here. The headroom has not been measured on a host
  where the server does not share a core with its load generator.
- The consume path still writes a delivery and an acknowledgement record per
  message (208 bytes per 100-byte message against RabbitMQ's 9 on the drain).
  A coalesced range record for deliveries and acknowledgements would cut
  that; it is not implemented.
- Memcached, Valkey and Dragonfly are not in the cache comparison; only
  Redis is. NATS JetStream is not in the stream comparison.
- The WAL space reservation is Linux-only (`fallocate`). macOS and other
  platforms fall back to extending writes and do not get the commit-rate or
  tail-latency improvement recorded above.
