# Benchmarks

Every number here describes a recorded run on the machine named with it. None
of them is a guarantee for other hardware, and none is a marketing claim.

**Reading this file**

- **↑** means higher is better, **↓** means lower is better. The best value in
  each row is **bold**.
- The reference host has noisy storage. Where a before/after ratio matters,
  the two sides are interleaved trial by trial and the *ratio of medians* is
  the result — not the absolute rate.
- Every comparison states the durability setting each system ran at. Numbers
  measured at different durability settings are not comparable, and this file
  does not rank them as if they were.

---

## 1. Cross-server comparison

One workload per engine, one client process driving every system through its
own maintained library, every system configured so a write is acknowledged
only after its bytes are on the device. Server cost is read from `/proc` for
the server's whole process tree, so it does not depend on the client library.

**Host for all three tables:** Ubuntu 26.04.1, AMD EPYC 9354P, **one shared
vCPU**, 3.8 GiB RAM, ext4. Client and server share that vCPU over loopback,
no TLS, no compression; the host also serves a website.

### 1.1 Cache — KuttiDB vs Redis

200,000 distinct keys, 100-byte values, batches of 256. Write every key, then
read every key back and verify each value. Both sides use one command per
batch — KuttiDB `put_many`/`get_many`, Redis `MSET`/`MGET`. Both run a
one-second durability window (KuttiDB `--fsync-ms 1000`, Redis `appendfsync
everysec`). Three trials, medians.

| Metric | KuttiDB | Redis 8.0.5 | KuttiDB advantage |
|---|---:|---:|---:|
| Write, ops/s ↑ | **502,332** | 234,867 | **2.1×** |
| Read, ops/s ↑ | **424,069** | 359,362 | 1.2× |
| Write, server CPU s / M ops ↓ | **1.1** | 1.6 | 1.5× less |
| Read, server CPU s / M ops ↓ | **0.6** | 0.7 | 1.2× less |
| Disk bytes / write ↓ | **133** | 137 | ~equal |
| Idle RSS ↓ | **2.9 MiB** | 14.5 MiB | **5.0× less** |
| RSS holding 200k keys ↓ | **35.8 MiB** | 54.0 MiB | 1.5× less |
| Data directory ↓ | **26.6 MB** | 27.4 MB | ~equal |

> Redis was given `MSET`, not a pipeline of `SET`s. Pipelining 256 individual
> `SET`s measures 80k/s, but that is `redis-py` assembling 256 commands
> (9.5 client CPU seconds per million), not the Redis server. The 2.1× above
> is against Redis at its best on this workload.
>
> **Not covered:** Redis data structures, scripting, replication, cluster
> mode, or eviction under memory pressure. This is the plain key/value path.

### 1.2 Queue — KuttiDB vs Redis, NATS, RabbitMQ, Kafka

100,000 messages, 100-byte payloads, batches of 256, one durable queue.
Publish all, then drain all with an explicit consumer acknowledgement,
verifying payloads and the exact count. Three trials, medians.

| Metric | KuttiDB | Redis | NATS | RabbitMQ | Kafka | Best rival |
|---|---:|---:|---:|---:|---:|---:|
| Publish, msgs/s ↑ | **142,346** | 42,536 | 607 | 1,651 | 27,013 | **3.3×** |
| Consume+ACK, msgs/s ↑ | **85,409** | 50,917 | 600 | 29,596 | 12,257 | **1.7×** |
| Publish CPU s / M ↓ | **1.1** | 3.5 | 125.3 | 428.1 | 28.4 | 3.2× less |
| Consume CPU s / M ↓ | **1.3** | 4.1 | 183.1 | 18.2 | 22.1 | 3.2× less |
| Client CPU s / M ↓ | **0.9** | 9.6 | 26.0 | 170.2 | 2.4 | 2.7× less |
| Publish disk B / msg ↓ | **149** | 201 | 4,230 | 303 | 164 | 1.1× less |
| Consume disk B / msg ↓ | 208 | 240 | 4,199 | **9** | 18 | **23× worse** |
| Idle RSS ↓ | **2.9 MiB** | 14.5 | 15.6 | 128.8 | 343.8 | **5.0× less** |
| Loaded RSS ↓ | **21.4 MiB** | 26.5 | 36.2 | 160.5 | 369.5 | 1.2× less |
| Startup to ready ↓ | 0.2 s | **0.0 s** | 0.2 | 3.2 | 12.2 | — |
| Data directory ↓ | 9.1 MB | 37.1 | **0.0** | 7.9 | 1,102 | — |

**Durability each system ran at**

| System | Setting |
|---|---|
| KuttiDB | durable queue; publish, delivery and ACK replies each wait for an fsync covering their record |
| Redis 8.0.5 | Streams with a consumer group; `appendonly yes`, `appendfsync always` |
| NATS 2.10.27 | JetStream file store, `sync_interval: always`; synchronous publish, `ack_sync` |
| RabbitMQ 4.0.5 | durable classic queue, persistent messages, publisher confirms |
| Kafka 4.1.2 | single broker, `acks=all`, `log.flush.interval.messages=1` |

> **The publish gap is a batching gap.** KuttiDB puts 256 messages under one
> barrier; Redis gets the same amortisation from `appendfsync always`, which
> fsyncs once per event-loop iteration. NATS and RabbitMQ do not group-commit
> at this setting, so they pay roughly one device barrier per message and land
> near this device's fsync ceiling. That is a real architectural difference —
> but it is a statement about amortisation, not about how fast each system can
> make *one* message durable.
>
> **At one message per barrier, KuttiDB does not win.** Publishing one at a
> time and waiting for durability, KuttiDB measures ~1,414 msgs/s against
> RabbitMQ's ~1,651. All of these sit near the raw device ceiling, because at
> that point the storage is the limit, not the server.
>
> **KuttiDB loses the consume-side write volume badly** — 208 bytes per
> message against RabbitMQ's 9. It writes a delivery record and an
> acknowledgement record per message; RabbitMQ needs almost no new durable
> state to acknowledge an already-persisted message. This is the largest
> open item on the queue path.
>
> **NATS's 4,230 bytes written per 100-byte message** is what
> `sync_interval: always` costs its file store. NATS is not normally run this
> way — its default is a two-minute interval, which is a different durability
> promise and not comparable to these rows.

### 1.3 Stream — KuttiDB vs Redis Streams, Kafka

100,000 records, 100-byte payloads, one topic with 8 partitions, batches of
256. Append all, then read all back from offset zero, committing the group
offset after each batch. Three trials, medians.

| Metric | KuttiDB | Redis Streams | Kafka 4.1.2 | Best rival |
|---|---:|---:|---:|---:|
| Append, records/s ↑ | **117,061** | 43,586 | 27,126 | **2.7×** |
| Read+commit, records/s ↑ | **157,686** | 87,914 | 12,755 | **1.8×** |
| Append CPU s / M ↓ | **0.9** | 3.3 | 25.6 | 3.7× less |
| Read CPU s / M ↓ | **0.9** | 1.0 | 16.1 | 1.1× less |
| Client CPU s / M ↓ | **1.1** | 9.1 | 2.3 | 2.1× less |
| Append disk B / record ↓ | **140** | 203 | 164 | 1.2× less |
| Idle RSS ↓ | **3.0 MiB** | 14.5 | 347.7 | **4.8× less** |
| Loaded RSS ↓ | **16.8 MiB** | 26.0 | 365.3 | 1.5× less |
| Startup to ready ↓ | 0.2 s | **0.0 s** | 11.9 s | — |
| Data directory ↓ | **12.4 MB** | 16.5 | 1,248.8 | 1.3× less |

Durability: KuttiDB stream appends wait for an fsync covering their record;
Kafka `acks=all` with `log.flush.interval.messages=1`; Redis Streams
`appendonly yes` with `appendfsync always`.

> **Kafka's numbers are what `flush.messages=1` costs it.** That is not how
> Kafka is normally run — its default leaves flushing to the OS and relies on
> replication across brokers for durability. This comparison is single-node
> and fsync-per-record by construction: the tier KuttiDB targets and the one
> Kafka is least suited to. A replicated multi-broker Kafka answers a
> different question, and nothing here speaks to it.
>
> **Kafka's 1.2 GB data directory** against 12.4 MB is mostly segment and
> index preallocation plus `__consumer_offsets`, not payload.

### 1.4 What none of these tables measure

Replication, clustering, multi-consumer fan-out and rebalancing under load,
compaction, retention enforcement while writing, or crash consistency under
power loss. RSS also excludes the kernel page cache these disk-backed brokers
rely on, so the memory rows compare process footprint, not total machine
memory. RabbitMQ and Kafka are built for guarantees this single-node
comparison does not exercise.

---

## 2. What changed — WAL write path, 2026-09-13

Profiling the durable queue path attributed **86% of server CPU to the kernel
write path** and, during a drain, **66% to the checkpoint** — both because
records reached the disk one syscall per field. Two rounds of fixes followed.
Eight interleaved trials per side, 100,000 × 100-byte messages, batches of 256.

Each round is its own interleaved A/B, so the two ratio columns come from
separate runs on a drifting host; **combined** is their product, not a single
measurement. (The runs agree where they overlap: round 1 ended at 2.4 s
publish CPU and round 2 started at 2.5.)

| Measurement | Round 1 | Round 2 | Combined |
|---|---:|---:|---:|
| Publish, server CPU s / M ↓ | 3.0 → 2.4 | 2.5 → **0.9** | **~3.3× less** |
| Consume+ACK, server CPU s / M ↓ | 8.8 → 4.8 | 5.2 → **1.4** | **~6.3× less** |
| Consume+ACK, msgs/s ↑ | 50.0k → 66.9k | 53.1k → **69.8k** | **~1.40×** |
| Durable publish, 1 msg/call ↑ | 926 → 957 | 781 → **1,414** | **~1.53×** |
| Single publish p50 ↓ | unchanged | 949 → **476 µs** | **2.0× less** |
| Publish, batches of 256 ↑ | 107.7k → 118.7k | 110.4k → 115.2k | **~1.07×** |
| WAL bytes / message | unchanged | unchanged | **1.00** |
| Idle RSS | unchanged | unchanged | **1.00** |

Batched publish barely moves because on a one-vCPU host it is bound by the
client and the device, not by server CPU — the client immediately consumes
whatever the server frees. The CPU rows are the real result there.

### Round 1 — fewer syscalls per record

1. **One syscall per record instead of two or three.** A record's header, name
   and payload are assembled in a staging buffer and issued as a single
   `pwrite` rather than a `write` per field.
2. **Slice-by-8 CRC-32**, same reflected polynomial, output verified
   bit-identical to the byte-at-a-time routine over 68,800 length and split
   combinations — so existing WAL files verify unchanged.
3. **The checkpoint stages its writes.** `ckpt_emit` issued up to three write
   syscalls per record while holding the metadata lock and every queue lock,
   so its cost was a pause for the whole store.

### Round 2 — two wins that each needed a safety mechanism first

Both were measured in round 1 and deliberately held back. Each now ships with
its obstacle addressed.

**Batch coalescing — one write per batch instead of one per record.**

*Obstacle:* both engines applied a record to memory as soon as the write
returned, so deferring the write to the barrier moved write-time errors past
the mutation. A failed append could leave a record readable that no barrier
would ever cover. The stream engine's disk-full test caught exactly this.

*Fix:* the batch paths stage every record and write before touching memory.
`queue_publish_batch` validates, allocates IDs, stages all, writes once, then
links the messages. `queue_consume_batch` selects into an array during the
scan, writes every delivery record in one call, then publishes the in-flight
state — under the queue lock throughout. `queue_ack_batch`/`queue_nack_batch`
already had a log pass and an apply pass; the log pass now stages and the
`sync_log` between them does the single write. **The single-record path still
writes before returning**, because its caller mutates immediately after.

**WAL space reservation, committed with `fdatasync`.**

*Obstacle:* writing inside a span pre-grown with `fallocate` skips the
size-update journal transaction, but a process exiting without a clean close
leaves a zero tail — so "the records run to the end of the file" stops holding
until the next open. `job_crash_matrix` showed a record appended past such a
gap being **silently truncated** instead of refusing the open.

*Fix:* replay proves the tail is safe before truncating. It truncates when the
remainder is all zeros (a reservation) or holds nothing that parses, and
otherwise refuses the open with `QUEUE_OPEN_TRAILING_RECORDS`, every byte
preserved. A torn tail still truncates exactly as before — matching a CRC-32
by chance is a 2⁻³² event — and the scan costs nothing when there is no tail.
`src/test_queue_crash.c` covers both outcomes.

The acknowledgement contract is unchanged throughout: publish, delivery and
ACK replies return only after a barrier covers their bytes in the file.

---

## 3. Reproducing

```sh
# Cross-server: cache, queue, stream
python3 src/xbench_kv.py     --systems kuttidb,redis --count 200000 --batch 256 \
  --fsync-ms 1000 --repeats 3 --kuttidb-binary /abs/path/kuttidb --jsonl /abs/new.jsonl
python3 src/xbench.py        --systems kuttidb,redis,nats,rabbitmq,kafka \
  --count 100000 --batch 256 --repeats 3 --kuttidb-binary /abs/path/kuttidb --jsonl /abs/new.jsonl
python3 src/xbench_stream.py --systems kuttidb,redis,kafka --count 100000 --batch 256 \
  --partitions 8 --repeats 3 --kuttidb-binary /abs/path/kuttidb --jsonl /abs/new.jsonl

# Reduce any of the above to medians (failed trials are listed, not dropped)
python3 src/xsummary.py /abs/new.jsonl

# Single-system verified queue workload, with exact ID/order/payload/ACK checks
python3 src/bench_queue_net.py 17411 200000 --binary /abs/path/kuttidb \
  --data-dir /abs/path/on/ssd --threads 1 --batch 256 --repeats 5 --jsonl /abs/new.jsonl

# Gate for any change to how records reach the disk
python3 src/test_queue_wal_compat.py OLD_BINARY NEW_BINARY
```

Rules that keep these results usable:

- The JSONL file must be new; previous evidence is never overwritten. Failed
  runs exit nonzero, record `verified: false` with a traceback, and retain
  their log and WAL directory.
- Linux tmpfs/ramfs data directories are refused — a durable benchmark on
  tmpfs measures nothing.
- For a before/after comparison, alternate the two sides trial by trial and
  report the ratio of medians. Do not run competing load generators at the
  same time on the one-vCPU host.
- Run correctness tests outside the benchmark window.
- Process-kill recovery tests validate that failure model only; they do not
  simulate a hypervisor or storage device losing power.

---

## 4. Reference detail

<details>
<summary><b>Device ceiling and WAL compatibility</b> — what the storage can do, and proof the format is unchanged</summary>

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

</details>

<details>
<summary><b>Engine-level and platform history</b> — macOS/ARM64 baselines and the 2026-08 milestone measurements</summary>

These are engine-level and single-machine measurements taken while the queue
and stream engines were built. They are kept because the milestone tables are
the comparison points for those changes; they are not cross-server results and
do not describe the reference VPS.

### Cache baseline — 2026-08-28 (macOS 15.3, ARM64, Apple Clang 17)

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

### Progress sample — 2026-08-29 (`make bench-quick`, same workload)

After the stream consumer generations, Prometheus endpoint, and fuzz suites:

| Clients | ops/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| 1 | 904,077 | 272 us | 454 us | 523 us |
| 2 | 2,598,685 | 204 us | 295 us | 314 us |
| 4 | 3,462,964 | 232 us | 528 us | 685 us |
| 8 | 1,869,718 | 390 us | 2,301 us | 2,415 us |

The four-client sample sits ~4% above the recorded baseline — no regression
over the gate.

### Linux-native sample — 2026-08-29 (`make bench-quick` equivalent in a container)

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

### Isolated single-op latency — 2026-08-29 (`make bench-single`)

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

### Queue and exchange routing — 2026-08-29 (`src/bench_exchange.py`)

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

### Queue engine, depth scaling — 2026-08-29 (`make bench-queue`)

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

### Queue engine, indexes and retention skip — 2026-08-29 (after group commit)

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

### Queue engine, consume scan hint — 2026-08-29

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

### Queue engine, group commit — 2026-08-29 (`make bench-queue`, CONC phases)

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

### Queue engine, batch operations — 2026-08-29 (`make bench-queue`, BATCH phase)

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

### Stream engine, retained-history scaling — 2026-08-29 (`make bench-stream`)

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

### Stream engine, group commit — 2026-08-29 (`make bench-stream`, CONC phase)

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

### Queue WAL checkpoint — 2026-08-29 (server-level, SIGKILL restart)

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

### Cache client-scaling cliff — 2026-08-29 (`src/bench_matrix.py --quick`, this machine)

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

</details>

<details>
<summary><b>Withdrawn: the 2026-09-12 cross-server scorecard</b> — what was wrong with it and why</summary>

The comparison in this section was withdrawn as a ranking. It is kept because
it documents the specific defects the current harnesses were built to avoid.

### Cross-server VPS measurements — 2026-09-12: comparison audit

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

#### Findings verified against commands, source and the VPS

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

#### Historical SSD observations, not a ranking

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

#### Reproducing the verified queue workload

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

</details>

<details>
<summary><b>Harness reference</b> — what each benchmark measures</summary>

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

</details>

---

## 5. Known gaps

Things this file does **not** establish, and open items on the engines.

| Gap | Detail |
|---|---|
| **Consume-side write volume** | KuttiDB writes a delivery *and* an acknowledgement record per message — 208 bytes per 100-byte message against RabbitMQ's 9. A coalesced range record would cut it; not implemented. Largest open item on the queue path. |
| **Single-message durable publish** | ~1,414 msgs/s against RabbitMQ's ~1,651 on the reference VPS (was ~950 before the space reservation). The remaining gap is not attributed; cross-WAL interference was tested and ruled out. |
| **Batched publish headroom** | Server CPU per message fell 3.3×, but throughput rose only 7% because one vCPU is shared with the load generator. The freed CPU has not been measured on a host where the server does not compete with its own client. |
| **Stream batch paths** | `stream_append_batch` already writes one record per batch, so it had nothing to gain — but `stream_commit_batch_if_generation` and the group offset reset still write one record per commit. |
| **Reservation is Linux-only** | `fallocate`; macOS and other platforms fall back to extending writes and do not get the commit-rate or tail-latency improvement. |
| **Windows** | No native benchmark tables; the Windows server build remains the documented platform blocker. |
| **Competitor coverage** | Cache compares only against Redis — no Memcached, Valkey or Dragonfly. The stream comparison has no NATS JetStream. |
| **Workload coverage** | One single-node workload per engine. No clustering, replication, multi-consumer fan-out or rebalancing, compaction, retention under write load, consumer lag under slow consumers, or a SIGKILL-recovery cost table for streams. |
| **Failure model** | Process-kill recovery validates that failure model only. Nothing here simulates a hypervisor or storage device losing power. |
