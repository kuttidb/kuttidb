# Migration and upgrade

## What KuttiDB is, and when to use something else

KuttiDB is a lightweight, crash-recoverable event cache: one binary holding a
fast TTL cache, reliable work queues, and durable partitioned streams, with
atomic cache-plus-event operations across them. It is not a drop-in
replacement for any of the systems below, and the honest "when to use
something else" list matters more than a feature checklist:

- **Redis** instead, when you need its rich data structures (lists, hashes,
  sorted sets, Lua) or its ecosystem modules. KuttiDB's cache is key/value
  with TTL, batch, and single-flight loading — deliberately smaller.
- **RabbitMQ** instead, when you need AMQP semantics, complex topologies, or
  mature multi-language broker ecosystem. KuttiDB queues implement the core
  work-queue contract (ACK/NACK/requeue, visibility, prefetch, confirms,
  dead-lettering) with a native protocol, not AMQP.
- **Kafka** instead, when you need its retention ecosystem, exactly-once
  transactions, log compaction today, or Kafka-ecosystem tooling. KuttiDB
  streams implement partitions, keyed routing, offsets, consumer groups with
  generations, retention, and replay — a focused subset, tested as
  at-least-once.
- **SQLite** instead, when you need queries, secondary indexes, or
  relational integrity. KuttiDB is not a database in that sense.

## Migrating workloads in

### From Redis (cache-aside)

- Map `SET key value EX ttl` to `put(key, value, ttl=...)`; semantics match
  (absolute expiry, lazy plus swept deletion).
- `MSET`/`MGET` map to `put_many`/`get_many`; batches are atomic per batch.
- Use `get_or_load` (or the single-flight opcodes) to replace ad-hoc lock-key
  stampede protection; leases replace Redlock-style patterns.
- Cache-aside invalidation through pub/sub maps to the atomic
  `put_and_publish`/`delete_and_publish` operations, which add the
  durability Redis lacks: after recovery, either both the cache mutation and
  the event exist, or neither does.

### From RabbitMQ (work queues)

- Declare durable queues with `queue_declare`; publish/consume with manual
  ACK. Visibility timeout replaces RabbitMQ's consumer ack-timeout; NACK with
  `requeue=false` routes to the declared dead-letter queue.
- Exchange types direct/fanout/topic with routing keys map conceptually; the
  topic matcher uses AMQP-style `*`/`#` words with a bounded matcher.
- What does not carry over: AMQP channels, per-message publisher confirms
  are per-publish, and priority queues are not implemented.

### From Kafka (event streams)

- Topics with 1–256 partitions, keyed partition selection, per-partition
  offsets, consumer groups with round-robin assignment and offset commits,
  retention by age/bytes, and replay map to `stream_*` operations.
- Differences to plan for: at-least-once (no transactions), no log compaction
  yet, in-memory retained set bounded by `max_bytes`, and no Kafka wire
  compatibility — use the native clients, or a bridge you write against them.

## Upgrade policy

See [DEPLOYMENT.md](DEPLOYMENT.md) for the full procedure. The essentials:

- Protocol `major.minor` with a capability bitset; majors are refused,
  higher minors are additive. Check feature bits before using new commands.
- WAL, snapshot, and shared-memory formats are versioned and CRC-checked;
  unknown record types fail closed during replay.
- Upgrade = clean stop → replace binary → start on the same data directory;
  crash during upgrade resumes through the tested recovery path.
- Do not run an older binary against data written by a newer one unless its
  release notes state the formats are unchanged.

## Backup and restore

Covered in [DEPLOYMENT.md](DEPLOYMENT.md): cold backup of the single data
directory, idempotent restore through tested WAL recovery, and why live
copies are not supported.
