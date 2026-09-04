# KuttiDB architecture

KuttiDB is currently a compact, single-process cache server.  This document
describes the verified current system and the target shape for its evolution
into a cache, queue, and stream platform.  It deliberately separates facts
about the present implementation from planned capabilities.

## Managed local lifecycle

Managed mode is an opt-in, detached local service. `kuttidb ensure` creates or
validates an owner-only data directory, obtains a short bootstrap lock, and
starts a server only after a local endpoint probe confirms absence. The server
then holds sorted companion locks for its endpoint and every enabled
persistence path for its full lifetime, before recovery opens a WAL. A random
`instance.id` is persisted in the directory and returned through the
authenticated `SERVER_INFO` operation, so a managed client rejects an endpoint
belonging to a different directory.

The managed default is a Unix socket under the data directory. Explicit TCP is
restricted to literal IPv4 loopback endpoints. Connection acceptance is the
lease mechanism: after the final native connection closes, an idle deadline
requests the same graceful shutdown path used by `SIGTERM`; a new arrival
either cancels that deadline or starts a recovered successor after ownership is
released. Standalone commands retain their connect-only, operator-controlled
lifetime semantics.

## Current implementation (baseline: 2026-08-28)

The current server is a C11 executable with a binary little-endian protocol.
It provides cache operations, durable queues/exchanges, atomic cache-event
operations, single-flight coordination, and the first durable stream slice.
The existing KuttiDB protocol is version 1.2; prior opcode layouts remain
compatibility contracts. See [PROTOCOL.md](PROTOCOL.md).

```
TCP / Unix socket                         embedded trusted-local path
        |                                            |
        v                                            v
   kqueue event loops                         mmap'd Cache table
        |                                            |
        +------------------ Cache core --------------+
                            |
                 shards + TTL + slabs + eviction
                            |
                    WAL + snapshots + recovery
```

### Cache core

- A 256-shard hash table gives each shard its own lock.  Cache entries store
  the key and value in one allocation.  Small entries come from 16 KiB slabs;
  larger entries use direct allocations.
- Expirations are absolute Unix seconds, checked lazily on reads and swept by
  a maintenance thread.  Snapshot and WAL recovery preserve the expiration
  deadline rather than extending it after downtime.
- A configurable global memory budget triggers bounded random cache eviction.
  Eviction is deliberately cache-only semantics: evicted entries are not
  durable and must never be described as lossless.
- Network GET paths append directly into a connection response vector while
  holding the relevant shard lock, avoiding a temporary value allocation.

### Server, protocol, and persistence

- The server uses one native event loop per worker (up to four by default),
  not one thread per connection.  The platform boundary selects kqueue on
  macOS and epoll on Linux.  Connections have bounded input/output,
  request-size, batch-size, and client-count limits; `STATS` reports the active
  event backend for operational verification.
- The cache WAL is CRC-checked and is ordered under a process-wide WAL mutex.
  It supports normal mutations, deletes, and absolute-expiry puts.  Recovery
  accepts a valid prefix, stopping safely on a torn or corrupt trailing record.
- Snapshots are written from staged live records.  The WAL is folded after a
  successful snapshot and a graceful shutdown creates a snapshot then flushes
  and syncs the remaining log.
- `--durability periodic` is the default and can lose recently acknowledged
  cache writes after an OS or power failure.  `--durability always` flushes and
  fsyncs before acknowledgement.  Neither mode protects a single node from
  disk, machine, or site failure.
- The stream engine has its own CRC-checked WAL. Topic declaration, append,
  retention-trim metadata, and consumer-group offset commits are fsynced
  before acknowledgement. It restores a valid prefix after a torn or corrupt
  tail. It keeps retained records in memory and compacts the WAL into a
  crash-safe checkpoint once stale history becomes disproportionate.
- Stream durable operations use group fsync: the record is written and the
  in-memory mutation applied under the store lock, the caller then waits with
  the lock released until an fsync covers its sequence, and one syncer at a
  time fsyncs the shared high-water mark for everyone. Because the WAL is a
  single sequential log, a record's durability implies every earlier record's,
  so the acknowledgement point is unchanged. A write or fsync failure marks
  the store failed permanently and all durable operations fail closed.

### Security and locality

- TCP defaults to loopback.  Non-loopback binding requires an owner-only token
  file; token comparisons are constant-time.  Native OpenSSL TLS is optional.
- WALs, snapshots, Unix sockets, and embedded-region files are owner-only and
  opened without following symlinks where the platform supports it.
- Embedded mode exposes the cache table directly through a shared mmap file
  and uses recoverable owner-PID spin locks.  It is for mutually trusted local
  processes only.

The current embedded format (`CEMBv3`) contains only fixed-width offsets for
all shared heap, bucket, and entry links.  Each process holds a private cache
wrapper and may map the region at a different virtual address.  The direct
path is therefore ASLR-safe; a test reserves the creator's address in a child
process before attaching and verifies cross-process reads and writes at a
different mapping address.  Pointer-based `CEMBv2` regions are explicitly
rejected as incompatible.

### Current platform boundary

The checked-in project now has a CMake build as well as the simple Makefile.
The event backend is isolated in `src/platform.c`, with kqueue on macOS and
epoll on Linux.  macOS is verified locally; the checked-in native CI workflow
is configured to build and test Linux, but has not run from this workspace.
Unix sockets, `flock`, `mmap`, POSIX signals,
and process liveness are still POSIX-specific, so Windows has no server build,
Windows shared-memory implementation, or IOCP backend yet.  Docker and
Kubernetes deployment assets are available for Linux; Windows remains
outstanding.

## Target architecture

KuttiDB remains one executable, one configuration, and one data directory,
but the cache, queues, and streams must be logically separate engines.

```
                           KuttiDB process
        +-------------------------+--------------------------+
        |                         |                          |
        v                         v                          v
  Cache engine              Queue engine                Stream engine
 hash / TTL / eviction    messages / ACKs            topics / partitions
  best-effort allowed     retry / dead letter          offsets / retention
        |                         |                          |
        +---------------- shared services -------------------+
                  protocol | auth/TLS | metrics | recovery
                  WAL/segments | quotas | platform layer
```

The engines do **not** share a single item representation or eviction pool.
They do share the durable commit coordinator and operational/security surface.

### Storage and quotas

- Cache state uses its existing memory-first store, cache snapshot, and cache
  WAL.  Cache quota/eviction never applies to queues or streams.
- Durable queues use an append-only message log plus durable delivery and ACK
  records.  A durable message is retained until its durable acknowledgement or
  explicit retention/dead-letter policy permits deletion.
- Streams currently use an independent per-topic, per-partition in-memory
  record index backed by a CRC-checked stream WAL, monotonically increasing
  offsets, and separately persisted consumer-group commits. Retention removes
  old retained records; the WAL compacts stale history into a checkpoint.
- Accounting is distinct for cache data, queue messages, stream segments,
  consumer metadata, and persistence buffers.  Every queue, topic, connection,
  response buffer, and in-flight delivery has a configured upper bound.

### Durable commit coordinator

The differentiating cache-plus-event operations will use a versioned commit
record with a monotonically increasing commit ID:

1. Validate all operations and reserve bounded storage.
2. Append cache mutation and queue/stream publication as one uncommitted
   transaction in the durable log.
3. Flush at the configured durable acknowledgement point.
4. Append and flush the commit marker.
5. Apply/publish the operation and return the commit ID.

Recovery replays only transactions with a valid commit marker.  A crash before
the marker makes neither side visible; a crash after it restores both.  The
record format and fsync ordering are implemented and tested at every commit
boundary: the queue WAL prepares the delivery with pre-allocated message ids,
the cache WAL record is the fsynced commit marker, and a queue WAL commit
record materializes the messages; startup reconciliation finishes
transactions that died inside the window.  See [DURABILITY.md](DURABILITY.md).

### Queue semantics (native MVP)

The initial native queue implementation is at-least-once. Durable publishes,
delivery records, and ACK records are fsynced in a queue-specific WAL before a
successful response; durable writes use group fsync, where the fsync runs
outside the store lock and one syncer completes many waiters. Queue operation
cost is bounded away from depth: delivery tags resolve through a per-queue
intrusive hash index (proportional to in-flight count), messages are doubly
linked with O(1) removal, and the
retention/visibility pass only walks the
queue when a TTL message exists or
an in-flight visibility deadline is due. Locking uses three domains —
metadata (declarations and lookups), one lock per queue for message state,
and a WAL lock for record writes and grouped fsync — acquired in that
order; independent queues mutate in parallel, dead-letter routing takes the
DLQ lock with trylock, and the atomic transaction keeps the metadata lock
across its prepare/marker/commit window. A delivery remains durableA delivery remains durable until its durable ACK;
NACK/requeue and visibility timeout make it eligible for redelivery, which is
flagged to the consumer. A connection-close immediately requeues its owned
delivery; ACK/NACK requires the connection-bound one-use delivery token.
Queues may declare a dead-letter queue with an optional delivery limit:
rejected, expired, and limit-exhausted messages are routed there with durable
routing-before-removal ordering, and a full dead-letter queue fails closed
instead of dropping the message. Prefetch and persistent consumer
registration remain separate pending work. Exactly-once is not a goal
for the initial protocol. See [QUEUES.md](QUEUES.md).

### Exchange semantics (native routing)

Exchanges are a stateless routing layer above the queue engine: direct,
fanout, and topic matching, plus the unnamed default exchange that routes by
queue name. A publish resolves matching bound queues, deduplicates targets,
capacity-checks every target before writing, then appends one copy per target
and fsyncs the queue WAL once before confirming. Unroutable publishes are
reported (routed = 0) and counted, with optional single-hop alternate-exchange
routing. A per-exchange binding cap bounds fanout. Exchange and binding
declarations are durable in the queue WAL; routing is at-least-once and never
silently lossy. See [EXCHANGES.md](EXCHANGES.md).

### Stream semantics (native vertical slice)

Streams are multi-subscriber, append-only partition logs. Ordering is only
within a partition. Topics have 1–256 partitions, deterministic keyed
selection, replay from an offset, age/byte retention, and persisted
per-topic/per-partition group offsets. Consumer offsets are independent from
records; fetch and commit are separate, so consumption is at-least-once.

Groups have lease-based membership and deterministic round-robin assignment;
clients rejoin to heartbeat and refresh assignments, and disconnects remove
members immediately. Cooperative rebalance notifications/draining, batch wire
operations, segment log compaction, consumer lag metrics, and atomic
cache-plus-stream commits remain future work. See [STREAMS.md](STREAMS.md)
for the exact current contract.

### Cross-platform local transport

The shared-memory format stores offsets, never process pointers. A trusted
same-host client can use this direct path safely under ASLR. The Python
`LocalKuttiDB` client automatically falls back at connection setup to a Unix
socket when configured, then TCP or TLS. It never replays an in-flight direct
write over a socket. Named-pipe fallback and equivalent selector APIs for the
other native clients remain Windows/future-client work.
Event polling now has kqueue and epoll implementations; files, locks, timers,
process liveness, sockets, and shutdown still need Windows implementations.

## Durability claims

| Data class | Intended acknowledgement point | What is and is not guaranteed |
|---|---|---|
| Evictable cache | configurable; periodic by default today | Cache entries may be evicted and may be lost under periodic durability. |
| Durable queue message | durable append/commit flush | Survives a process crash and clean restart on a healthy single node; no claim against disk or machine loss without replication. |
| Durable queue ACK | durable ACK record flush | The message is not removed merely because an ACK reached memory. |
| Stream record / offset commit | durable append/commit flush | Same healthy-single-node process-crash guarantee; no replication claim initially. |
| Atomic cache + event | shared durable commit marker flush | Recovery exposes both sides or neither side. |

Replication is required before any claim of tolerating disk, machine, or node
failure.  Until then, deployment documentation must clearly call durable modes
single-node durability, not zero data loss.

## Compatibility policy

- Existing cache opcodes and their v1.1 layouts remain unchanged.
- Version 1.2 adds an authenticated `CAPABILITIES` handshake: clients reject
  an incompatible major version and can require advertised feature bits.
- New features use negotiated protocol capabilities and distinct cache, queue,
  exchange, and topic namespaces.
- The native protocol comes before Redis, AMQP, or Kafka compatibility layers.
  No such compatibility claim is valid until separately implemented and tested.
- Format changes to WAL, snapshots, and shared memory need explicit versions,
  migration tests, and a documented rollback story.

### Compatibility-adapter evaluation (Milestone 8)

Each adapter was evaluated against two rules: it must not weaken KuttiDB's
lightweight single-binary design, and it must never answer a wire dialect
whose semantics it does not actually implement.

- **Redis RESP.** A read/write subset (`GET`, `SET`, `DEL`, `TTL`, batched
  variants) maps directly onto the native cache opcodes and is technically
  small. It is still deferred: RESP clients expect Redis list/hash/pubsub
  semantics, silent subsets breed broken integrations, and the honest
  alternative — advertising the native binary protocol — already works in
  five shipped client languages. Re-evaluate only on demonstrated demand,
  implemented as an opt-in gateway process, never inside the core binary.
- **AMQP 0-9-1.** The durable-queue model (ACK/NACK, prefetch, confirms,
  exchanges, DLQs) maps well onto KuttiDB queues, but the framing/method
  machinery, channels, and per-connection state are a large, separately
  fuzzed surface. A full broker gateway would dominate the codebase's risk
  budget while native Python clients already cover the documented work-queue
  and routing patterns. Defer; revisit only as an out-of-process gateway.
- **Kafka wire protocol.** Dozens of RPC APIs plus the Kafka group-coordination
  protocol make in-binary compatibility unrealistic for a lightweight
  product. KuttiDB's native streams deliberately implement the useful
  semantics (partitions, keyed routing, offsets, generation-based consumer
  groups, retention, replay) instead. Integration with Kafka-ecosystem tools
  should go through a bridge using the native client, clearly documented as
  a bridge — never as "Kafka-compatible".

Compatibility claims in documentation remain limited to what is implemented
and tested; the current honest claim is: native protocol v1.3, no Redis,
AMQP, or Kafka wire compatibility.
## Optional Management API

The Management API is a small isolated HTTP module inside the server binary.
When `--admin-bind` is absent it starts no listener or thread. When enabled it
uses bounded snapshots and public engine adapters, then releases engine locks
before network output. Mutations pass through the audit and idempotency
boundary and do not share the data-protocol authentication token.
