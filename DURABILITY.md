# Durability

This document states exactly what KuttiDB promises to keep and what it
explicitly does not. There is no "zero data loss" claim anywhere in this
project: evictable cache entries are lost by policy, and single-node durability
does not protect against disk, machine, or node destruction.

## Data classes and acknowledgement points

| Data class | Acknowledgement point | Guarantee after a process crash or clean restart |
|---|---|---|
| Evictable cache entry (no WAL / periodic durability) | WAL write buffered or flushed per mode; may be evicted at any time | Best effort. Entries may be lost by policy (eviction) or by the durability mode; no survival claim. |
| Cache entry, `always` durability | fsync of the CRC-checked cache WAL record before the response | Present after recovery on a healthy single node. |
| Durable queue message (publish confirm) | fsync of the queue WAL publish record before the response | Present after recovery on a healthy single node. Delivery is at-least-once. |
| Durable ACK | fsync of the queue WAL ACK record before the response | The message is not removed before its ACK record is durable. |
| Exchange-routed copy | fsync of the target queue's publish record (single fsync per publish) | Same as a durable queue message, per target queue. |
| Atomic cache-plus-message operation | fsync of the cache WAL commit marker, which sits between the queue WAL prepare and commit records | Both sides are visible after recovery, or neither is. |

Queue and stream durable writes use group fsync: publish and delivery
records may become visible before the covering fsync lands (in one
sequential WAL, a record's durability implies every earlier record's), but
every response that acknowledges durable work is released only after an
fsync covers that work's sequence, and transform operations (durable ACK,
NACK requeue, dead-letter routing) still complete their fsync before their
mutation. Group fsync therefore changes fsync count, never acknowledgement
points.

All WAL records carry CRC32 checksums. Recovery accepts the valid prefix and
truncates a torn or corrupted tail; recovery is idempotent and is exercised by
tests that inject torn tails, corrupted checksums, SIGKILL during writes, and
disk exhaustion (`RLIMIT_FSIZE`).

## Managed idle shutdown

Managed idle shutdown uses the normal graceful server teardown: it stops
admitting new leases, closes listeners, snapshots the cache, flushes and syncs
the WALs, then releases ownership locks. It therefore preserves the same
acknowledgement points in this document as a signal-driven clean shutdown. A
process crash or forced kill remains a crash case and relies on normal WAL
recovery instead.

## Atomic cache-plus-message operations

`PUT_AND_PUBLISH`, `PUT_AND_ENQUEUE`, `DELETE_AND_PUBLISH`, and
`UPDATE_AND_EMIT` mutate the cache
and deliver queue messages under one commit marker. The durable sequence is:

1. Queue WAL: `TX_PREPARE` record (transaction id, pre-allocated message ids,
   message payload) — written and fsynced.
2. Cache WAL: transaction record `0x08` (the commit marker, carrying the cache
   mutation) — written, flushed, and fsynced. **This is the commit point.**
3. Queue WAL: `TX_COMMIT` record — written and fsynced; the reserved messages
   are materialized into their queues.
4. The response (with the commit id) is sent only after step 3.

`UPDATE_AND_EMIT` is the conditional variant: it commits only when the cache
key already exists. The existence check happens before anything is written, so
a missing key leaves both logs untouched and answers MISS. Its commit marker
carries a conditional sub-operation, and replay applies the value only when
the key exists at that replay point; a conditional marker whose key is missing
(an impossible live state, only reachable by crafting) keeps the transaction
uncommitted so reconciliation discards the prepared event — recovery still
shows both sides or neither.

Recovery behaves correctly at every boundary:

- prepare without the cache marker → discarded; neither side is visible;
- cache marker without `TX_COMMIT` → startup reconciliation materializes the
  reserved messages and writes the missing `TX_COMMIT` record;
- both records → replay materializes the messages directly, exactly once.

The commit id returned to the caller is the durable transaction id recorded in
both logs and can be used for tracing. Atomic operations always fsync both WALs
at their commit boundaries regardless of the `--durability periodic` mode, and
they are refused unless cache persistence and a healthy queue WAL are enabled
and every target queue is durable: mixed-durability transactions cannot satisfy
all-or-nothing recovery and are not degraded silently. Unroutable publishes
commit nothing (the cache mutation is refused with them) and are reported as
unroutable, not as errors.

WAL folding defers while any transaction is inside its commit window so a cache
commit marker can never be folded away before its queue side is
self-contained. An error response from an atomic operation after the commit
marker was written is the standard in-doubt window of any commit protocol: the
operation may still appear after recovery (at-least-once, never silently
dropped).

## What a single node does not protect against

- **Disk loss** — both WALs live on one volume.
- **Machine or node loss** — there is no replication.
- **Media corruption mid-log** — a CRC failure is treated as end-of-log; data
  after the corruption point on that volume is not recoverable.

Replication with a documented consensus design is a prerequisite for any
availability claim beyond "a healthy single node survives process crashes and
clean restarts". Until it exists, deployment documentation must describe
durable modes as single-node durability, not as zero data loss.

## Modes and configuration

- `--durability periodic` (default): cache WAL records are batched and fsynced
  every `FSYNC_MS`; acknowledged cache writes inside that window can be lost
  after an OS or power failure. Queue records and atomic operations always
  fsync at their own acknowledgement points.
- `--durability always`: every cache mutation is flushed and fsynced before its
  response.
- `--queue-wal -` disables durable queue declarations; atomic operations are
  refused while it is disabled.
