# Native queues (MVP)

KuttiDB's queue engine is separate from the cache engine. Queue messages never
enter the cache's memory budget or eviction path. The current native queue MVP
provides named queues, durable declarations, bounded depth, publish confirms,
one-message consume, manual ACK/NACK, requeue, visibility leases, redelivery
flags, expiration, dead-letter queues, retry limits, and crash recovery.

## Durability and delivery semantics

- A durable queue uses its own CRC-checked append-only log (`--queue-wal`, or
  `<cache-wal>.queues` when cache WAL persistence is configured).
- The WAL is bounded by a crash-safe checkpoint: when the log outgrows twice
  the live state plus 1 MiB, maintenance rewrites declarations, live
  messages (with delivery counts and retry deadlines), exchanges, bindings,
  consumers, and pending transactions using only existing record types,
  fsyncs and atomically renames the new log, and continues appending to it.
  Replay code is unchanged and recovery time tracks live state rather than
  history.
- Delivery tags resolve through a per-queue intrusive hash index, so ACK and
  NACK do not scan the queue; the index holds only in-flight tags, keeping it
  proportional to live in-flight state. Messages are doubly linked, so
  removal (including the queue tail) is O(1). The retention/visibility pass
  runs only when a TTL message exists or the earliest visibility deadline is
  due, so publish/consume cost does not grow with queue depth. Consume scans
  start at a maintained ready hint — every message before it is in-flight —
  so deliveries do not re-walk earlier in-flight deliveries; any requeue
  resets the hint, and a ready message whose retry delay has not elapsed is
  never jumped (it is delivered once its delay elapses).
- Recovery accepts the valid log prefix and truncates a torn or CRC-corrupted
  trailing record; it never treats a partial header as a clean end of file.
- Durable `DECLARE`, `PUBLISH`, delivery records, and `ACK` records are fsynced
  before their successful response. A successful durable publish is therefore a
  publisher confirmation for process-crash and clean-restart recovery on a
  healthy single node.
- Durable writes use group fsync: because the WAL is one sequential log, a
  record's durability implies the durability of every earlier record, so
  publish and delivery records may be written and applied under the store
  lock and their callers then wait — with the lock released — until an fsync
  covers their sequence. One thread at a time fsyncs the high-water mark for
  everyone; a sole writer fsyncs immediately under the lock, so low-load
  latency is unchanged. Transform operations (ACK, NACK/requeue, dead-letter
  routing) keep the strict contract ordering: their fsync completes before
  their mutation, inside the lock, exactly as before. A write or fsync
  failure marks the store failed permanently, wakes every waiter with an
  error, and all durable operations fail closed while non-durable queues
  keep working.
- A durable message is deleted only after its durable ACK record. A crash after
  delivery but before ACK leaves it in the log and makes it available again on
  restart. Because durable delivery records are written before handing a
  message to the client, the subsequent delivery is marked `redelivered`.
- Delivery is **at least once**. There is no exactly-once claim.
- A visibility lease makes an unacknowledged delivery available again after its
  deadline. Lease timing uses a monotonic process clock, so NTP or manual wall
  clock changes cannot expire a live delivery early. NACK with `requeue=true` makes it immediately available again;
  NACK with `requeue=false` durably discards it, or routes it to the queue's
  dead-letter queue when one is declared. Closing the connection that owns a
  delivery requeues it immediately. Delayed retries are supported.
- Dead-letter routing: a queue declared with a dead-letter queue and an
  optional delivery limit routes messages there when they are rejected
  (`NACK` with `requeue=false`), when they expire, or when the next delivery
  would exceed the limit. Routing is durable before the source removal: a
  crash between the two records leaves the message in the source queue for
  re-routing, so routing is at-least-once and never drops a message silently.
  A routed copy is a new message with a new ID and an empty delivery count;
  an expiry-routed copy loses its expiry so two dead-letter queues cannot
  ping-pong a message forever. If the dead-letter queue is full, the routing
  operation fails closed: the message stays in the source queue (or stays
  in-flight for a rejection) and the client receives an error.
- Single-node durability does not protect against disk, machine, or node loss.
  Replication remains required for that availability claim.

## Native protocol

All requests retain the existing framing:

```text
[op:1][queue_name_len:2][value_len:4][queue_name][value]
```

| Operation | Opcode | Value | Successful response |
|---|---:|---|---|
| Declare | `0x20` | `[durable:1][max_depth:8 LE]`, optionally followed by `[ext_len:2 LE][ext]` | normal OK |
| Publish | `0x21` | message bytes | `[OK][len=8][message_id:8 LE]` |
| Publish with TTL | `0x25` | `[ttl_ms:8 LE][message]` | `[OK][len=8][message_id:8 LE]` |
| Queue stats | `0x26` | empty | `[OK][len=16][depth:8 LE][inflight:8 LE]`, or MISS |
| Prefetch | `0x27` | `[count:4 LE]` | normal OK; zero disables the connection limit |
| Cancel consumer | `0x28` | empty | normal OK; requeues this connection's deliveries |
| Consumer register | `0x29` | key = consumer name, empty value | `[OK][len=8][owner:8 LE]`; registers (or heartbeats) a durable named consumer and binds the connection |
| Consumer unregister | `0x2A` | key = consumer name, empty value | normal OK; requeues the consumer's deliveries; MISS for an unknown consumer |
| Consume as consumer | `0x2B` | key = queue, value = `[consumer_len:2 LE][consumer][visibility_ms:8 LE]` | same response as Consume; MISS for an unknown consumer |
| Consume | `0x22` | `[visibility_ms:8 LE]` | `[OK][len][delivery_tag:8][message_id:8][redelivered:1][delivery_count:4 LE][message]`, or MISS |
| ACK | `0x23` | `[delivery_tag:8 LE]` | normal OK/MISS |
| NACK | `0x24` | `[delivery_tag:8 LE][requeue:1]` or `[delivery_tag:8 LE][requeue:1][retry_delay_ms:8 LE]` | normal OK/MISS |
| Publish batch | `0x2D` | `[count:4 LE]` then count × `[len:4 LE][message]` (1–256) | `[OK][count:4 LE]` then count × `[message_id:8 LE]`; the whole batch is capacity-checked first and nothing is written when it would exceed max depth |
| Consume batch | `0x2E` | `[max:4 LE]` (1–256) | `[OK][n:4 LE]` then n × `[delivery_tag:8][message_id:8][delivery_count:4 LE][redelivered:1][len:4 LE][message]`; empty queues return n=0 |
| ACK/NACK batch | `0x2F` | `[mode:1]` (0=ACK, 1=NACK requeue, 2=NACK discard/DLQ) + `[count:4 LE]` + count × `[delivery_tag:8 LE]` | `[OK][acked:4 LE]`; unknown or already-processed tags are skipped |

The batch operations (capability bit 12) exist to amortize round trips and
durability waits: one batch carries one group fsync for all of its records,
so a 256-message durable publish costs one fsync instead of 256. The
per-message durability contract is unchanged — a message is removed only
after the fsync covering its own ACK record, and delivery records are
durable before the batch response is sent. Delayed retry stays a
single-message operation; dead-letter routing inside a discard batch keeps
its per-message fail-closed behavior.

The optional Declare extension carries the dead-letter policy:
`ext = [dlq_name_len:2 LE][dlq_name][max_deliveries:4 LE]`, so
`ext_len = 6 + dlq_name_len`. `ext_len = 0` declares no policy. A queue
cannot be its own dead-letter queue, and a re-declaration that changes the
durable mode or the dead-letter policy is refused.

`delivery_tag` is a one-use lease token bound to the connection that consumed
the message; it is the only value accepted by ACK/NACK. `message_id` is the
stable identifier returned by publish and is informational on consume.
`delivery_count` starts at one and increments on every delivery, including
redelivery after restart. A delayed NACK requeue is durable: if the process
restarts before its due time, the message remains unavailable until that time.
When a dead-letter policy with `max_deliveries` is declared, the delivery
count is enforced natively and exhausted messages are routed to the
dead-letter queue instead of being delivered again.

Queue names are currently limited to 255 bytes and message values use the
existing server value limit (64 MiB by default). `max_depth=0` means unlimited;
otherwise publish returns an error when the queue already has that many ready
or in-flight messages. A nonzero publish TTL is an absolute expiry deadline
recorded with a durable message; expiry is recovered across restart. Expired
messages are discarded before capacity is checked, so they cannot retain a
bounded queue slot until the background sweep runs.

## Python example

```python
from kuttidb_client import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    db.queue_declare("jobs", durable=True, max_depth=10_000,
                     dead_letter_queue="jobs.dead", max_deliveries=5)
    message_id = db.queue_publish("jobs", b"resize:123")
    delivery = db.queue_consume("jobs", visibility=30)
    if delivery:
        try:
            process(delivery["value"])
            db.queue_ack("jobs", delivery["id"])
        except Exception:
            db.queue_nack("jobs", delivery["id"], requeue=True, delay=5)
```

## Current limits

Durable named consumers (`0x29`–`0x2B`) close the persistent-registration
gap: a registration maps a stable name to a stable owner token, survives
restart on durable stores, and changes disconnect semantics for its
deliveries — a dropped connection's in-flight work follows its visibility
deadlines instead of being requeued immediately, and reconnecting with the
same name keeps ownership and prefetch accounting. Delivery tags stay
one-use per process, so deliveries made before a restart are redelivered
(rather than ACKable with a stale tag). Locking uses three documented
domains — metadata (declarations, name lookups, transactions), one lock per
queue for message state, and a WAL lock for record writes and the group
fsync — acquired in that order; independent queues mutate in parallel. The
atomic cache-plus-message transaction deliberately holds the metadata lock
across its prepare/cache-marker/commit window. Neither is RabbitMQ/AMQP
compatibility.

Exchanges, topic routing, alternate routing, and unroutable reporting are
implemented as a separate routing layer above these queues and are specified
in [EXCHANGES.md](EXCHANGES.md).
