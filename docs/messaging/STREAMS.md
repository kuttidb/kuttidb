# Streams

KuttiDB streams are native, append-only partition logs. They are designed for
replayable events, not work distribution: reading a record does not remove it.
Ordering is guaranteed only within one partition.

## Current native API

- `stream_declare(topic, partitions, max_bytes, max_age)` creates a durable
  topic. Topic declarations are immutable and idempotent.
- `stream_append(topic, value, key=..., partition=...)` appends one durable
  record. Explicit partitions take precedence; otherwise an FNV-1a hash of a
  non-empty key selects a partition, and keyless records use partition zero.
  Both arbitrary binary key and body are retained through recovery and
  checkpoint compaction.
  The response includes the selected partition and its monotonically
  increasing offset.
- `stream_fetch(topic, partition, offset, max_records)` replays records from
  an offset. The current retained base is returned when an older requested
  offset has expired.
- `stream_commit(topic, group, partition, offset)` durably records a group's
  next offset. Group offsets are independent for every topic/partition.
- `stream_group_offset(topic, group, partition)` reads the saved offset.
- `stream_group_lag(topic, group, partition)` returns the records between the
  committed next offset and the partition end offset.
- `stream_group_join(topic, group, lease=...)` joins or heartbeats a native
  consumer group and returns this connection's assigned partitions plus the
  group's membership generation. Members
  are ordered by server connection id; partitions are assigned round-robin.
  Membership is lease-based (1–60 seconds), is removed immediately on client
  disconnect, and is swept on expiry. A group commit is accepted only from a
  live member assigned that partition.
- `stream_group_leave(topic, group)` releases one group's membership
  gracefully after draining in-flight work; the group rebalances immediately
  instead of waiting for the lease to expire.

Each append and group-offset commit is added to a CRC-checked stream WAL and
`fsync`ed before success is returned. Startup replays the valid WAL prefix and
truncates a torn or checksum-corrupt tail. This protects process crashes and
clean restarts on a healthy single node. It does not protect disk, machine, or
node loss; replication is not implemented.

## Retention and delivery

`max_bytes` and `max_age` evict the oldest records within a topic. This is
stream retention, not cache eviction, and does not consider consumer commits:
consumers must stay within the retention window. The in-memory retained set is
bounded. Its WAL is compacted into a crash-safe checkpoint once history grows
beyond the current retained state, so expired records and superseded group
commits do not grow disk use without bound. A single accepted record may still
be as large as the configured server value limit; size capacity must account
for that operationally.

Fetching and committing are deliberately separate, which makes the initial
delivery model at-least-once: process a record, then commit the next offset;
a crash in between causes replay. Do not claim exactly-once processing.

Membership and assignment are native but deliberately minimal: consumers
heartbeat by joining again and refresh their assignment after a join, leave, or
lease expiry. The join response carries a membership generation that increases
on every membership change (join, graceful leave, disconnect, lease expiry) and
is unchanged by a plain heartbeat, so a member that observes a new generation
knows a rebalance happened even when its own assignment did not change. A
member that loses partitions must stop committing them — the server refuses
commits from members that no longer own the partition — finish in-flight work
for the partitions it keeps, and call `stream_group_leave` when it shuts down.
There is still no pushed notification channel or incremental cooperative
assignment, so applications must poll the heartbeat and tolerate replay when
ownership changes.

Storage model and its limits: every retained record lives in memory in front
of a single CRC-checked WAL per store, and retention `max_bytes` therefore
bounds live stream memory as well as disk use. Retention cleanup is
interruption-safe — a crash during trimming or WAL checkpoint compaction can
never lose the valid prefix, only delay it. This is a correctness-complete
model for bounded topics; it is not segmented log storage. Segment files
would let retention drop whole files without compaction work and keep
retained records out of the checkpoint path; until that exists, plan capacity
for the checkpoint compaction (it rewrites retained state under the store
lock) and size `max_bytes` to what the process should hold in RAM.

Compaction eligibility and retention accounting are incrementally maintained,
so no durable mutation ever scans the retained history to decide whether
housekeeping is due. The store keeps a running live-checkpoint byte estimate
(topic declarations, retained record headers, and group commit footprints)
and per-topic retained-record counters, updated at the shared mutation points
(append, batch append, trim, declare, group creation). Retention persists one
coalesced trim boundary per affected partition per pass rather than one trim
record per evicted record; replay applies trim boundaries monotonically, so
the recovered state is identical. Retention remains a ceiling, not an
acknowledgement: a crash before a trim boundary reaches the WAL replays
slightly more history, and the startup retention pass re-trims it. Fetching
from a lagging offset still walks the retained prefix before it — segment
files with sparse offset indexes are the planned fix.

Durability acknowledgement with group fsync: all writers share one
sequentially ordered WAL, and a record's durability implies the durability of
every earlier record. A durable operation therefore writes its record and
applies its in-memory mutation under the store lock, then waits — with the
lock released — until an fsync covers its record before the caller sees
success. One thread at a time performs that fsync for the whole high-water
mark, so under concurrency a single fsync completes many operations while a
sole writer still fsyncs immediately (low-load latency is unchanged). A write
or fsync failure marks the store failed permanently and wakes every waiter
with an error: durable work fails closed. A crashed compaction cannot weaken
this: the compacted checkpoint is fsynced and atomically renamed, and once
published it is the durability point for everything it contains.

## Wire protocol

All commands use the standard `[opcode][topic_len:u16][value_len:u32][topic]
[value]` envelope and little-endian integers.

| Opcode | Command | Value | Success response |
|---:|---|---|---|
| `0x60` | declare | `partitions:u32, max_bytes:u64, max_age_ms:u64` | standard OK |
| `0x61` | append | `partition:u32, key_len:u16, key, message` (`0xffffffff` = select) | `partition:u64, offset:u64` |
| `0x62` | fetch | `partition:u32, offset:u64, max_records:u32` | `count:u32, records[offset:u64,len:u32,value]` |
| `0x63` | commit | `group_len:u16, group, partition:u32, next_offset:u64` | standard OK |
| `0x64` | group offset | `group_len:u16, group, partition:u32` | `offset:u64` |
| `0x65` | group join/heartbeat | `group_len:u16, group, lease_ms:u32` | `count:u32, partition:u32[], generation:u64` |
| `0x66` | group lag | `group_len:u16, group, partition:u32` | `lag:u64` |
| `0x67` | append batch | `partition:u32, count:u32, records[key_len:u16, value_len:u32, key, value]` | `count:u32, results[partition:u64,offset:u64]` |
| `0x68` | group leave | `group_len:u16, group` | standard OK |
| `0x6b` | commit batch | `group_len:u16, group, count:u32, entries[partition:u32, offset:u64]` (1–256) | standard OK; entries apply in order and share one group fsync |
| `0x6c` | keyed fetch | `partition:u32, offset:u64, max_records:u32` | `count:u32, records[offset:u64,key_len:u16,value_len:u32,key,value]` |

`0x62` intentionally keeps its original body-only response for legacy
clients. Clients that negotiate capability bit 14 use `0x6c` to receive every
retained binary key as well as its body.

Topic and group names are limited to 255 bytes, partitions to 256, and one
fetch to 1,024 records and the server's configured batch-byte limit. If the
next eligible record cannot fit in that byte budget, fetch fails rather than
allocating an oversized response buffer. Stream messages follow the server
value-size limit.

Batch append accepts at most 1,024 records and is encoded as one fsynced
stream-WAL record, so a crash cannot retain just a prefix of an acknowledged
batch. The entire durable record is capped at 64 MiB.
