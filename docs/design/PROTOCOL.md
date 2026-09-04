# KuttiDB protocol (v1.8)

Binary, little-endian, request/response over TCP (or Unix socket). Pipelining is
safe; batch ops group many operations into one round trip. When authentication
is configured, `AUTH` must be the first request on every connection.

## Single ops

Request: `[op:1][klen:2][vlen:4][key][value]`

| op | name   | sends value | response                 |
|----|--------|-------------|--------------------------|
| 01 | PUT    | yes         | `[status:1][vlen:4][value]` |
| 02 | GET    | no          | `[status:1][vlen:4][value]` |
| 03 | DELETE | no          | `[status:1][vlen:4]` (vlen=0) |
| 04 | STATS  | no          | `[status:1][vlen:4][json]`  |
| 05 | PUT_TTL| yes         | `[status:1][vlen:4]`        |
| 06 | AUTH   | token in key field | `[status:1][vlen:4]` |
| 09 | HEALTH | no          | `[status:1][vlen:4]`        |
| 0A | CAPABILITIES | `[major:u16,minor:u16]` | `[major:u16,minor:u16,features:u64]` |
| 0C | SERVER_INFO | empty | managed instance identity and lifecycle metadata |

`PUT_TTL` request layout: `[05][klen:2][vlen:4][ttl_ms:4][key][value]`.
`ttl_ms = 0` behaves like PUT (no expiry). Expired records behave as misses
and are deleted lazily on read plus by a background sweep.

Status: `0x00` = OK/HIT, `0x01` = MISS, `0x02` = ERROR.
GET miss: status `0x01`, vlen `0`. DELETE miss: status `0x01`.

`AUTH` layout is `[06][token_len:2][zero:4][token]`. The server compares the
token in constant time and closes the connection after a failed attempt. AUTH
is not encryption; enable native TLS, or use a private Unix socket/VPN, when
traffic can be observed.

`HEALTH` uses an empty key and value. It is subject to the normal AUTH gate
when authentication is configured, and returns OK only while the cache, queue,
and stream persistence engines are all writable.

`CAPABILITIES` makes protocol evolution explicit. Version 1 clients send
`[1:u16, supported_minor:u16]` after AUTH (when enabled). The server returns
MISS for a different major version; otherwise it returns its version and a
feature bitset: cache (`bit 0`), queues (`1`), exchanges (`2`), atomic
cache-plus-message (`3`), single-flight (`4`), streams (`5`), stream batch
append (`6`), health (`7`), stream group generations (`8`), durable named
queue consumers (`9`), conditional atomic update (`10`,
`UPDATE_AND_EMIT`), stale-while-revalidate (`11`,
`PUT_SWR`/`GET_OR_REFRESH`), queue batch publish/consume/ACK (`12`), stream
offset commit batch (`13`), and keyed Stream fetch (`14`, `0x6c`). A higher minor
version is backward-compatible unless the client requires an absent feature
bit.

`SERVER_INFO` is authenticated like every other native request. It returns
`[format:1=1][instance_id_len:1=32][instance_id:32 ASCII lowercase hex]`
`[lifecycle:1 (0 standalone, 1 managed-idle)][started_unix_ms:8][pid:8]`
`[transport:1 (0 TCP, 1 Unix)]`. Servers without a named data directory return
MISS. Managed SDKs use this only to prove they reached the expected local
instance; it intentionally never exposes paths, arguments, or credentials.

## Error reporting

Every response carries one of three statuses: `0x00` OK/HIT, `0x01` MISS,
`0x02` ERROR. The documented failure conditions map onto these signals plus
the connection state, so a client can distinguish each one:

| Condition | Wire signal |
|---|---|
| Cache GET/DELETE miss | status `0x01`, vlen 0 |
| Unroutable publish (exchange, `0x40`/`0x42`/`0x43`) | status `0x01`, nothing committed |
| Queue/stream/topic not found | `queue_stats`/`stream_group_lag`/`stream_group_offset` answer `0x01`; other commands answer `0x02` |
| Queue full (bounded depth backpressure) | status `0x02`, nothing confirmed |
| Durability failure (any WAL fail-closed) | status `0x02`; the failing engine latches its `*_wal_failed` flag, `HEALTH` answers `0x02`, `/ready` answers 503 |
| Persistence disabled (`-` WAL) for an atomic op | status `0x02`, nothing committed |
| Authentication failure | status `0x02`, connection closed (constant-time compare, failures counted in STATS) |
| Protocol major mismatch | `CAPABILITIES` answers `0x01` before any feature command is attempted |
| Malformed frame, oversized value/batch, opcode/payload-shape violation | `0x02` or connection close, per the parser gate |
| Stream offset beyond the retained range | fetch answers `0x00` with count 0 (replay is position-based, not an error) |
| Lease conflict (single-flight) | not an error: `GET_OR_CLAIM` answers state `wait` / `GET_OR_REFRESH` `holder=0` |

`0x02` is deliberately fail-closed: a client must treat it as "the durable
effect of this request did not happen" (or, for in-doubt commit windows, as
"unknown until recovery reconciles" — see [DURABILITY.md](DURABILITY.md)).
The status set is kept at three values so every native client maps it onto
one exception type plus one miss type; richer condition detail belongs in
STATS/metrics counters, which name each failure mode explicitly.

`klen` is `uint16`, `vlen` is `uint32`, both little-endian.
Max key: 65535 bytes. Values: arbitrary binary, with a server-configured size
limit (64 MiB by default).

## PUT_BATCH

Request: `[0x11][pad:2][count:4] ( count * [klen:2][vlen:4][key][value] )`
Response: `[status:1]` — one status for the whole batch (0x00 = all applied).

## PUT_BATCH_TTL

Request: `[0x13][pad:2][count:4] ( count * [klen:2][vlen:4][ttl_ms:4][key][value] )`
Response: `[status:1]`. Per-item TTL in milliseconds; 0 = no expiry.

## GET_BATCH

Request: `[0x12][pad:2][count:4] ( count * [klen:2][key] )`
Response: `[count:4] ( count * [status:1][vlen:4][value] )`

Misses report status `0x01` with vlen `0`. Max count: 65536.

## Queue MVP

Native durable queue commands use opcodes `0x20`–`0x2F`. Their binary layouts,
durability point, ACK/NACK, visibility, redelivery, dead-letter, retry
semantics, and the batch operations (`0x2D`–`0x2F`, capability bit 12) are
specified in [QUEUES.md](../messaging/QUEUES.md). They are native KuttiDB
commands, not AMQP.

## Exchanges and routing

Native exchange commands use opcodes `0x30`–`0x33` and share the request
framing. Direct, fanout, and topic matching, the unnamed default exchange,
alternate-exchange routing, unroutable reporting, binding limits, and the
routing durability point are specified in [EXCHANGES.md](../messaging/EXCHANGES.md).
Publish responses report the number of routed copies; an unroutable publish
answers MISS, and a failed publish (full target queue, persistence failure)
answers ERROR without confirming anything.

## Atomic cache plus messaging

Opcodes `0x40`–`0x43` commit a cache mutation and queue delivery together
under one durable commit id; their value layouts, durability point, and
crash-recovery guarantees are specified in [DURABILITY.md](DURABILITY.md).
Responses are `[OK][len=12][tx_id:8 LE][routed:4 LE]`; an unroutable publish
answers MISS and commits nothing.

`0x43 UPDATE_AND_EMIT` is conditional: its request layout matches `0x40`
(`[xlen:2][exchange][rklen:2][rkey][ttl_ms:4][value]`), but the key must
already exist. A missing (or deleted) key answers MISS and commits nothing —
no cache change and no event. The commit marker carries a conditional
sub-operation, and replay applies it only when the key exists at that point,
so recovery keeps both sides or neither even for a crafted marker; a marker
whose key is missing discards the prepared event during reconciliation.

## Inventory and inspection

Three read-only snapshots support operational tooling (`kuttidb-cli queues`,
`kuttidb-cli topics`, `kuttidb-cli groups`; Python `queue_list()`,
`stream_list()`, `stream_group_list()`):

| Opcode | Name | Request | Response |
|---|---|---|---|
| `0x2c` | Queue list | empty key/value | `[OK][n:2] ( n * [nlen:2][name][depth:8][inflight:8] )` |
| `0x69` | Stream list | empty key/value | `[OK][n:2] ( n * [tlen:2][topic][partitions:4][records:8][bytes:8] )` |
| `0x6a` | Stream group list | empty key/value | `[OK][n:2] ( n * [tlen:2][topic][glen:2][group][generation:8][members:4] )` |

Each response is bounded at 256 entries and taken under the owning engine's
lock; expired group members are reaped before their group is reported. The
labeled Prometheus series (`kuttidb_queue_depth{name}`, `kuttidb_topic_*`)
cover the same data for scrapers.

## Anti-cache-stampede (singleflight)

Opcodes `0x50`–`0x54` implement native single-flight loading so a hot missing
key is loaded once no matter how many clients miss simultaneously. The
response envelope is `[OK][len][state:1][value]` with states: `0` value,
`1` claimed (caller must load), `2` wait, `3` negative, `4` released,
`5` timeout, `6` lost. `GET_OR_REFRESH` (0x54) extends the envelope with a
holder byte: `[OK][len][state:1][holder:1][value]` — `holder=1` means this
caller owns the revalidation lease and must reload.

| Operation | Opcode | Name | Value | Response |
|---|---:|---|---|---|
| Get or claim | `0x50` | key | `[lease_ms:4 LE]` | envelope: hit / negative / claimed / wait |
| Wait for key | `0x51` | key | `[timeout_ms:4 LE]` | deferred envelope: value / negative / released / timeout / lost |
| Put and release | `0x52` | key | `[ttl_ms:4 LE][negative:1][value]` | normal OK; wakes every waiter of the key |
| Release claim | `0x53` | key | empty | normal OK; waiters receive `released` |

Semantics and limits:

- A claim is an in-memory lease for one key. A second `GET_OR_CLAIM` while a
  live lease exists answers `wait`. A crashed loader cannot strand a key: the
  lease expires (≤ 60 s) and the key becomes claimable again.
- `WAIT_FOR_KEY` never blocks an event loop: the response is deferred and
  delivered by the waiter's own loop when a loader finishes. A wait registered
  after the loader already finished is answered immediately. Timeouts are
  enforced server-side (deadline sweep; ≤ 60 s, one pending wait per
  connection, at most 256 waiters per key, waiter total bounded by the
  connection limit). Because the response is deferred, clients must not rely
  on response order across a `WAIT_FOR_KEY`.
- Any successful `PUT` of the key also wakes its waiters with the value, so
  plain writers participate in single-flight.
- Negative answers (`PUT_AND_RELEASE` with `negative=1`) are cached in a
  bounded in-memory registry (4096 entries, default TTL 60 s), are invisible
  to plain `GET`, and expire like TTL data.
- Claims, waiters, and negative answers are ephemeral coordination state:
  memory-only, never persisted, and cleared by a restart. `STATS` reports
  `claims`, `singleflight_waiters`, and `negatives`.

## Stale-while-revalidate and refresh-ahead

`PUT_SWR` (`0x0b`) stores a value exactly like `PUT_TTL` —
request `[0x0b][klen:2][vlen:4][ttl_ms:4][stale_ms:4][refresh_ms:4][key][value]`
(metadata precedes the key, `vlen` counts the value only) — and additionally
retains a bounded in-memory stale copy until `ttl + stale`. Plain `GET`
semantics never change: once the TTL expires, plain GETs miss.

`GET_OR_REFRESH` (`0x54`, request `[lease_ms:4]`) is the SWR-aware read:

- fresh hit → state `0` (value attached);
- fresh hit with the refresh-ahead window elapsed → state `8` with the value;
  the caller named by `holder=1` should revalidate;
- expired key with a retained copy → state `7` (stale value attached)
  immediately, without waiting; `holder=1` names the one caller that must
  revalidate via `PUT_SWR` (or `PUT_AND_RELEASE`);
- expired key with no retained copy → the `0x50` claim/wait machinery;
- a successful plain `PUT`, `PUT_TTL`, `PUT_AND_RELEASE`, or `DELETE`
  supersedes the retained copy; a delete is a fresh state change.

The stale registry is bounded (4,096 entries / 32 MiB, windows capped at
7 days; full registry → SWR degrades to claim/wait), memory-only, never
persisted, never visible to plain `GET`, and cleared by a restart — the same
ephemeral-coordination category as claims and negatives. `STATS` reports
`stale_entries`, `stale_serves`, and `refresh_serves`; the metrics endpoint
exposes `kuttidb_stale_entries`, `kuttidb_stale_serves`, and
`kuttidb_refresh_serves`. `PUT_SWR` requires `ttl_ms > 0` and `stale_ms > 0`
and refuses longer windows.

## Partitioned streams

Native stream commands use opcodes `0x60`–`0x6C`. They provide durable topic
declarations, append-only partition offsets, replay fetch, persisted per-group
offsets, lease-based group membership/partition assignment, batch append
(`0x67`), batch offset commit (`0x6B`, capability bit 13), and an additive
key-preserving fetch (`0x6C`, capability bit 14). Since v1.3 the
group-join response carries a membership generation after the assignment, and
`0x68` performs a graceful group leave. Layouts and current at-least-once
semantics are in
[STREAMS.md](../messaging/STREAMS.md). They are native KuttiDB commands, not Kafka wire
compatibility.

## Client support matrix

| Feature | Python | Node.js | Go | Java | Rust | CLI |
|---|---|---|---|---|---|---|
| put / get / delete / TTL / STATS | yes | yes | yes | yes | yes | yes |
| KV batches (`0x11`–`0x13`) | yes | yes | yes | yes | yes | yes |
| Health and capability discovery | yes | yes | yes | yes | yes | yes |
| Queues and durable consumers | yes | yes | yes | yes | yes | yes |
| Queue inventory and batches | yes | yes | yes | yes | yes | yes |
| Exchanges and routing | yes | yes | yes | yes | yes | yes |
| Atomic cache plus messaging | yes | yes | yes | yes | yes | yes |
| Single-flight and SWR | yes | yes | yes | yes | yes | no |
| Streams and consumer groups | yes | yes | yes | yes | yes | yes |
| Stream inventory, keyed fetch, and batches | yes | yes | yes | yes | yes | yes |
| Managed local lifecycle | yes | yes | yes | yes | yes | n/a |
| Pool / concurrency | thread per conn | built-in pool | built-in pool | built-in pool | `Pool` | n/a |
| Shared-memory embed | ctypes | no | cgo | no | no | n/a |

## Server CLI

```
./kuttidb [PORT [WAL [FSYNC_MS [UNIX_PATH [MAX_MEM_MB [EMBED_PATH]]]]]] \
  [--bind IPv4] [--auth-file PATH] [--tls-cert PATH --tls-key PATH] \
  [--max-value-mb N] [--max-batch-mb N] [--max-clients N] \
  [--threads N] [--durability periodic|always] [--embed-region-mb N] \
  [--queue-wal PATH|-] [--stream-wal PATH|-]
```
- `WAL` = path for the write-ahead log (writes `-` to disable persistence)
- `FSYNC_MS` = background fsync interval, `0` disables fsync
- `--durability periodic` (default) batches WAL writes and syncs them at
  `FSYNC_MS`; acknowledged writes inside that window can be lost after an OS
  or power failure. `always` flushes and fsyncs before acknowledging a single
  mutation, or once before acknowledging a complete batch.
- `UNIX_PATH` = optional extra listener on a Unix domain socket
- `MAX_MEM_MB` = optional memory budget; records are randomly evicted
  (memcached-style) when writes push the store over the budget

Managed local startup uses `kuttidb ensure --data-dir ABS_PATH --listen
unix:ABS_PATH/kuttidb.sock` by default. The advanced form accepts only
`tcp:127.x.x.x:PORT`; it never resolves host names or starts a non-loopback
listener. `--lifecycle managed-idle` is internal launcher configuration, while
ordinary server commands remain standalone.
- `--bind` defaults to `127.0.0.1`. A non-loopback bind is refused unless
  `--auth-file` is configured.
- `--auth-file` must be a regular, server-owned `0600` file containing a
  1..1024-byte token. A final newline is ignored.
- `--tls-cert` and `--tls-key` enable TLS on every TCP listener. The certificate
  is a PEM chain and the private key must be server-owned with no group/other
  permissions. TLS 1.2 is the minimum; TLS 1.3 is preferred. Unix sockets are
  unchanged because filesystem permissions are their transport boundary.
- `--max-value-mb`, `--max-batch-mb`, and `--max-clients` bound resource use;
  defaults are 64 MiB, 64 MiB, and 1024 clients.
- `--threads` selects 1..64 event loops. The default is the online CPU count
  capped at four, which avoids context-switch overhead on small machines.
- `--embed-region-mb` selects the sparse embedded mapping size (minimum 16
  MiB, default 1024 MiB).
- `--queue-wal` selects the durable native queue WAL. By default it is
  `<WAL>.queues` when cache persistence is enabled; `-` disables durable queue
  declarations while still allowing non-durable queues.
- `--stream-wal` selects the durable native stream WAL. By default it is
  `<WAL>.streams` when cache persistence is enabled; `-` disables stream topic
  declarations. Stream commands are durable-only in this release slice, so
  they never quietly fall back to volatile storage.

`STATS` reports live record/table bytes as `mem_bytes`, allocator-owned bytes
as `allocated_bytes`, plus `wal_failed`, `event_loops`, `event_backend`, and
`durability`, plus `queues`, `queue_depth`, `queue_inflight`,
`queue_redeliveries`, `queue_deadlettered`, and `queue_wal_failed`, plus
`exchanges`, `exchange_bindings`, and `exchange_unroutable`, plus
`stream_topics`, `stream_partitions`, `stream_retention_bytes`, and
`stream_wal_failed`.
`event_backend` is `kqueue` on
macOS and `epoll` on Linux.
Once `wal_failed` becomes 1, persistent mutations fail closed rather than
being acknowledged without a durable log record.

The WAL is an internal, CRC-checked stream. New TTL records use operation
`0x07` and store an absolute Unix expiry second, so downtime never extends a
TTL. Recovery remains compatible with legacy relative-TTL `0x05` records.

Native TLS is built when OpenSSL development files are found (`make TLS=1`, the
default). Use `./kuttidb --features` to check the binary. `make TLS=0`
produces a dependency-free plaintext/Unix-socket build.

## Embedded shared-memory mode

Start the server with an embed region path to share its live table with
same-host clients over memory instead of sockets:

```
./kuttidb PORT WAL FSYNC_MS UNIX_PATH MAX_MEM_MB /path/to/db.embed
```

The region is a configurable sparse file containing the whole table (shards,
locks, offset heap). Embed clients attach via `libkuttidb_embed.dylib` and access
records without socket syscalls; WAL-enabled writes still perform persistence
I/O. Cross-process shard locks use lock-free owner words that can recover when
an attached process dies;
the server's TTL sweeps, eviction, snapshots and event loops operate on the
same table as if the writes came over the network.

Durability: embed writers hold the WAL `flock` across the cache mutation and
absolute-expiry WAL append, preserving the same order seen by network writers.
WAL folding retains the existing inode so long-lived attached writers cannot
append to an obsolete file. If the server restarts, it re-attaches the region
and its whole RAM state is available without replay.

`CEMBv3` stores only fixed-width offsets in the mapped file. Each process may
attach at an arbitrary virtual address, so ASLR and unrelated mappings do not
affect correctness. The caller-visible `KuttiDB` handle is process-local; no
process address is stored in or exchanged from the region. Incompatible older
regions, including pointer-based `CEMBv2`, are refused rather than interpreted
using a different allocator layout.

The Python `LocalKuttiDB` client presents one API for trusted-local and network
operation. It tries CEMBv3 only during connection setup and falls back to a
Unix socket when given `unix_path`, otherwise TCP or TLS. It never retries a
failed direct write through the network path, avoiding accidental duplicate or
reordered mutations.

## Performance and correctness gates

- `make test` runs core, platform, ASLR-safe embedding, persistence,
  TTL/eviction, embedded, security,
  crash-ordering, runtime-WAL-failure, TLS, and client compatibility tests.
- `make sanitize` runs the concurrent core test under AddressSanitizer and
  UndefinedBehaviorSanitizer.
- `make bench-quick` runs fresh-server scaling and idle-footprint gates and
  reports batch round-trip p50/p95/p99 latency.
- `make bench-matrix` adds batch-size and value-size coverage with the same
  throughput, latency, live-memory, allocated-memory, and RSS measurements.
## Operational HTTP surfaces

KuttiDB's binary protocol remains the data protocol. The optional metrics
listener and the optional, versioned Management API are separate
operational HTTP surfaces; the latter is documented in
[MANAGEMENT_API.md](../api/MANAGEMENT_API.md) and does not alter binary protocol
compatibility.
