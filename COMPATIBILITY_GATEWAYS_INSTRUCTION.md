# Implementation instruction: partial Redis, AMQP 0-9-1, and Kafka client protocol support

## Objective

Add an **optional compatibility gateway** that lets ordinary Redis, RabbitMQ
(AMQP 0-9-1), and Kafka clients connect to KuttiDB for a carefully defined
subset of common operations.

This is intentionally partial protocol support. Do not present KuttiDB as a
drop-in Redis, RabbitMQ, or Kafka replacement. The accurate product claim is:

> KuttiDB provides opt-in Redis RESP, AMQP 0-9-1, and Kafka client protocol
> subsets for common cache, work-queue, and event-stream operations.

Unsupported commands and semantics must fail explicitly. Never acknowledge an
operation with stronger semantics than KuttiDB actually provides.

## Current repository facts to preserve

- The native server is C11 and uses one bounded event loop per worker.
- The native protocol is binary, little-endian, pipelined, capability-negotiated,
  and currently documented as version 1.5.
- Cache, queue/exchange, and stream state are separate engines with separate
  persistence behavior.
- Cache values support binary data and optional TTL. The public cache API does
  not currently expose TTL inspection, conditional SET, increment, scans, or
  rich Redis data structures.
- Queues already provide durable declarations, publish, pull consumption,
  ACK/NACK, requeue, visibility deadlines, prefetch, durable named consumers,
  delivery tags, DLQs, and delayed retry.
- Exchanges already provide direct, fanout, topic, and default routing.
- Streams already provide durable topics, 1-256 partitions, append/fetch,
  offsets, retention, batch append, and lease-based consumer groups.
- Queue delivery and stream consumption are at-least-once. There is no
  exactly-once claim, replication, Kafka transaction support, or node-failure
  tolerance.
- The working tree may contain unrelated user changes. Do not reset, overwrite,
  or reformat them.

Before coding, reconcile stale documentation that still mentions protocol 1.2
or 1.3 with `PROTOCOL.md`, which currently defines 1.5. Do not change wire
versions only to make the prose agree.

## Architectural boundary

Implement compatibility as a new optional executable, tentatively named
`kuttidb-compat`, under `compat/`. It must connect upstream to KuttiDB's native
TCP or Unix-socket listener and translate requests. Do not put three foreign
protocol parsers into `src/server.c`, and do not let a compatibility process
open KuttiDB WAL files directly.

Recommended shape:

```text
Redis client  -> RESP listener ------+
AMQP client   -> AMQP 0-9-1 listener +-> kuttidb-compat -> native KuttiDB listener
Kafka client  -> Kafka listener ------+
```

Use separate, explicitly configured listener addresses. Default all listeners
to loopback and disabled. Suggested flags:

```text
kuttidb-compat \
  --upstream unix:/path/to/kuttidb.sock \
  --redis-listen 127.0.0.1:6380 \
  --amqp-listen 127.0.0.1:5673 \
  --kafka-listen 127.0.0.1:9093 \
  --token-file /owner-only/path/token
```

Do not claim that the standard ports belong to KuttiDB and do not enable them
implicitly. A non-loopback listener must require authentication. TLS must be
configurable per listener. Upstream AUTH/TLS/Unix-socket behavior must reuse
the native security model.

Prefer Go for the optional gateway because the repository already requires Go
for a supported client, and its networking primitives reduce parser and
connection-state risk. Keep the core C build dependency-free. A protocol-codec
dependency may be used only by the optional gateway, must be pinned, and must
not become part of the default `make` or CMake build.

## Shared gateway requirements

1. Perform native AUTH and CAPABILITIES negotiation when each upstream
   connection is created. Refuse to start a listener whose required native
   capability bits are absent.
2. Keep protocol parsers incremental: frames may be fragmented, combined, or
   pipelined. Never assume one socket read equals one request.
3. Bound connection count, frame size, field count, nesting depth, pending
   responses, inflight deliveries, fetch bytes, and protocol-specific metadata.
4. Apply read, write, handshake, idle, and incomplete-frame timeouts.
5. Preserve downstream correlation/order rules while allowing multiple native
   upstream connections where a foreign protocol requires independent state.
6. On an upstream timeout or disconnect after a mutation was sent, report an
   in-doubt/connection failure. Do not invent success or automatically replay a
   non-idempotent publish.
7. Put protocol-specific metadata into a versioned, CRC-checked gateway
   envelope only where necessary. Raw native cache values must never be
   enveloped. For AMQP/Kafka envelope data, document what native clients see
   and support decoding it in KuttiDB's official clients or CLI.
8. Expose counters for accepted connections, parse errors, unsupported
   operations, upstream errors, published/consumed records, auth failures, and
   per-protocol bytes. Never include credentials, keys, message bodies, or
   arbitrary client strings as metric labels.
9. Log the foreign command/API name and a safe error category, not payloads or
   secrets.
10. Unknown or unsupported operations must receive the protocol's normal
    explicit error response whenever the frame is valid. Malformed or unsafe
    frames may close the connection.

## Redis RESP subset

### Connection and handshake support

Implement RESP2 requests and responses first, plus enough RESP3 for modern
client handshakes. Support:

- `PING [message]`
- `ECHO message`
- `AUTH password` and `AUTH username password`; accept only username
  `default`, and map the password to the KuttiDB token
- `HELLO 2|3 [AUTH default password] [SETNAME name]`
- `CLIENT SETNAME`, `CLIENT GETNAME`, `CLIENT ID`, and `CLIENT SETINFO`
- `SELECT 0` only
- `COMMAND` and `COMMAND INFO` describing only the implemented subset
- `QUIT`

Return a truthful HELLO map identifying the server as `kuttidb`, standalone,
primary, and the actual RESP mode. Do not impersonate a Redis version.

### Cache command support

Implement this first release subset:

| Redis command | KuttiDB mapping | Required behavior |
|---|---|---|
| `GET key` | native GET | bulk value or null |
| `SET key value` | native PUT | `OK` only after native success |
| `SET key value EX n` | native PUT_TTL | validate positive seconds |
| `SET key value PX n` | native PUT_TTL | validate positive milliseconds |
| `DEL key [key ...]` | repeated/native batched DELETE | return actual deletion count |
| `EXISTS key [key ...]` | GET/batched GET | return hit count without changing TTL |
| `MGET key [key ...]` | native GET_BATCH | preserve input order and null misses |
| `MSET key value [...]` | native PUT_BATCH | validate all arguments before sending |
| `DBSIZE` | native STATS | return live cache count |

`SET EX/PX` uses KuttiDB's current second-granularity absolute expiry and may
round a positive sub-second duration up. Document that difference.

Do not implement `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, or `PERSIST` by reading and
rewriting a value. If these commands are desired, first add atomic native
expiry inspection/mutation operations, capability bits, persistence records,
recovery tests, and official-client support. Only then expose them through RESP.

Do not emulate the following in the gateway:

- `SET NX`, `SET XX`, `GETSET`, `INCR`, `DECR`, or compare-and-set through a
  racy GET followed by PUT
- transactions (`MULTI`, `EXEC`, `WATCH`), Lua/functions, pub/sub, streams,
  lists, sets, hashes, sorted sets, bitmaps, HyperLogLog, modules, cluster, or
  replication commands
- `KEYS` or `SCAN` until a bounded native cursor API exists

Return `-ERR unsupported command or option in KuttiDB RESP subset` for these.
Use protocol-correct RESP3 null/map/boolean forms after a successful HELLO 3.

### Redis acceptance clients

Test at least `redis-cli`, redis-py, node-redis, and go-redis with explicitly
documented versions. Their default connection handshakes must succeed. Add a
short client configuration example for each. Tests must cover pipelining,
binary keys/values within KuttiDB limits, fragmented frames, authentication,
RESP2/RESP3 negotiation, misses, expiry, malformed lengths, and unsupported
commands.

## RabbitMQ / AMQP 0-9-1 subset

Support AMQP **0-9-1 only**. Reject AMQP 1.0 or other protocol headers.

### Connection and channel methods

Implement the normal handshake and lifecycle:

- protocol header `AMQP\x00\x00\x09\x01`
- `connection.start/start-ok` with SASL PLAIN
- `connection.tune/tune-ok`, `connection.open/open-ok`, close, and heartbeats
- virtual host `/` only
- `channel.open/open-ok`, channel close, and connection close
- multiple bounded channels per connection

Map the SASL PLAIN password to the KuttiDB token. Accept a configurable
username but do not use it as an authorization namespace. State this clearly.

### Topology and messaging methods

Implement:

| AMQP method | KuttiDB mapping |
|---|---|
| `exchange.declare` | native direct/fanout/topic exchange declare |
| `queue.declare` | native queue declare |
| `queue.bind` / `queue.unbind` | native exchange bind/unbind |
| `basic.publish` + content frames | native exchange publish |
| `basic.qos` | native per-owner/channel prefetch |
| `basic.consume` / `basic.cancel` | bounded polling plus async `basic.deliver` |
| `basic.get` | native single-message consume |
| `basic.ack` | native ACK |
| `basic.reject` / `basic.nack` | native NACK with requeue flag |
| `confirm.select` | publisher-confirm mode |

Rules:

- Use one stable delivery-tag namespace per AMQP channel and maintain a
  channel-local map to KuttiDB's one-use delivery tokens.
- `multiple=true` ACK/NACK must apply to the exact channel-local prefix and
  must fail the channel if any referenced tag is invalid. Do not acknowledge a
  tag on another channel.
- Manual acknowledgement maps naturally to KuttiDB at-least-once delivery.
  Auto-ack acknowledges immediately after the complete delivery has been
  accepted for downstream write; document its weaker safety.
- Channel/connection closure must requeue unacknowledged, unnamed-consumer
  deliveries according to native ownership rules. Named durable-consumer
  behavior must retain its visibility deadline.
- `basic.publish mandatory=true` must send `basic.return` when native routing
  reports zero copies. With `mandatory=false`, report no return.
- In confirm mode, emit `basic.ack` only after KuttiDB confirms the publish.
  Emit `basic.nack` or close the channel with the correct error class when
  durability/routing fails. Preserve publish sequence order.
- A durable AMQP queue maps to a durable KuttiDB queue. Auto-delete, exclusive,
  server-named queues, queue deletion/purge, exchange deletion, priorities,
  transactions, and alternate exchange arguments are unsupported in v1 unless
  a precise native mapping is added.
- Support body fragmentation across multiple content-body frames and enforce
  negotiated `frame-max`.
- Preserve this limited Basic.Properties set in a versioned gateway envelope:
  `content_type`, `content_encoding`, `headers` with bounded primitive values,
  `delivery_mode`, `correlation_id`, `reply_to`, `expiration`, `message_id`,
  `timestamp`, `type`, `user_id`, and `app_id`. Unknown property flags fail
  explicitly. Native queue messages without an envelope are delivered as a raw
  AMQP body with empty properties.
- Map AMQP per-message `expiration` to native queue-message TTL. Queue-level
  `x-message-ttl`, DLX, delivery-limit, and delayed retry arguments may be added
  only where their exact native mapping is documented and tested.

`basic.consume` is push-based while KuttiDB consume is currently immediate
pull. Implement bounded adaptive polling with cancellation and backoff for the
first slice. Do not spin on an empty queue. Track a follow-up native long-poll
or waiter opcode if polling becomes a measured bottleneck.

### AMQP acceptance clients

Test at least Pika, amqp091-go, RabbitMQ Java Client, and amqplib for Node.js
with pinned versions. Cover handshake, heartbeat timeout, channel isolation,
declaration idempotence/conflicts, routing types, mandatory returns, confirms,
consumer push, prefetch, manual/auto ACK, NACK/requeue, redelivery, connection
loss, frame fragmentation, oversized field tables, and restart recovery.

## Kafka client protocol subset

Kafka support is the narrowest and must be version-pinned. Advertise only API
versions that have complete request and response codecs plus tested semantics.
Never advertise an API/version and then partially parse it.

### Broker model

- Present one standalone broker with one configured broker/node id.
- Every KuttiDB stream topic is a Kafka topic; partitions map one-to-one.
- Replication factor is always 1. Leader epoch is a stable synthetic value.
- Topic names must satisfy both Kafka client validation and KuttiDB's 255-byte
  stream name limit. Partition count remains 1-256.
- Offsets map one-to-one to KuttiDB stream offsets.
- Do not claim Kafka cluster, ISR, replication, rack awareness, transactions,
  idempotent producers, exactly-once, log compaction, tiered storage, ACLs, or
  Kafka Connect compatibility.

### Required APIs

Start with exact, tested versions of:

- `ApiVersions` (18)
- `Metadata` (3)
- `Produce` (0)
- `Fetch` (1)
- `ListOffsets` (2)
- `FindCoordinator` (10)
- `JoinGroup` (11)
- `Heartbeat` (12)
- `LeaveGroup` (13)
- `SyncGroup` (14)
- `OffsetCommit` (8)
- `OffsetFetch` (9)
- `DescribeGroups` (15) and `ListGroups` (16), if required by an acceptance
  client
- `CreateTopics` (19) as an optional mapping to native stream declaration
- `SaslHandshake` (17) and `SaslAuthenticate` (36) only when SASL/PLAIN is
  enabled

Use the official Kafka protocol schemas or a pinned codec package for every
advertised version. Begin with the lowest API versions accepted by the chosen
client matrix, then add versions one at a time. Flexible versions, tagged
fields, fetch sessions, leader epochs, and newer consumer-group protocols must
remain unadvertised until implemented.

### Produce and fetch behavior

- Support uncompressed record batches first. Reject gzip, Snappy, LZ4, and
  Zstandard with the appropriate Kafka error until each codec is deliberately
  added and bomb-resistant limits are tested.
- Reject transactional and idempotent producer requests. Documentation must
  show client settings such as `enable.idempotence=false` where required.
- Validate CRCs, record counts, lengths, deltas, timestamps, and partition
  bounds before appending anything.
- Use native batch append for records targeting the same topic/partition.
- Preserve Kafka record timestamp, key, headers, and nullable value in a
  versioned gateway envelope. Native stream records without an envelope are
  returned as Kafka records with a null key, empty headers, and a documented
  synthetic timestamp.
- `acks=0` may suppress a response but must not change native durability.
  `acks=1` and `acks=all` both mean the single KuttiDB node accepted the
  durable append; replication is still 1.
- Map earliest/latest ListOffsets to the retained start offset and next append
  offset. Retention gaps must not be silently renumbered.
- Bound fetch wait, min bytes, max bytes, partition count, and total response
  size. A first implementation may answer an empty fetch after a bounded wait;
  it must not block the whole gateway.

### Consumer-group behavior

- Initially support one topic subscription per group member and the
  `range`/`roundrobin` assignment names needed by the acceptance clients.
- Map group membership leases and generations onto native
  `stream_group_join`, heartbeat, leave, assigned-partition commit, and stored
  offsets.
- Keep Kafka member id, generation, protocol metadata, and native owner mapping
  in bounded gateway state. A gateway restart triggers a normal rebalance; it
  must not lose committed native offsets.
- Reject commits from a stale generation or an unassigned member.
- Do not fake cooperative rebalancing or the new consumer protocol. Advertise
  only classic group APIs that pass the compatibility matrix.

If correct JoinGroup/SyncGroup translation cannot be completed in the first
slice, release producer plus explicit-partition fetch support separately and
label consumer-group support unavailable. Do not ship a superficially working
group coordinator.

### Kafka acceptance clients

Test at least the Apache Java client, kafka-python, and librdkafka through
confluent-kafka with pinned versions and documented settings. Cover correlation
ids, version negotiation, metadata, multi-partition produce/fetch, record
envelopes, CRC failure, retention gaps, offsets, group rebalance/generation,
heartbeat expiry, commit/restart recovery, SASL failure, unsupported APIs,
fragmented frames, oversized arrays, and unknown tagged fields.

## Build and packaging

- Default `make`, the CMake core build, and the existing container must remain
  unchanged unless compatibility is explicitly requested.
- Add explicit targets such as `make compat`, `make test-compat`, and a
  separate optional container image or Compose profile.
- The gateway must refuse to start when no listener is enabled.
- Print a startup capability table listing protocol, address, TLS/auth state,
  and exact supported subset/version. Never log the token.
- Add health/readiness endpoints or commands that check both gateway parser
  health and upstream KuttiDB HEALTH.

## Documentation changes

Update `README.md`, `ARCHITECTURE.md`, `ROADMAP.md`, `MIGRATION.md`, deployment
docs, and add `COMPATIBILITY.md`. Replace the old blanket statement that
KuttiDB speaks none of the protocols only after the corresponding tests pass.

Every protocol section must include:

- supported commands/APIs and exact versions
- unsupported features and the error clients receive
- semantic differences from Redis/RabbitMQ/Kafka
- required client settings
- security/TLS examples
- durability, redelivery, and single-node limitations
- a tested-client/version matrix

Never use the phrases "Redis-compatible", "RabbitMQ-compatible",
"Kafka-compatible", "drop-in replacement", "exactly once", or "no data loss"
without a narrowly qualified and test-backed statement.

## Implementation order

1. Add `COMPATIBILITY.md` with the proposed matrices and obtain agreement on
   exact client versions and settings.
2. Build a bounded native-upstream connection layer with AUTH, CAPABILITIES,
   reconnect, deadlines, and in-doubt error handling.
3. Implement and fuzz RESP first; it is the smallest parser and validates the
   gateway architecture.
4. Implement AMQP connection/channel framing, then topology, publishing,
   confirms, pull consume, and finally push consume.
5. Implement Kafka framing and ApiVersions/Metadata, then Produce/Fetch, then
   offsets, and only then the classic group coordinator.
6. Add integration suites with real clients and crash/restart cases.
7. Run sanitizers/fuzzers for C changes, Go race tests and fuzz tests for the
   gateway, and the complete existing `make test` suite.
8. Update product claims only for slices whose exit gates pass.

## Exit gates

The work is complete only when:

- all existing native tests still pass;
- each enabled listener is bounded, authenticated as configured, fuzzed, and
  safe under fragmented/pipelined input;
- the documented real-client matrix passes in CI;
- unsupported features fail explicitly and consistently;
- durable acknowledgements occur only after the native engine's documented
  acknowledgement point;
- queue redelivery and stream offset behavior survive a KuttiDB restart;
- gateway restart behavior is documented and tested;
- no foreign protocol can bypass native value, batch, connection, auth, or
  durability limits;
- documentation accurately says "protocol subset" and lists the gaps.

Do not broaden the compatibility claim merely because a client opens a socket.
Connection success, command support, and semantic compatibility are separate
testable claims.
