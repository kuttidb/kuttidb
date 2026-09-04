# Implementation instruction: full Management UI API for KuttiDB

## Objective

Replace the currently unused read-only Management API draft with a complete,
secure Management UI API at the existing prefix:

```text
/api/admin/v1
```

There is no deployed compatibility requirement for the current v1 draft. Do
not create `/v2`, keep obsolete read-only shapes for compatibility, or maintain
two implementations. Update the implementation, documentation, tests, and
`openapi/management-v1.yaml` together so v1 becomes the single full-control
contract.

The completed API must let an authorized Management UI perform every useful
KuttiDB administration and data operation:

- inspect, browse, read, create, update, and delete Keyspace state;
- declare, configure, inspect, publish to, consume from, ACK, NACK, requeue,
  purge, and delete Queues;
- inspect and manage durable Queue consumers and dead-letter behavior;
- create, configure, inspect, append to, fetch from, tail, truncate, and delete
  Streams;
- inspect Consumer Groups, offsets, lag, generations, and active membership;
- set or reset Consumer Group offsets safely;
- configure Routing and publish through it;
- invoke the supported atomic Keyspace-plus-delivery operations;
- inspect and invoke appropriate maintenance operations;
- discover all supported capabilities without assuming they exist.

Do not build the Management UI in this task. Build the API that an official or
community UI can use without falling back to the native binary protocol.

## Product language

Use only KuttiDB's generic public names in routes, schemas, documentation,
logs, audit events, and user-visible errors:

- Keyspaces
- Queues
- Streams
- Consumer Groups
- Routing
- Records, messages, entries, deliveries, offsets, and partitions

Do not expose compatibility-product terminology. Internal engine structures
may retain their existing names, but the translation belongs behind the
Management API boundary.

For Routing, expose KuttiDB-owned modes:

| Management API mode | Existing internal behavior |
| --- | --- |
| `exact` | exact routing-key match |
| `broadcast` | route to every configured target |
| `pattern` | word-pattern routing |

Public API resources are `routers` and `routes`, not internal compatibility
names.

## Compatibility decision

The present `/api/admin/v1` implementation is an unreleased read-only draft.
Replace it directly. The new OpenAPI document is the first supported v1
contract.

Before merging:

1. Remove language that describes v1 as read-only.
2. Replace old response schemas rather than supporting parallel legacy shapes.
3. Update `MANAGEMENT_API.md`, `README.md`, `ARCHITECTURE.md`, `SECURITY.md`,
   `DEPLOYMENT.md`, and `PROTOCOL.md` in the same change.
4. Add a contract version field to `/capabilities`, such as
   `management_api_contract: "1.0"`, so additive evolution can be reported
   without changing the URL prefix.
5. Future v1 changes must be additive unless a new major API version is
   deliberately introduced.

## Architectural boundaries

KuttiDB remains one C11 server binary. Keep the API in isolated modules and do
not move a large HTTP dispatcher into `src/server.c`.

Recommended layout:

```text
src/admin_http.c          bounded HTTP transport and dispatch
src/admin_http.h
src/admin_auth.c          token loading, authorization, origin policy
src/admin_auth.h
src/admin_json.c          bounded JSON writer and binary representation
src/admin_json.h
src/admin_resources.c     resource handlers and engine adapters
src/admin_resources.h
src/admin_sessions.c      bounded delivery/claim registries
src/admin_sessions.h
src/admin_jobs.c          bounded destructive-maintenance job runner
src/admin_jobs.h
src/admin_audit.c         structured mutation audit trail
src/admin_audit.h
src/test_management_api.py
openapi/management-v1.yaml
```

Do not force this exact split when a smaller coherent layout is clearer. The
important boundary is that HTTP parsing, JSON encoding, authentication,
resource operations, transient delivery ownership, jobs, and audit writing do
not become one monolithic function.

The Management API must call public engine functions. It must not inspect
private hash tables, linked lists, WAL structures, or locks. Add bounded engine
snapshot and mutation APIs where necessary.

Never hold an engine lock while:

- parsing HTTP;
- formatting JSON;
- reading from or writing to a socket;
- waiting for a job, SSE client, audit fsync, or another engine;
- copying an unbounded collection.

Every engine adapter must validate the complete request first, copy bounded
input into owned memory, execute one native operation, copy a bounded result
while holding only the required lock, release it, and then produce JSON.

## Listener and startup policy

Retain the existing flags:

```text
--admin-bind IPv4:PORT
--admin-token-file PATH
--admin-allow-origin ORIGIN
--admin-tls-cert PATH
--admin-tls-key PATH
```

Add:

```text
--admin-audit-log PATH
--admin-max-clients N
--admin-max-tail-clients N
--admin-session-limit N
--admin-job-limit N
```

Requirements:

- The listener remains disabled unless `--admin-bind` is supplied.
- Enabling it requires a separate admin token even on loopback.
- The token file remains a non-symlink regular file, server-owned, mode `0600`,
  containing 1-1024 bytes after trimming one final line ending.
- The admin token grants full v1 access. Do not reuse the ordinary client or
  metrics token implicitly.
- This v1 design does not introduce users, passwords, cookies, authentication
  sessions, or browser login forms. Those require a later identity design.
- Plain HTTP is allowed only on loopback. A non-loopback bind requires native
  TLS.
- Certificate and key must be supplied together. The key must follow the
  existing private-key ownership and permission rules.
- Exact CORS origins may be repeated. Wildcards, suffix matching, reflected
  origins, `null`, and credentials through query parameters are forbidden.
- `--admin-audit-log` is mandatory because v1 permits mutations. It must be a
  non-symlink server-owned file created with mode `0600`.
- Failure to bind, initialize TLS, load credentials, or open the audit log is a
  fatal startup error when the listener is configured.
- No admin listener, thread, registry, job worker, or audit file is created
  when `--admin-bind` is absent.
- `--help` and `--features` report the full Management API, TLS, live-tail,
  audit, and engine capabilities accurately.

## Authentication, authorization, and browser policy

Every request, including `OPTIONS`, SSE tail requests, job polling, and static
capability discovery, requires:

```text
Authorization: Bearer <admin-token>
```

Compare secrets in constant time. Never accept tokens through URLs, cookies,
WebSocket subprotocols, form fields, or JSON bodies. Never log them.

The single v1 token is intentionally an all-powerful operational credential.
The official UI must keep it in memory only, allow paste-on-connect, and never
put it in local storage, URLs, crash reports, analytics, or exported settings.

Because authentication is a bearer header and not a cookie, CSRF does not
authenticate a request. CORS still must remain exact-origin and fail closed.
An approved origin does not bypass the bearer requirement.

Count separately:

- authentication failures;
- forbidden origins;
- rejected mutation confirmations;
- audit failures;
- rate-limit and resource-limit rejections.

## Mutation safety model

Full control must not mean accidental control. Apply the following rules to
every mutation.

### Request identity

Return `X-KuttiDB-Request-ID` on every response. Accept a valid client-supplied
request ID or generate a random one. The value is safe to log and audit but is
not an authentication secret.

Require `Idempotency-Key` for non-idempotent operations, including Queue
publish, Stream append, atomic operations, and asynchronous job creation.
Maintain a bounded in-memory result cache with a documented TTL. A repeated
key with a byte-identical request returns the original result. A repeated key
with different content returns `409 idempotency_conflict`.

Do not claim durable idempotency across process restarts unless it is actually
persisted. If a connection fails after dispatch and no cached result exists,
the client must receive or infer `operation_in_doubt`; the official UI must not
automatically repeat the operation.

### Optimistic concurrency

Return an `ETag` or numeric `revision` for mutable resource definitions.
Require `If-Match` for:

- deleting or replacing a Queue, Stream, router, or route;
- purging a Queue;
- truncating a Stream;
- changing retention or dead-letter configuration;
- resetting Consumer Group offsets.

Missing preconditions return `428 precondition_required`; stale preconditions
return `412 precondition_failed`.

### Destructive confirmation

Operations that discard more than one item require both:

```text
X-KuttiDB-Confirm: <canonical-target-id>
If-Match: <current-etag>
```

This applies to purge, recursive delete, Stream truncation, Consumer Group
offset reset, and removal of a resource containing retained data. The value
must exactly match the opaque target identifier returned by the API. A UI must
show the target and estimated affected counts before enabling confirmation.

### Audit trail

Audit every mutation attempt. Never include Keyspace values, Queue message
bodies, Stream keys or record bodies, bearer tokens, certificates, delivery
ownership tokens, or filesystem paths.

Write structured JSON Lines containing:

- timestamp;
- request ID;
- operation code;
- target resource type and encoded identifier;
- authenticated principal label `admin-token`;
- safe request metadata such as counts, sizes, TTL, partitions, and offsets;
- precondition and idempotency-key hash, never their raw secret material;
- result category;
- engine durability result;
- whether the result is known or in doubt.

Before mutation, append an `attempt` event. If the audit file cannot accept and
sync this event, reject the mutation without executing it. After execution,
append a `completed`, `failed`, or `in_doubt` event. If the engine mutation
succeeds but the completion event cannot be synced, return
`500 operation_in_doubt`; never report that the mutation definitely failed.

Reads may continue when audit persistence has failed, but every mutation must
fail closed until audit health is restored.

## HTTP and JSON constraints

Retain the deliberately bounded HTTP/1.1 design:

- one request per connection except SSE tail connections;
- `Connection: close` for normal responses;
- no chunked request bodies;
- request line and headers no larger than 8 KiB;
- configurable body limit with a hard maximum of 16 MiB;
- short receive, send, and total request deadlines;
- fixed maximum JSON response bytes;
- bounded decompression by not supporting compressed request bodies;
- no multipart uploads in v1;
- no token or payload logging;
- no external HTTP server or JSON dependency.

Support `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, and authenticated
`OPTIONS`. Unsupported methods return `405` with `Allow`.

Every normal response includes:

```text
Content-Type: application/json; charset=utf-8
Cache-Control: no-store
X-Content-Type-Options: nosniff
Referrer-Policy: no-referrer
Connection: close
X-KuttiDB-Request-ID: ...
```

Return `Vary: Origin` with any CORS response. Approved preflights may list only
the methods and headers used by the requested route.

Use the stable error envelope:

```json
{
  "error": {
    "code": "precondition_failed",
    "message": "The resource changed. Refresh it and try again.",
    "request_id": "01...",
    "details": {}
  }
}
```

`details` is optional and may contain field validation errors, safe bounds,
and current revisions. It must never contain private paths, raw configuration,
payload content, pointer values, `errno`, or secrets.

Define at least these codes:

```text
bad_request              unauthorized
forbidden_origin         not_found
method_not_allowed       request_too_large
unsupported_media_type   validation_failed
conflict                 idempotency_conflict
precondition_required    precondition_failed
resource_exhausted       rate_limited
engine_unavailable       persistence_unavailable
audit_unavailable        delivery_expired
operation_in_doubt       internal_error
```

## Binary identifiers and payloads

Native names and keys are byte sequences. Never silently replace, normalize,
case-fold, or reinterpret them.

Every named object response contains an opaque URL-safe identifier derived
reversibly from the exact bytes:

```json
{
  "id": "b64u:AAH_...",
  "name": "orders",
  "name_encoding": "utf-8"
}
```

For non-UTF-8 names:

```json
{
  "id": "b64u:_wA",
  "name": "/wA=",
  "name_encoding": "base64"
}
```

Use the opaque `id` in path parameters. Do not put arbitrary decoded names in
URLs. Reject invalid, oversized, non-canonical, or wrong-resource IDs.

Represent values, message bodies, record keys, and record bodies as:

```json
{
  "encoding": "base64",
  "data": "AAEC",
  "size": 3,
  "content_type": "application/octet-stream"
}
```

The API may accept `encoding: "utf-8"` as a convenience after validating the
string, but the engine always receives exact bytes. Responses default to
Base64 unless valid UTF-8 output was explicitly requested. Enforce both decoded
byte limits and encoded JSON limits before allocation.

## Common collection contract

All collections use:

```json
{
  "data": [],
  "meta": {
    "count": 0,
    "limit": 100,
    "next_cursor": null,
    "snapshot_revision": 42,
    "weakly_consistent": true
  }
}
```

Support `limit`, default 100 and maximum 500, plus an opaque `cursor`. Cursors
must be bounded, validated, tied to the resource and query, and expire. Do not
place raw pointers, bucket addresses, secrets, or filesystem positions in a
cursor.

Engine scan APIs may be weakly consistent under concurrent mutation, but must
never crash, loop forever, expose freed memory, or return an invalid object.
Document that a weak scan can contain duplicates or omit objects changed during
the scan. Return `snapshot_revision` so the UI can detect changes and offer a
refresh.

Do not allocate memory proportional to the entire database to create a page.

## Capability and system resources

### `GET /api/admin/v1/capabilities`

Return:

- product and server version;
- `management_api_contract`;
- enabled engines and persistence availability;
- TLS and SSE availability;
- maximum request, value, batch, page, tail, session, and job limits;
- supported resource operations and optional features;
- supported Routing modes;
- supported durability modes;
- audit health;
- live-tail and cursor semantics.

The UI must gate controls from this response rather than infer features from a
version string.

### `GET /api/admin/v1/status`

Retain the current bounded status fields and add:

- admin connection and active-tail counts;
- active delivery and claim-session counts;
- queued and running admin jobs;
- mutation, audit-failure, rate-limit, and operation-in-doubt counters;
- audit health;
- maintenance/checkpoint health and last completion times;
- per-engine persistence health.

### Jobs

Use bounded asynchronous jobs for purge, delete-with-data, truncation,
checkpoint, and other operations whose work is not predictably small:

```text
POST   /api/admin/v1/jobs
GET    /api/admin/v1/jobs
GET    /api/admin/v1/jobs/{job_id}
DELETE /api/admin/v1/jobs/{job_id}        cancel only before execution
```

Prefer resource-specific action endpoints that return `202` with a job link;
do not make clients construct arbitrary job commands. Use a bounded queue and
one low-priority worker. Job execution must yield between bounded engine work
chunks and must not starve the data path.

## Keyspace API

KuttiDB currently has one global Keyspace. Keep the collection shape for future
expansion and use the stable ID `default`.

```text
GET    /api/admin/v1/keyspaces
GET    /api/admin/v1/keyspaces/default
GET    /api/admin/v1/keyspaces/default/entries
GET    /api/admin/v1/keyspaces/default/entries/{entry_id}
PUT    /api/admin/v1/keyspaces/default/entries/{entry_id}
DELETE /api/admin/v1/keyspaces/default/entries/{entry_id}
POST   /api/admin/v1/keyspaces/default/entries:batch-get
POST   /api/admin/v1/keyspaces/default/entries:batch-put
POST   /api/admin/v1/keyspaces/default/entries:batch-delete
```

Entry listing returns keys and metadata only by default:

- exact byte key identifier;
- value size;
- expiry timestamp and remaining TTL when present;
- revision or ETag;
- persistence/durability metadata where meaningful.

Support bounded filters for exact prefix bytes, expiration state, and page
cursor. Do not add substring or regular-expression scans that require a full
database traversal.

Entry retrieval may return the value after explicit `include=value` or through
the item endpoint. Enforce a response byte limit; return metadata with
`value_omitted: true` when the value cannot fit. Never truncate and present a
partial value as complete.

PUT supports exact bytes and optional TTL. DELETE reports whether an entry was
present. Batch endpoints preserve request order, validate the whole request
before mutation when the native operation is atomic, and state clearly when a
batch is not atomic.

Add engine APIs for metadata scans and TTL inspection. Do not implement TTL
inspection by fetching and rewriting values. Do not implement conditional
updates as a racy Management API GET followed by PUT; add a native atomic
compare/revision operation first.

### Single-flight and stale-state operations

Expose the native coordination features through bounded claim resources:

```text
POST   /api/admin/v1/keyspaces/default/claims
GET    /api/admin/v1/keyspaces/default/claims/{claim_id}
POST   /api/admin/v1/keyspaces/default/claims/{claim_id}:complete
POST   /api/admin/v1/keyspaces/default/claims/{claim_id}:release
POST   /api/admin/v1/keyspaces/default/entries/{entry_id}:get-or-refresh
```

Claim IDs are opaque, random, short-lived Management API registry entries.
They bind the native ownership token, exact key, deadline, and request ID.
They are never reusable after completion, release, expiry, or restart. Bound
the registry and requeue/release native ownership during shutdown.

The UI should expose these operations in an advanced diagnostics area, not as
the default way to edit a value.

## Queue API

```text
GET    /api/admin/v1/queues
POST   /api/admin/v1/queues
GET    /api/admin/v1/queues/{queue_id}
PATCH  /api/admin/v1/queues/{queue_id}
DELETE /api/admin/v1/queues/{queue_id}
POST   /api/admin/v1/queues/{queue_id}:purge
GET    /api/admin/v1/queues/{queue_id}/messages
GET    /api/admin/v1/queues/{queue_id}/messages/{message_id}
POST   /api/admin/v1/queues/{queue_id}/messages
POST   /api/admin/v1/queues/{queue_id}/messages:batch
POST   /api/admin/v1/queues/{queue_id}/deliveries
POST   /api/admin/v1/queues/{queue_id}/deliveries:batch
GET    /api/admin/v1/queues/{queue_id}/deliveries/{delivery_id}
POST   /api/admin/v1/queues/{queue_id}/deliveries/{delivery_id}:ack
POST   /api/admin/v1/queues/{queue_id}/deliveries/{delivery_id}:nack
POST   /api/admin/v1/queues/{queue_id}/deliveries:ack-batch
POST   /api/admin/v1/queues/{queue_id}/deliveries:nack-batch
```

Queue definitions expose:

- exact name;
- durable mode;
- maximum depth;
- ready, delayed, and in-flight counts;
- dead-letter target and maximum-delivery policy;
- redelivery and dead-letter counters;
- persistence health;
- revision/ETag.

PATCH may change only options supported atomically by the engine. If an option
cannot safely change after declaration, return a precise conflict instead of
silently accepting it.

Message browsing is a non-mutating bounded snapshot. Support `state=ready`,
`state=delayed`, and `state=in-flight`, cursor, and `include=body`. Return
message ID, state, byte size, enqueue time if available, expiry, delivery count,
redelivery flag, and visibility deadline where safe. Do not expose native
connection ownership tokens or raw delivery tags.

Implement browsing with a new engine snapshot API. Never simulate peek by
consuming and immediately requeueing because that changes order, delivery
counts, visibility, persistence, and dead-letter behavior.

### Management delivery registry

Consuming is different from browsing. A delivery request calls the normal
Queue consume operation with a dedicated admin owner and returns an opaque
`delivery_id`. Store a bounded registry entry containing:

- exact Queue ID;
- native delivery tag;
- dedicated owner ID;
- delivery and visibility deadlines;
- message ID;
- terminal state.

ACK and NACK accept only the opaque delivery ID. NACK supports `requeue` and a
bounded delay. A delivery is one-use and becomes terminal after ACK, NACK,
expiry, shutdown, or ownership loss. Return `410 delivery_expired` for a known
expired delivery and `404` for an unknown ID.

Do not send native delivery tags or connection ownership values to the UI.
Never ACK or NACK a message based only on a Queue/message ID pair.

Registry capacity exhaustion returns `429 resource_exhausted` before consuming
another message. Reap entries periodically. On clean shutdown, release or
requeue the dedicated admin owner through the normal engine API.

Batch ACK/NACK accepts only delivery IDs from the same Queue and owner. Validate
the entire batch before calling the native batch operation. Return an outcome
for every requested delivery ID and make every successfully processed ID
terminal, even when a later response write fails.

### Durable Queue consumers

```text
GET    /api/admin/v1/queue-consumers
POST   /api/admin/v1/queue-consumers
GET    /api/admin/v1/queue-consumers/{consumer_id}
DELETE /api/admin/v1/queue-consumers/{consumer_id}
POST   /api/admin/v1/queue-consumers/{consumer_id}/deliveries
```

Use the native durable-consumer behavior. The UI may consume under a named
consumer only after explicit selection. Unregistering is destructive and
requires confirmation and `If-Match`.

### Missing Queue engine operations

If the current engine lacks browse, update, purge, or delete primitives, add
them to `queue.h`/`queue.c` with explicit WAL records, replay compatibility,
crash tests, and durability semantics. Do not emulate them with loops of
existing public operations.

Purge and delete must remain crash-safe. A successful durable response means
the removal survives restart. Dead-letter routing and in-flight ownership must
have explicit behavior documented and tested.

## Routing API

```text
GET    /api/admin/v1/routing/routers
POST   /api/admin/v1/routing/routers
GET    /api/admin/v1/routing/routers/{router_id}
PATCH  /api/admin/v1/routing/routers/{router_id}
DELETE /api/admin/v1/routing/routers/{router_id}
GET    /api/admin/v1/routing/routers/{router_id}/routes
POST   /api/admin/v1/routing/routers/{router_id}/routes
GET    /api/admin/v1/routing/routers/{router_id}/routes/{route_id}
DELETE /api/admin/v1/routing/routers/{router_id}/routes/{route_id}
POST   /api/admin/v1/routing/routers/{router_id}/messages
POST   /api/admin/v1/routing/default/messages
```

Router definitions expose `exact`, `broadcast`, or `pattern` mode, durability,
optional alternate router, route count, publish counters, unroutable count,
persistence health, and revision.

Routes connect a router to a Queue with an exact routing key or pattern as
required by the mode. Validate patterns with the native rules before mutation.

Publishing accepts exact binary routing keys and bodies, optional TTL where
supported, and returns the routed Queue count and durable result. A zero-target
publish is a successful but explicit `unroutable` outcome, not an internal
error. Non-idempotent publish requires `Idempotency-Key`.

`routing/default/messages` maps an exact Queue identifier directly through the
native default Routing behavior. It does not create a hidden router resource.

## Stream API

```text
GET    /api/admin/v1/streams
POST   /api/admin/v1/streams
GET    /api/admin/v1/streams/{stream_id}
PATCH  /api/admin/v1/streams/{stream_id}
DELETE /api/admin/v1/streams/{stream_id}
GET    /api/admin/v1/streams/{stream_id}/partitions
GET    /api/admin/v1/streams/{stream_id}/partitions/{partition}/records
GET    /api/admin/v1/streams/{stream_id}/partitions/{partition}/records/{offset}
POST   /api/admin/v1/streams/{stream_id}/records
POST   /api/admin/v1/streams/{stream_id}/records:batch
POST   /api/admin/v1/streams/{stream_id}/partitions/{partition}:truncate
GET    /api/admin/v1/streams/{stream_id}/partitions/{partition}/records:tail
```

Stream definitions expose:

- exact name;
- partition count;
- maximum retained bytes and age;
- retained records and bytes;
- earliest and next offsets by partition;
- persistence health;
- revision/ETag.

Append accepts an optional binary key, exact body bytes, and either a concrete
partition or native automatic/keyed selection. Return the chosen partition and
offset. Batch append preserves native atomicity and durability.

Fetch requires a partition and starting offset plus both `max_records` and
`max_bytes` bounds. Return exact offsets and complete records only. If the next
record cannot fit, return a safe size error; never return a partial record.

PATCH may change retention only if the engine supports a durable atomic update.
Partition count remains immutable unless a separate engine design safely adds
partition expansion.

Truncate and delete require confirmation, `If-Match`, audit, and an async job.
Add native durable operations and WAL/recovery coverage if they do not exist.
Do not emulate truncation by changing retention temporarily.

### Live Stream tailing

Use authenticated Server-Sent Events for one partition per connection:

```text
GET /api/admin/v1/streams/{stream_id}/partitions/{partition}/records:tail?offset=N
Accept: text/event-stream
Authorization: Bearer ...
```

The official browser UI must use streaming `fetch()` so it can send the bearer
header. Never put the token in the SSE URL.

Each event is bounded JSON:

```text
id: <partition>:<offset>
event: record
data: {"partition":0,"offset":42,"key":null,"body":{...}}
```

Requirements:

- cap concurrent tail clients independently of normal admin clients;
- cap record bytes, events per second, buffered output, idle duration, and
  total connection duration;
- apply backpressure by disconnecting a slow client with a final safe event;
- fetch in bounded batches and release all Stream locks before socket writes;
- support heartbeat comments;
- support reconnection from an explicit offset;
- report an `offset_out_of_range` event when retention passed the requested
  offset;
- never claim global ordering across partitions;
- do not block the normal admin dispatcher or data event loops;
- do not allocate one unbounded buffer or engine thread per tail.

A small bounded tail-worker pool is acceptable. A single sequential admin
thread that remains occupied for the life of an SSE request is not.

## Consumer Group API

```text
GET    /api/admin/v1/consumer-groups
GET    /api/admin/v1/streams/{stream_id}/consumer-groups
GET    /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}
GET    /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/offsets
PUT    /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/offsets/{partition}
POST   /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/offsets:batch
POST   /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}:reset-offsets
GET    /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/members
POST   /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/sessions
GET    /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}
POST   /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}:heartbeat
POST   /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}:leave
GET    /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}/records
POST   /api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}/offsets:commit
```

Return group generation, partition offsets, high-water offsets, lag, active
member count, assignments, and lease deadlines where safe. Never expose native
owner tokens or reusable connection credentials. Member identifiers may be
returned only if they are already public non-secret names; otherwise return
stable per-snapshot opaque IDs.

Offset writes are administrative commits. Validate the Stream, group,
partition, retained range, and target offset atomically. Require `If-Match` on
the group generation. Batch changes must use native batch commit semantics.

Reset supports explicit strategies:

```text
earliest
latest
absolute
relative
timestamp        only if the engine gains a real timestamp index
```

Do not fake timestamp reset with a full retained-record scan under a lock. Do
not reset active groups unless `force: true`, confirmation, and the current
generation are supplied. Audit the old and new offsets but no record bodies.

Management tailing must not join, heartbeat, or alter a Consumer Group. It is
an independent observational fetch.

### Management Consumer Group sessions

The UI may explicitly join a Consumer Group for diagnostics through an opaque,
bounded Management API group session. Session creation calls the native join
operation and returns the generation, assigned partitions, and a short lease.
The opaque session ID binds the native owner token and must never expose it.

Heartbeat refreshes the native lease and returns any new generation and
assignment. Session record fetch is allowed only for currently assigned
partitions. Offset commit validates both the active generation and assignment,
then calls the owner-aware native commit operation. Leave is terminal and must
release assignments immediately.

Session expiry, listener shutdown, registry exhaustion, and process restart
must invoke or preserve normal native group lease behavior. Do not silently
create a group session when the user merely opens a group details or tailing
screen. Joining an active group can trigger a rebalance, so the UI must show an
explicit warning and confirmation first.

## Atomic operation API

Expose the existing atomic Keyspace-plus-delivery guarantees through one
explicit endpoint:

```text
POST /api/admin/v1/atomic-operations
```

Use a tagged request union whose allowed forms map one-to-one to real native
atomic operations:

```text
put-and-route
put-and-enqueue
delete-and-route
update-if-present-and-route
```

Each request contains exact Keyspace key/value bytes, target Queue or router,
routing key where needed, and supported TTL metadata. Validate the entire
request before execution. Require `Idempotency-Key`. Return the native commit
identifier and known durability result.

Do not expose a generic transaction language, arbitrary operation list, or
multi-engine semantics that the server does not implement. Never decompose an
atomic request into separate Management API calls.

## Maintenance API

```text
GET  /api/admin/v1/maintenance
POST /api/admin/v1/maintenance/keyspace-checkpoint
POST /api/admin/v1/maintenance/queue-checkpoint
POST /api/admin/v1/maintenance/stream-checkpoint
POST /api/admin/v1/maintenance/checkpoint-all
```

Return `202` and a bounded job resource. Maintenance endpoints must call the
same crash-safe checkpoint functions as automatic maintenance. They must not
accept filesystem paths, filenames, arbitrary commands, or snapshot download
destinations.

Do not expose process shutdown, signal delivery, file deletion, raw WAL access,
configuration-file editing, certificate replacement, or plugin loading in v1.
Those are host-administration operations, not database-management operations.

## Rate and resource limits

Use independent limits for:

- accepted admin connections;
- simultaneous request handlers;
- SSE tail clients;
- request header and body bytes;
- JSON response bytes;
- page items and scan work;
- payload bytes returned per request;
- Queue deliveries held by the admin registry;
- Keyspace claims held by the admin registry;
- Consumer Group sessions held by the admin registry;
- idempotency records;
- queued/running jobs;
- audit backlog;
- mutations per second and payload bytes per minute.

Return `429` with a safe `Retry-After` where retry is appropriate. Management
limits are separate from ordinary client limits and cannot consume every data
listener connection or event-loop worker.

One stalled Management UI must not materially affect ordinary Keyspace, Queue,
Stream, Consumer Group, or Routing operations.

## Persistence failure behavior

Reads may return partial health information with `200` when a persistence
engine is failed, provided the response marks that engine unhealthy. An
operation requiring the failed engine returns `503 persistence_unavailable`.

Never expose local paths or low-level I/O errors. Distinguish:

- validation failure before dispatch;
- known engine rejection;
- known durable success;
- failure with no mutation;
- mutation outcome in doubt.

The UI must display `operation_in_doubt` prominently and refresh the target
before allowing a repeat.

## Engine additions required

Add narrow, bounded public engine APIs instead of reaching into private state.
At minimum, assess and implement what is missing for:

- Keyspace metadata scan, TTL inspection, and revision-aware mutation;
- Queue definition snapshots, message peek snapshots, update, purge, delete,
  and revision reporting;
- Routing definition/route snapshots, updates, deletes, and revisions;
- Stream partition boundaries, definition update, truncate, delete, and
  revisions;
- Consumer Group offset snapshots and atomic revision-aware reset;
- chunked maintenance operations suitable for low-priority jobs.

Every new durable mutation requires:

1. a versioned WAL record or a proven composition of existing atomic records;
2. CRC validation and bounded replay;
3. compatibility handling for previous WALs;
4. fsync and acknowledgment semantics matching the engine contract;
5. torn-tail, corruption, crash-boundary, and restart tests;
6. checkpoint/compaction preservation;
7. failure injection proving fail-closed behavior.

Do not implement a Management API feature by violating an engine invariant.
If an engine primitive requires a separate design, add the primitive and its
tests first, then expose it.

## OpenAPI contract

Replace `openapi/management-v1.yaml` with a complete OpenAPI 3.1 contract.

Document:

- every route, method, parameter, header, request body, status, and schema;
- bearer authentication on every operation, including `OPTIONS` and SSE;
- opaque binary identifiers and payload encodings;
- cursor consistency and expiry;
- ETag/`If-Match` semantics;
- destructive confirmation;
- idempotency and `operation_in_doubt`;
- audit requirements;
- Queue delivery registry behavior;
- Stream SSE format, limits, and reconnect behavior;
- all error codes;
- capability discovery;
- persistence failure behavior;
- examples for successful and failed requests.

Avoid large unreadable one-line YAML maps. Use named reusable components and
validate the YAML in tests. Treat the checked-in document as the public
compatibility contract.

Generate or maintain a route/schema conformance test that compares implemented
routes and response examples with the OpenAPI document. The implementation may
not silently add undocumented mutations.

## Tests

Add focused unit, integration, concurrency, failure, and recovery coverage.
At minimum test:

### Listener and security

1. No listener, thread, audit file, or registry exists by default.
2. Token and TLS startup rules remain fail closed.
3. An audit log is required for the full API.
4. Missing, malformed, wrong, URL, and cookie credentials are rejected.
5. Exact-origin CORS works and unapproved origins cannot act.
6. Preflight still requires authentication.
7. Tokens and payloads never appear in errors, logs, metrics, or audit data.
8. Oversized headers, bodies, JSON depth, arrays, Base64, and values fail
   without excessive allocation.
9. Request, connection, tail, job, registry, and rate limits are enforced.

### Keyspaces

10. Entries can be listed, read, put with/without TTL, deleted, and batched.
11. Binary keys and values round-trip exactly.
12. TTL metadata is accurate without rewriting values.
13. Cursor scans remain safe during concurrent put/delete/expiry/eviction.
14. Claim completion, release, timeout, registry capacity, and shutdown cleanup
    preserve native coordination behavior.

### Queues and Routing

15. Queues can be created, inspected, updated where allowed, purged, and
    deleted with correct durability.
16. Message browsing does not change order, delivery count, state, visibility,
    WAL content, or dead-letter behavior.
17. Publish and batch publish preserve exact bodies and TTL.
18. Management consume returns opaque delivery IDs; ACK/NACK/requeue/delay are
    one-use and owner-safe.
19. Delivery expiry, disconnect, restart, shutdown, and registry exhaustion do
    not lose messages.
20. Durable Queue consumer operations preserve their ownership behavior.
21. Routers and routes support exact, broadcast, and pattern behavior.
22. Unroutable publishing is reported accurately.
23. Queue purge/delete and Routing changes survive crash/restart or fail closed.

### Streams and Consumer Groups

24. Streams can be created, inspected, updated where allowed, appended,
    fetched, truncated, and deleted.
25. Binary record keys and bodies round-trip exactly.
26. Fetch obeys record and byte bounds and never returns partial records.
27. SSE tail returns ordered offsets for one partition, heartbeats, resumes,
    detects retention gaps, times out, and disconnects slow clients.
28. Many tail clients cannot stall ordinary appends or normal admin requests.
29. Consumer Group offsets, high-water positions, lag, generation, assignments,
    and safe member information are correct.
30. Offset commit, batch commit, reset strategies, active-group protection, and
    stale generations are tested.
31. Management tailing never mutates group membership or offsets.
32. Explicit Management Consumer Group sessions join, rebalance, heartbeat,
    fetch only assigned partitions, commit owner-safe offsets, leave, expire,
    and clean up without exposing native ownership tokens.
33. Stream mutation and offset reset survive crash/restart or fail closed.

### Mutation integrity

34. `If-Match`, confirmation headers, and stale revisions work for every
    destructive route.
35. Repeated idempotency keys return the original result; mismatched bodies
    conflict; restart limitations are documented and tested.
36. Every mutation produces safe attempt and completion audit events.
37. Audit failure before dispatch prevents mutation.
38. Audit failure after a known engine success returns `operation_in_doubt`.
39. Atomic Keyspace-plus-delivery requests preserve their native all-or-nothing
    crash-recovery guarantee.
40. Jobs are bounded, cancellable before start, non-cancellable after the
    irreversible point, and do not starve the data path.
41. Persistence failures return safe health or mutation errors without paths or
    secrets.

### Compatibility and quality

42. Existing native binary protocol behavior and official clients remain
    compatible.
43. All JSON is valid under unusual UTF-8 and binary identifiers.
44. OpenAPI parses and matches every implemented route and stable error.
45. Parallel status, browsing, tailing, and mutation traffic does not deadlock
    or materially regress ordinary operations.
46. Graceful shutdown closes listeners, tails, registries, jobs, and audit
    handles without use-after-free or leaked delivery ownership.
47. OpenSSL and no-OpenSSL builds work; no-OpenSSL permits only loopback HTTP.
48. Make and CMake include every module and test.
49. ASan/UBSan and relevant TSan targets cover the new modules and concurrency.

Run at least:

```text
make
make test
make sanitize-server
```

Also run the relevant Queue and Stream failure/crash suites, the no-TLS CMake
build, and a concurrency test mixing admin work with ordinary client traffic.

## Documentation

Rewrite `MANAGEMENT_API.md` as the operator and UI-author guide. Include:

- full-control warning and security model;
- secure startup and audit-log examples;
- loopback and remote TLS examples;
- exact CORS example;
- token-handling guidance for browser UIs;
- capability discovery;
- all resources and common workflows;
- binary identifier and payload examples;
- Queue browse versus consume behavior;
- delivery ACK/NACK lifecycle;
- Stream fetch versus live tail behavior;
- Consumer Group offset inspection and reset safeguards;
- ETag, confirmation, idempotency, jobs, and audit semantics;
- failure and in-doubt guidance;
- resource limits and performance expectations.

Update the other public documents so none still claims Management API v1 is
read-only.

## Explicitly out of scope

Do not add:

- the Management UI itself;
- user accounts, passwords, login forms, authentication sessions, or cookies;
- OAuth/OIDC or external identity providers;
- role-based authorization in this single-token v1 design;
- wildcard CORS;
- tokens in URLs;
- arbitrary SQL/query languages, scripts, or server-side code execution;
- raw WAL, snapshot, filesystem, certificate, or configuration-file access;
- remote process shutdown or signal operations;
- plugin loading;
- semantics that the engines do not actually provide;
- unbounded scans, buffers, registries, tails, jobs, or audit queues.

These can be designed later without weakening the full engine-management
capability of v1.

## Implementation sequence

Implement in reviewable stages while keeping the tree buildable:

1. Finalize the OpenAPI shapes, opaque ID format, common errors, request IDs,
   ETags, idempotency, confirmation, and audit record format.
2. Refactor the current read-only module into transport/auth/JSON/resource
   boundaries without changing engine behavior.
3. Add Keyspace metadata/value operations and bounded scans.
4. Add Queue browsing and the delivery registry, then Queue definition
   mutations with durable engine primitives.
5. Add Routing resources and publishing.
6. Add Stream fetch/append resources and bounded SSE tail workers.
7. Add Consumer Group offset inspection and mutation.
8. Add atomic operations, maintenance jobs, purge/delete/truncate primitives,
   and their recovery coverage.
9. Complete security, concurrency, failure-injection, sanitizer, no-TLS, and
   OpenAPI conformance gates.
10. Rewrite all public documentation and examples.

Do not merge a route that advertises success before its engine durability,
audit, resource-bound, and recovery behavior is defined and tested.

## Acceptance criteria

The work is complete when:

- `/api/admin/v1` is the only Management API contract and supports the full
  authorized Keyspace, Queue, Stream, Consumer Group, Routing, atomic, and
  maintenance workflows described above;
- an official Management UI can perform these workflows without using the
  native binary protocol;
- browsing and live tailing are bounded and do not mutate state accidentally;
- ACK/NACK operations are ownership-safe through opaque one-use deliveries;
- destructive operations require revisions and exact confirmation;
- non-idempotent retries have explicit semantics;
- every mutation is safely audited and audit failure fails closed;
- remote access requires TLS and every request requires the separate admin
  bearer token;
- no response exposes secrets, filesystem paths, private ownership tokens, or
  engine internals;
- every new durable engine operation has WAL, recovery, crash, and failure
  tests;
- ordinary KuttiDB data operations and protocol compatibility remain intact;
- Make, CMake, full tests, sanitizer tests, and no-TLS builds pass;
- the OpenAPI 3.1 contract, implementation, tests, and documentation agree.
