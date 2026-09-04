# Management API v1

KuttiDB's Management API is an optional authenticated HTTP/1.1 listener for
operators and Management UIs. It is part of the `kuttidb` binary and remains
disabled unless `--admin-bind` is supplied. Version 1 is the single,
additively evolving Management API contract; mutations are separately audited.

## Secure startup

Create a separate administrator token, owned by the server user and mode
`0600`:

```sh
umask 077
printf '%s\n' 'replace-with-a-long-random-token' > admin.token
./kuttidb 7379 kuttidb.wal --admin-bind 127.0.0.1:7380 \
  --admin-token-file admin.token --admin-audit-log admin-audit.jsonl
```

Loopback administration can use HTTP. Remote administration must use native
TLS and a separate certificate/key pair:

```sh
./kuttidb 7379 kuttidb.wal --admin-bind 192.0.2.10:7380 \
  --admin-token-file admin.token --admin-audit-log admin-audit.jsonl \
  --admin-tls-cert admin.crt --admin-tls-key admin.key
```

Authenticate every request with `Authorization: Bearer <admin-token>`:

```sh
curl -H "Authorization: Bearer $(cat admin.token)" http://127.0.0.1:7380/api/admin/v1/status
```

For a browser-based community UI, opt in only exact trusted origins (never a
wildcard):

```sh
./kuttidb --admin-bind 127.0.0.1:7380 --admin-token-file admin.token \
  --admin-audit-log admin-audit.jsonl \
  --admin-allow-origin https://console.example.org
```

The API still requires bearer authentication for CORS preflights and normal
requests. Without an allowed origin, no CORS headers are returned.

## Resources

All paths begin with `/api/admin/v1`.

| Resource | Purpose |
| --- | --- |
| `GET /capabilities` | Feature, engine, TLS, and persistence discovery |
| `GET /status` | Bounded process, Keyspace, Queue, Stream, and persistence health |
| `GET /jobs`, `GET/DELETE /jobs/{job_id}` | Inspect or cancel bounded asynchronous maintenance jobs; only queued jobs are cancellable |
| `GET /maintenance` | Discover supported checkpoint operations and engine availability |
| `POST /maintenance/keyspace-checkpoint`, `POST /maintenance/queue-checkpoint`, `POST /maintenance/stream-checkpoint`, `POST /maintenance/checkpoint-all` | Queue crash-safe engine checkpoints through the bounded job worker |
| `GET /keyspaces` | The default Keyspace’s aggregate state |
| `GET /keyspaces/default` | Aggregate state for the stable default Keyspace ID |
| `GET/PUT/DELETE /keyspaces/default/entries/{entry_id}` | Read, update, or delete an exact binary Keyspace entry |
| `GET /keyspaces/default/entries` | Bounded Keyspace metadata inventory without values; supports binary `prefix`, `expires=present|none`, and opaque cursor filters |
| `POST /keyspaces/default/entries:batch-get` | Read up to 100 exact entries in request order; oversized values are marked `value_omitted` |
| `POST /keyspaces/default/entries:batch-put`, `POST /keyspaces/default/entries:batch-delete` | Fully validate up to 100 exact mutations before ordered durable execution; current Keyspace batches explicitly report `atomic: false` |
| `POST /keyspaces/default/claims`, `GET /keyspaces/default/claims/{claim_id}` | Create or inspect a bounded, opaque, short-lived native single-flight claim |
| `POST /keyspaces/default/claims/{claim_id}:complete`, `POST /keyspaces/default/claims/{claim_id}:release` | Complete a claim with a durable value or release its native ownership immediately |
| `POST /keyspaces/default/entries/{entry_id}:get-or-refresh` | Return a current value, or acquire and return an opaque claim when the entry is absent |
| `GET/POST /queues` | Queue inventory and durable Queue declaration |
| `GET/PATCH/DELETE /queues/{queue_id}` | Inspect a Queue; validate an update request (current durable Queue options are explicitly immutable); or durably delete it after an exact ETag and confirmation check |
| `GET /queues/{queue_id}/messages` | Browse a bounded, non-mutating Queue message snapshot; optional bodies are Base64 encoded |
| `GET /queues/{queue_id}/messages/{message_id}` | Read one exact, non-mutating retained Queue message; `include=body` requests a complete bounded Base64 body |
| `POST /queues/{queue_id}:purge` | Durably discard retained Queue messages after explicit confirmation and Queue ETag validation |
| `POST /queues/{queue_id}/messages` | Publish an explicitly Base64-encoded Queue message |
| `POST /queues/{queue_id}/messages:batch` | Atomically capacity-check and publish up to 100 complete Base64 Queue messages |
| `POST /queues/{queue_id}/deliveries`, `POST /queues/{queue_id}/deliveries:batch`, `GET /queues/{queue_id}/deliveries/{delivery_id}` | Create one or a bounded batch of safe opaque deliveries; batch creation returns compact receipts, and `?include=body` reads a verified active delivery body without exposing native tags or owners |
| `POST /queues/{queue_id}/deliveries:ack-batch`, `POST /queues/{queue_id}/deliveries:nack-batch` | Atomically apply ACK or NACK to 1–50 active delivery IDs from one Queue and opaque owner cohort; every requested ID receives an outcome |
| `GET/POST /queue-consumers`, `GET/DELETE /queue-consumers/{consumer_id}`, `POST /queue-consumers/{consumer_id}/deliveries` | Inspect, register, explicitly delete, or safely consume through a durable Queue consumer without exposing its native owner token |
| `POST /atomic-operations` | Execute a tagged, native all-or-nothing Keyspace-plus-Queue/Routing operation |
| `GET/POST /streams` | Stream inventory and durable Stream declaration |
| `GET/PATCH/DELETE /streams/{stream_id}` | Inspect a Stream; conditionally replace both durable retention ceilings; or queue a confirmed conditional durable deletion job |
| `GET /streams/{stream_id}/partitions` | Snapshot each partition's earliest retained and next offsets without reading record bodies |
| `POST /streams/{stream_id}/records` | Append one Base64 body with an optional Base64 binary key |
| `POST /streams/{stream_id}/records:batch` | Append 1–100 complete Base64 bodies with optional binary keys in one durable batch |
| `POST /streams/{stream_id}/partitions/{partition}:truncate` | Durably discard earlier partition records after explicit confirmation and Stream ETag validation |
| `GET /streams/{stream_id}/partitions/{partition}/records` | Fetch a bounded, complete Stream record page, including each retained key |
| `GET /streams/{stream_id}/partitions/{partition}/records/{offset}` | Read one exact complete Stream record and its binary key within an explicit response bound |
| `GET /streams/{stream_id}/partitions/{partition}/records:tail?offset=N` | Stream one partition through an authenticated, bounded Server-Sent Events worker |
| `GET /routing/routers` | List bounded Router topology snapshots without publishing or mutating state |
| `POST /routing/routers` | Declare a router; bind Queues and publish through its action paths |
| `GET/PATCH /routing/routers/{router_id}` | Inspect a Router definition and revision, or conditionally set its alternate Router using the current ETag |
| `GET/POST /routing/routers/{router_id}/routes`, `GET/DELETE /routing/routers/{router_id}/routes/{route_id}` | List or bind routes; inspect or durably remove one stable route resource after current router ETag and exact route-ID confirmation |
| `DELETE /routing/routers/{router_id}` | Durably delete an empty Router after current revision and exact confirmation checks; routes and alternate-router dependents must be removed first |
| `POST /routing/default/messages` | Publish directly to an explicit Queue through native default Routing |

Routing inventory reads are bounded snapshots under the Routing metadata lock.
They never publish, consume, acknowledge, or alter Queue or Router state.
Router snapshots include per-router publish-attempt and unroutable counters.
Those operational counters are explicitly `process_lifetime` metrics and reset
on server restart; Router topology and revisions remain durable separately.

| `GET /consumer-groups` | Stream Consumer Group generation and active member count |
| `GET /streams/{stream_id}/consumer-groups` | Bounded Consumer Group inventory for one Stream |
| `GET /streams/{stream_id}/consumer-groups/{group_id}` | Consumer Group generation, offset, high-water, lag, and active-member snapshot |
| `GET /streams/{stream_id}/consumer-groups/{group_id}/members` | Privacy-safe member snapshot with opaque per-snapshot IDs, assignment counts, and lease remaining time |
| `GET /streams/{stream_id}/consumer-groups/{group_id}/offsets` | Inspect per-partition Consumer Group offsets and lag |
| `PUT /streams/{stream_id}/consumer-groups/{group_id}/offsets/{partition}` | Conditionally commit one retained offset using the current group generation |
| `POST /streams/{stream_id}/consumer-groups/{group_id}/offsets:batch` | Conditionally commit 1–100 retained offsets with one generation check and durable batch fsync |
| `POST /streams/{stream_id}/consumer-groups/{group_id}:reset-offsets` | Reset all partitions to `earliest`, `latest`, `absolute`, or `relative` offsets after generation and confirmation checks; active groups also require `force: true` |
| `POST /streams/{stream_id}/consumer-groups/{group_id}/sessions` | Explicitly join through a bounded opaque Management session; requires exact group confirmation because it can rebalance members |
| `GET /streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}` | Inspect an opaque session without exposing the native owner token |
| `POST /streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}:heartbeat` | Refresh the native lease and return the current assignment |
| `GET /streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}/records` | Read only a partition currently assigned to that session |
| `POST /streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}/offsets:commit` | Conditionally commit an assigned retained offset using the current generation |
| `POST /streams/{stream_id}/consumer-groups/{group_id}/sessions/{session_id}:leave` | Release assignments immediately and terminate the session |

Collections accept `?limit=N`, defaulting to 100 and capped at 500. Collection
metadata includes `count`, `limit`, `next_cursor`, `snapshot_revision`, and
`weakly_consistent`. A non-null cursor is opaque and has the scope and expiry
reported by `/capabilities`; it must never be reconstructed by a client.

For Keyspace entry scans, `snapshot_revision` is a native process-lifetime
mutation epoch. It advances for value writes, deletes, replacements, and TTL
reaping; because cursors are also intentionally process-lifetime, a restart
invalidates both rather than pretending to preserve an old scan position.

Resource identifiers use `b64u:` URL-safe Base64. Keyspace listing returns
metadata only; exact Keyspace reads, Queue deliveries, and Stream fetches
return content only as explicitly Base64-encoded fields. Responses never
return tokens, consumer owner tokens, audit contents, or local filesystem
paths.

## Keyspace claims and refresh

Claims are an advanced diagnostics and coordination interface over the native
single-flight registry. A successful create returns a random `kc:` identifier,
not the native owner token. It is bound to one exact binary key and a bounded
lease. While live, another claim or a miss through `:get-or-refresh` for that
key returns `409 conflict`; completion writes the Base64 value durably and
releases ownership, while release simply relinquishes it. Expired, completed,
released, and restarted claims are never reusable and return `410` when read
or mutated through their old identifier.

`POST ...:get-or-refresh` returns `200` with `outcome: "value"` for a present
entry. For an absent entry it creates a claim and returns `201` with
`outcome: "claimed"`; the caller then completes or releases that claim. These
operations require an idempotency key. Claim counts are exposed only as the
aggregate `management.active_claims` status metric; native owner tokens are
never returned.

## Queue browsing versus delivery

`GET /queues/{queue_id}/messages?limit=100&state=ready&include=body` is a
read-only snapshot. `state` may be `ready`, `delayed`, or `in-flight`; omit it
to inspect all three states. `include=body` requests Base64 message bodies,
but the aggregate copied-body budget is deliberately limited. A message whose
complete body cannot fit is returned with `body_omitted: true`, never a
partial body. When `meta.next_cursor` is present, pass its opaque value as
`cursor` to request the next page. The server binds it to this Queue and the
state/body filters, expires it after ten minutes, and uses a monotonic ID
keyset boundary internally; removal or expiry of earlier messages does not
invalidate a live cursor. Cursor state is intentionally in-memory and is
therefore invalid after a server restart.

Browsing does not consume or requeue messages. It does not alter Queue order,
delivery count, visibility, dead-letter behavior, counters, or Queue WAL
content. To actually receive work, use `POST /queues/{queue_id}/deliveries`;
the opaque delivery ID it returns is required for the subsequent ACK or NACK.

Queue inspection returns `ETag: "q-<revision>"`. Purging is deliberately
destructive: it removes ready, delayed, and in-flight messages. Send both the
exact ETag in `If-Match` and the opaque Queue identifier in
`X-KuttiDB-Confirm`; the server checks the revision inside the native purge
operation and returns `412 precondition_failed` if the Queue changed.

## Stream truncation

Stream inspection returns `ETag: "s-<revision>"`. To advance one partition's
retained base offset, send that exact ETag in `If-Match`, the opaque Stream
identifier in `X-KuttiDB-Confirm`, an idempotency key, and a JSON
`base_offset`. The request returns `202` and an opaque job ID. Poll its job
resource until it reaches a terminal state; the worker conditionally applies
the durable native operation. A changed Stream becomes a job failure with
`precondition_failed`; an offset outside the retained partition range becomes
`conflict`.

## Stream retention and deletion

`PATCH /streams/{stream_id}` accepts both `max_retained_bytes` and
`max_retained_age_ms`, plus the current `If-Match: "s-<revision>"`. The
native update is durable and atomically checks the revision; any newly
out-of-policy records are durably trimmed before success is returned.

`DELETE /streams/{stream_id}` requires the same current Stream ETag, an exact
`X-KuttiDB-Confirm` stream identifier, and an idempotency key. It returns a
bounded asynchronous job. A successful job has durably removed the Stream,
all retained records, and its Consumer Group offsets; a stale or missing
Stream is reported by the job rather than silently treated as deleted.

## Stream tailing

Send `Accept: text/event-stream` and the normal bearer authorization header to
tail one partition from an explicit offset. Each worker emits bounded `record`
events with `id: partition:offset`, sends heartbeat comments while idle, and
ends after a short maximum lifetime. If retention has advanced past the
requested offset, the stream sends an `offset_out_of_range` event rather than
silently skipping records. Tail workers have a separate configured capacity;
their slow-client send deadline cannot occupy the normal Management API
dispatcher or Stream locks. A worker sends at most the advertised
`limits.tail_events_per_second` record events in any one-second window; it
resumes from the last sent offset after throttling.

## Consumer Group offset commits

Read the group’s offsets first. Its collection metadata contains the current
membership generation as `snapshot_revision`; submit it as
`If-Match: "g-<generation>"` when updating one partition:

```sh
curl -X PUT \
  -H "Authorization: Bearer $(cat admin.token)" \
  -H 'If-Match: "g-1"' \
  -H 'Idempotency-Key: group-offset-202' \
  -H 'Content-Type: application/json' \
  --data '{"offset":42}' \
  http://127.0.0.1:7380/api/admin/v1/streams/b64u:b3JkZXJz/consumer-groups/b64u:d29ya2Vycw/offsets/0
```

The server checks the Stream, group, generation, partition, and retained
offset range together before durably committing. A missing precondition is
`428 precondition_required`; a changed generation is `412
precondition_failed`; an offset outside retained data is a conflict. This is
an administrative commit and never joins or changes Group membership.

Errors have the stable form `{"error":{"code":"...","message":"..."}}`.
The detailed public schema, examples, limits, and security policy are in
[`openapi/management-v1.yaml`](openapi/management-v1.yaml).

## Security and stability

The listener handles one bounded request per connection, accepts only bounded
JSON request bodies, closes connections, and uses short socket timeouts. A
configured admin listener fails startup if its token or audit log is unsafe,
or its binding cannot be created. Non-loopback plaintext administration is
refused. Every supported mutation records an audit attempt before dispatch and
fails closed when the audit trail is unavailable.

Community UI authors should begin with `/capabilities`, tolerate absent or
failed persistence engines, honor `truncated`, preserve Base64 identifiers,
and avoid treating dashboard state as a transactionally consistent snapshot.
An operation or limit reported as unavailable (for example `sse.available:
false` or `jobs: 0`) must keep the corresponding UI control disabled; the
server does not provide compatibility stubs for features without native
durability and recovery semantics.
