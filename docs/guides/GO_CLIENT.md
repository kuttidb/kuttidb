# KuttiDB Go client

Install: `go get github.com/kuttidb/kuttidb/clients/go` (git tags `go-vX.Y.Z`;
see [CLIENT_PUBLISHING.md](../operations/CLIENT_PUBLISHING.md)).

```go
import kuttidb "github.com/kuttidb/kuttidb/clients/go"

db, err := kuttidb.New("127.0.0.1:7379", 8)                 // pool of idle conns
db, err := kuttidb.NewAuthenticated(addr, 8, token)
db, err := kuttidb.NewTLS(addr, 8, token, tlsConfig)
db, err := kuttidb.NewManaged(kuttidb.ManagedOptions{DataDir: dir})
defer db.Close()
```

The pool is an idle-connection cache: `get` dials overflow connections when
it is empty, so pool size is not a concurrency cap.

## Context APIs

Every public cache, queue, stream, and single-flight method has a
`MethodContext(ctx, …)` twin with the same signature plus a leading
`context.Context`. Legacy names delegate with a background context and the
configured operation timeout, so existing code compiles unchanged.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := db.PutContext(ctx, "greeting", []byte("hello")); err != nil { ... }
v, err := db.GetContext(ctx, "greet")
d, err := db.QueueConsumeAsContext(ctx, "jobs", "worker-a", 30*time.Second)
ok, err := db.QueueNackContext(ctx, "jobs", d.DeliveryTag, true, time.Second)
recs, err := db.StreamFetchWithMetadataContext(ctx, "events",
    kuttidb.StreamFetchOptions{Offset: cursor.Offset, MaxRecords: 100,
                               ExpectedStreamID: cursor.StreamID})
```

One absolute deadline governs a complete operation — connection acquisition,
AUTH, capability probes, every batch chunk, and every response segment: the
earlier of the caller's deadline and the configured operation timeout
(30 s default). `errors.Is(err, context.Canceled)` and
`errors.Is(err, context.DeadlineExceeded)` recognize caller termination;
shutdown during a request returns an error matching `ErrClosed`, while a
response already fully received may still complete successfully. `Close()`
is idempotent and concurrency-safe; requests begun afterwards return
`ErrClosed`, and Close interrupts active I/O instead of waiting out the read
timeout.

Loading helpers take a context-aware loader:

```go
value, err := db.GetOrLoadContext(ctx, "dashboard:1",
    func(ctx context.Context) ([]byte, error) { return loadDashboard(ctx) },
    time.Minute, 5*time.Second, 10*time.Second)
```

The SDK cannot stop a loader that ignores its context — long loaders should
check `ctx` themselves — and loaders run synchronously on the caller's
goroutine.

## Connection affinity and mutation discipline

Queue consumption/disposition, named-consumer attachment, single-flight
leases, and stream-group membership ride one dedicated state connection per
client. Cancellation or shutdown that discards that socket invalidates
deliveries, claims, and memberships held there: reestablish the state
explicitly — the SDK never replays `Consume`/`Ack`/`Nack`/`GroupJoin`
automatically. Sent mutations are never retried after cancellation or a
lost response; the server may have durably committed before the client lost
the reply. Keep publication intents and deduplication in the application
(e.g. SQLite state) responsible for reconciliation.

## Embedded shared memory is explicitly opt-in

The default network package builds with either `CGO` setting and no KuttiDB
headers or libraries. Shared-memory embedding additionally requires
`CGO_ENABLED=1`, the `-tags kuttidb_embed` build tag, and a C toolchain:

```sh
CGO_ENABLED=1 go build -tags kuttidb_embed ./...
CGO_ENABLED=1 go run -tags kuttidb_embed ./cmd/embedsmoke /tmp/db.embed 7379
```

Build from a full repository checkout (`<checkout>/src` headers,
`libkuttidb_embed` from `make`) or link an installed library through
`CGO_CPPFLAGS`/`CGO_LDFLAGS`. With the tag but CGO disabled the embedding
symbols stay unavailable — nothing silently falls back to the network path.
`EmbedDB` is cache-only; durable work needs the network client.

## Stream replay cursors

Persist the replay cursor as `(StreamID, partition, nextOffset)`. The
metadata fetch returns records, the persisted topic incarnation, both
partition boundaries, the range decision, and the resume offset in one
round trip:

```go
res, err := db.StreamFetchWithMetadataContext(ctx, "events",
    kuttidb.StreamFetchOptions{Partition: 0, Offset: cursor.Offset,
                               MaxRecords: 500,
                               ExpectedStreamID: cursor.StreamID})
switch {
case err == nil:
    for _, r := range res.Records {
        handle(r) // at-least-once: dedupe by offset
    }
    cursor.StreamID, cursor.Offset = res.StreamID, res.ResumeOffset
    persist(cursor) // after handling, not before
case errors.Is(err, kuttidb.ErrStreamRecreated):
    sendResyncEvent()                    // incarnation changed
    cursor = kuttidb.StreamReplayCursor{Partition: 0}
case errors.Is(err, kuttidb.ErrStreamOffsetExpired),
    errors.Is(err, kuttidb.ErrStreamOffsetAhead):
    rebuildFromAuthoritativeState()
    cursor.StreamID, cursor.Offset = res.StreamID, res.NextOffset
}
```

A gap never returns records and never advances a cursor; the application
explicitly rebuilds at the base or the tail. Convert a last-delivered
offset to the next offset with `ReplayCursorFrom` (overflow-validated).
KuttiDB reports the gap; it cannot reconstruct expired events.

## Validation

```sh
cd clients/go
CGO_ENABLED=0 go test ./... -count=1
CGO_ENABLED=1 go test ./... -count=1
CGO_ENABLED=1 go test -race ./... -count=1
go vet ./...
CGO_ENABLED=1 go test -tags kuttidb_embed ./... -count=1
```

`make go-embed-smoke` and `make go-external-consumer` (repository root) run
the tagged embed smoke and the clean external-consumer matrix.