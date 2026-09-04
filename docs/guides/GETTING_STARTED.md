# Getting started with KuttiDB

KuttiDB is meant to be easy to start: run one executable, connect with a
client, and store data. You do not need a server cluster, a schema, or a
separate queue service to begin.

This guide uses Python because the included client makes the examples short.
The same server also works with the Go, Java, Rust, and Node.js clients in
[`clients/`](../../clients/).

## 1. Build and run it

From the project directory:

```sh
make
./kuttidb 7379 kuttidb.wal
```

Leave that terminal running. KuttiDB is now listening on `127.0.0.1:7379`.
The `kuttidb.wal` file is its local recovery log: keep it if you want durable
Queue and Stream data to survive a restart.

For a throwaway local experiment, use `-` instead of a file:

```sh
./kuttidb 7379 -
```

That is fast and simple, but data is not recovered after the server stops.

## 2. Put and get a value

Open a second terminal in this project and run:

```sh
PYTHONPATH=src python3
```

Then:

```python
from kuttidb_client import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    db.put("name", b"Ada")
    print(db.get("name"))       # b'Ada'
    print(db.get("missing"))    # None
```

Values are bytes, which keeps KuttiDB predictable for text, JSON, images, and
other binary data. Encode text when writing it and decode it when reading it:

```python
with KuttiDBClient(port=7379) as db:
    db.put("welcome", "hello".encode())
    print(db.get("welcome").decode())  # hello
```

To expire a value automatically, add a TTL in seconds:

```python
db.put("one-time-code", b"123456", ttl=60)
```

## 3. Use a Queue for background work

A Queue is useful when one part of your app creates work and another part
processes it. First declare the Queue once:

```python
with KuttiDBClient(port=7379) as db:
    db.queue_declare("emails", durable=True)
```

Send work to it:

```python
with KuttiDBClient(port=7379) as db:
    db.queue_publish("emails", b"welcome@example.com")
```

Process one message and acknowledge it only after the work succeeds:

```python
with KuttiDBClient(port=7379) as db:
    message = db.queue_consume("emails", visibility=30)
    if message:
        try:
            print("send email to", message["value"].decode())
            db.queue_ack("emails", message["id"])
        except Exception:
            db.queue_nack("emails", message["id"], requeue=True)
```

`visibility=30` means the message becomes available again if the worker does
not acknowledge it within 30 seconds. That makes a worker crash recoverable.

## 4. Use a Stream for an ordered event log

Streams keep records in order and let readers resume from an offset. They are
a good fit for audit events, activity feeds, and event-driven projections.

```python
with KuttiDBClient(port=7379) as db:
    db.stream_declare("orders", partitions=1)
    db.stream_append("orders", b'{"order_id": 42, "event": "created"}')

    records = db.stream_fetch("orders", partition=0, offset=0)
    for record in records:
        print(record["offset"], record["value"])
```

Start with one partition unless you need more write throughput. Ordering is
guaranteed within a partition, not across several partitions.

## The three things to remember

| Need | Use |
|---|---|
| A value you can read by key | `put` and `get` |
| Work that one worker should finish | Queue + ACK/NACK |
| An ordered history that readers can replay | Stream + offset |

## Where to go next

- [README.md](../../README.md) for the project overview and all supported clients.
- [QUEUES.md](../messaging/QUEUES.md) for retry, dead-letter, and delivery details.
- [STREAMS.md](../messaging/STREAMS.md) for partitions, retention, and Consumer Groups.
- [MANAGEMENT_API.md](../api/MANAGEMENT_API.md) when you need an authenticated admin
  API for dashboards or automation.
# Managed local mode

For a single-host application, the Python client can own one local KuttiDB
service without a separate supervisor. It uses an owner-only Unix socket,
starts only when that endpoint is absent, and stops after the final native
client connection has been closed for the configured grace period.

```python
from kuttidb_client import KuttiDBClient

with KuttiDBClient.managed(data_dir="./data/kuttidb", idle_timeout=60) as db:
    db.put("greeting", b"hello")
```

This is intentionally opt-in. Existing `KuttiDBClient(...)` constructors only
connect; they never start or stop a server. Managed mode is for one machine and
one application-owned data directory, not multi-container or remote-server
coordination. Do not replay an operation after a connection failure.

Unix sockets are the recommended default. When a local TCP listener is needed,
set `transport="tcp"` and pass a literal `127.x.x.x` host and port to
`KuttiDBClient.managed`; managed mode rejects DNS and non-loopback endpoints.

For advanced local configuration, construct `ServerParams` and pass it as
`server=` to `KuttiDBClient`. It provides typed durability/fsync, resource
limit, auth-file, TLS (TCP only), queue/stream WAL, metrics, and admin settings;
the launcher forwards only those explicit settings and never accepts token
values or an arbitrary command string.
