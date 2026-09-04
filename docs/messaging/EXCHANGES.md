# Native exchanges and routing

KuttiDB exchanges are a routing layer in front of the native queue engine. An
exchange never stores messages: a publish resolves the matching bound queues
and writes one independent copy into each. Exchanges and queues live in
separate namespaces — an exchange and a queue may share a name. The queue-side
semantics (durability, ACK/NACK, visibility, dead-lettering) are unchanged and
are specified in [QUEUES.md](QUEUES.md). KuttiDB exchanges are native KuttiDB
commands, not AMQP.

## Exchange types

| Type | Matching |
|---|---|
| direct | binding key byte-equal to the routing key |
| fanout | every bound queue; the routing key is ignored |
| topic | binding key is a `.`-separated pattern; `*` matches exactly one word, `#` matches zero or more words |
| default | the unnamed exchange: the routing key names the target queue |

Topic rules: keys and patterns are split on `.`; every string has at least one
(possibly empty) word, so `""` is one empty word and `"a."` is `["a", ""]`.
`*` matches one word including an empty word; `#` matches any number of words
including zero. A pattern may contain at most 128 words; patterns beyond that
are rejected at bind time. Matching is a bounded dynamic program, so wildcard
patterns cannot backtrack catastrophically.

## Routing semantics

- A publish delivers **at most one copy per queue** even when a queue matches
  through several bindings.
- Each target queue keeps its own capacity, durability, TTL, and dead-letter
  behavior. A copy in a durable queue is durable regardless of which exchange
  routed it.
- Routing is evaluated once per publish against the current bindings; a
  publish never buffers responses or fans out beyond the binding cap.
- **Unroutable** (no bound queue matched, or every matched queue no longer
  exists): the publish reports zero routed copies (response status MISS) and
  the `exchange_unroutable` counter increments. This is an outcome, not an
  error.
- **Alternate exchange**: an exchange may declare an alternate exchange. When
  a publish routes to zero queues, the message is published into the
  alternate exchange once, with the same routing key. The alternate's own
  alternate is not followed (one hop), so AE cycles cannot loop. A missing
  alternate degrades to unroutable.
- **Fail closed**: if any target queue is full (bounded depth including
  in-flight deliveries), the whole publish fails with an error before any
  copy is written, and the publisher receives no confirmation. The same
  happens on persistence failure. A failure partway through a durable fanout
  may leave confirmed-record copies in some target queues after recovery, so
  exchange routing is **at-least-once**, never silently lossy.
- Publisher confirmation: a successful durable exchange publish is confirmed
  only after every durable target's record has been written and the queue WAL
  fsynced once. A non-durable exchange publishing into durable queues still
  produces durable copies (durability belongs to the target queue).

## Limits

- Exchange names: 1–255 bytes. Routing keys and patterns: 0–255 bytes.
- Bindings per exchange: 1024 (`EXCHANGE_BINDINGS_MAX`); this bounds the
  fanout of a single publish. Exceeding the cap fails the bind, not the
  publish.
- A bind requires the target queue to exist. A durable binding whose queue
  was non-durable survives a restart; the queue must be redeclared, and until
  then that binding routes nowhere.
- Message size uses the server value limit (64 MiB by default). Publish TTLs
  (milliseconds) travel with the message and expire independently in each
  target queue.

## Native protocol

All requests retain the existing framing `[op:1][name_len:2][value_len:4][name][value]`.

| Operation | Opcode | Name | Value | Successful response |
|---|---:|---|---|---|
| Exchange declare | `0x30` | exchange | `[durable:1][type:1]`, optionally `[ext_len:2][ext]`, `ext = [ae_len:2][ae_name]` | OK |
| Bind | `0x31` | exchange | `[queue_len:2][queue][key_len:2][key]` | OK |
| Unbind | `0x32` | exchange | `[queue_len:2][queue][key_len:2][key]` | OK / MISS when absent |
| Publish | `0x33` | exchange (empty = default) | `[key_len:2][ttl_ms:8][key][message]` | `[OK][len=4][routed:4 LE]`; MISS when unroutable; ERROR on failure |

`type`: `0` direct, `1` fanout, `2` topic. Redeclaring an existing exchange
with the same parameters is idempotent; any difference (type, durability,
alternate exchange) is refused. Binding the same (queue, key) pair twice is
idempotent. Declared durable exchanges, bindings, and unbinds are recorded in
the queue WAL and recovered across restart; non-durable exchange state is
in-memory only.

## Python example

```python
from kuttidb_client import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    db.queue_declare("orders.all", durable=True)
    db.queue_declare("orders.eu", durable=True)
    db.exchange_declare("events", type="topic", durable=True,
                        alternate_exchange="events.unroutable")
    db.exchange_bind("events", "orders.all", "orders.#")
    db.exchange_bind("events", "orders.eu", "orders.eu.*")

    routed = db.exchange_publish("events", "orders.eu.paid", payload)
    if routed == 0:
        ...  # unroutable: rerouted to the alternate exchange if declared
```

## Guarantees and limitations

Delivery through an exchange is at-least-once into each target queue and
inherits the queue engine's single-node durability: acknowledged durable
copies survive process crashes and clean restarts, but a single node does not
protect against disk or machine loss — that requires replication, which is
not yet implemented. There is no AMQP compatibility claim; per-message
publisher confirms with individual message IDs through an exchange are future
work (the response currently reports the routed copy count).
