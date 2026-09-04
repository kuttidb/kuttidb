"""KuttiDB usage examples — cache, queues, exchanges, atomic operations,
single-flight, and streams against a running server.

Run one server, then this script:

    ./kuttidb 7390 /tmp/examples.wal 100
    python3 examples/python_examples.py 7390

Every pattern from the product docs is a self-contained function.
"""

import sys
import time

sys.path.insert(0, __file__.rsplit("/", 2)[0] + "/src")
from kuttidb_client import KuttiDBClient  # noqa: E402


def kuttidb_basics(db: KuttiDBClient) -> None:
    """Plain cache: put, get, delete, TTL expiry, batch operations."""
    db.put("user:1", b'{"name":"ada"}', ttl=60)
    print("get:", db.get("user:1"))
    db.put_many([("user:2", b"bob"), ("user:3", b"eve")])
    print("batch get:", db.get_many(["user:1", "user:2", "user:4"]))
    db.delete("user:3")
    db.put("session:1", b"short-lived", ttl=0.5)
    time.sleep(0.7)
    print("expired:", db.get("session:1"))


def kuttidb_eviction_budget(db: KuttiDBClient) -> None:
    """The memory budget (server argument MAX_MEM_MB) bounds cache memory;
    eviction applies to cache entries only, never to durable data."""
    stats = db.stats()
    print("mem_bytes:", stats["mem_bytes"], "budget:", stats["mem_bytes"] > 0)


def work_queue(db: KuttiDBClient) -> None:
    """Durable work queue with visibility timeout, ACK, NACK, redelivery."""
    db.queue_declare("emails", durable=True, max_depth=10_000,
                     dead_letter_queue="emails.dead", max_deliveries=3)
    db.queue_publish("emails", b"send:welcome:42")
    delivery = db.queue_consume("emails", visibility=1.0)
    print("delivery:", delivery["message_id"], delivery["value"],
          "redelivered:", delivery["redelivered"])
    # A NACK with requeue=True returns the message; after the visibility
    # timeout (or immediately with requeue=True) it is delivered again.
    db.queue_nack("emails", delivery["id"], requeue=True)
    redelivered = db.queue_consume("emails", visibility=5.0)
    assert redelivered["redelivered"], "second delivery must be flagged"
    db.queue_ack("emails", redelivered["id"])
    print("acked; depth:", db.queue_stats("emails"))


def pubsub_fanout(db: KuttiDBClient) -> None:
    """Fanout exchange: every bound queue gets its own durable copy."""
    db.exchange_declare("events.fanout", type="fanout")
    for name in ("audit", "metrics"):
        db.queue_declare(name, durable=True)
        db.exchange_bind("events.fanout", name, "")
    routed = db.exchange_publish("events.fanout", "", b"user signed up")
    print("fanout copies:", routed)  # 2
    for name in ("audit", "metrics"):
        d = db.queue_consume(name)
        db.queue_ack(name, d["id"])
        print(f"{name} got:", d["value"])


def topic_routing(db: KuttiDBClient) -> None:
    """Topic exchange: '*' matches one word, '#' matches zero or more."""
    db.exchange_declare("events.topic", type="topic")
    db.queue_declare("orders.all", durable=True)
    db.queue_declare("orders.eu", durable=True)
    db.exchange_bind("events.topic", "orders.all", "order.#")
    db.exchange_bind("events.topic", "orders.eu", "order.eu.*")
    print("routed all+eu:", db.exchange_publish("events.topic", "order.eu.created", b"x"))
    print("routed all only:", db.exchange_publish("events.topic", "order.us.created", b"x"))
    print("unroutable:", db.exchange_publish("events.topic", "refund.created", b"x"))


def atomic_cache_plus_event(db: KuttiDBClient) -> None:
    """Signature feature: a cache mutation and a message commit together.
    After a crash, recovery shows both or neither, with a durable commit id."""
    db.exchange_declare("orders.events", type="topic")
    db.queue_declare("order.events", durable=True)
    db.exchange_bind("orders.events", "order.events", "order.created")
    result = db.put_and_publish("order:123", b'{"total":42}',
                                exchange="orders.events", routing_key="order.created",
                                ttl=3600)
    print("commit id:", result["tx_id"], "routed:", result["routed"])
    # UPDATE_AND_EMIT is conditional: an existing key is replaced and the
    # event is emitted under the same commit guarantee; a missing key
    # commits nothing.
    updated = db.update_and_emit("order:123", b'{"total":50}',
                                 exchange="orders.events", routing_key="order.created")
    print("update commit:", updated["tx_id"], "routed:", updated["routed"])
    missing = db.update_and_emit("order:404", b"{}",
                                 exchange="orders.events", routing_key="order.created")
    print("missing key ->", missing)  # tx_id 0, nothing committed
    delivery = db.queue_consume("order.events", visibility=30)
    print("event for committed update:", delivery["value"])


def singleflight_loading(db: KuttiDBClient) -> None:
    """Anti-cache-stampede: one requester loads, others wait server-side."""
    calls = []

    def expensive_loader():
        calls.append(1)
        time.sleep(0.05)
        return b"weather: 21C"

    value = db.get_or_load("weather:istanbul", expensive_loader,
                           ttl=30, lease=5.0, wait=10.0)
    print("loaded:", value, "loader runs:", len(calls))
    # Now cached: the loader does not run again.
    value = db.get_or_load("weather:istanbul", expensive_loader, ttl=30)
    print("cached:", value, "loader runs:", len(calls))


def singleflight_swr(db: KuttiDBClient) -> None:
    """Stale-while-revalidate: expired values are served immediately while
    exactly one caller revalidates; refresh-ahead revalidates before expiry."""
    db.put_swr("weather:paris", b"18C", ttl=1.0, stale_for=60.0)
    time.sleep(1.1)  # past the TTL; plain GET now misses
    print("plain GET (expired):", db.get("weather:paris"))
    result = db.get_or_refresh("weather:paris")
    if result["state"] == "stale":
        print("stale served:", result["value"], "holder:", result["holder"])
        if result["holder"]:
            db.put_swr("weather:paris", b"19C", ttl=30.0, stale_for=60.0)
            db.release_claim("weather:paris")
    print("fresh:", db.get_or_refresh("weather:paris"))


def streams(db: KuttiDBClient) -> None:
    """Partitioned append logs with keyed ordering and replay."""
    db.stream_declare("user.events", partitions=4)
    for i in range(3):
        db.stream_append("user.events", f"event-{i}".encode(), key=f"user:{i % 2}")
    # Replay from the beginning of partition 0.
    records = db.stream_fetch("user.events", partition=0, offset=0)
    print("replay:", [(r["offset"], r["value"]) for r in records])


def consumer_groups(db: KuttiDBClient) -> None:
    """Consumer groups: leased partition assignment and offset commits."""
    db.stream_declare("clicks", partitions=2)
    join = db.stream_group_join("clicks", "analytics")
    print("assignment:", join.partitions, "generation:", join.generation)
    for partition in join.partitions:
        records = db.stream_fetch("clicks", partition=partition, offset=0)
        for record in records:
            db.stream_commit("clicks", "analytics", partition, record["offset"] + 1)
    print("lag:", db.stream_group_lag("clicks", "analytics", 0))
    db.stream_group_leave("clicks", "analytics")


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7390
    with KuttiDBClient(port=port) as db:
        kuttidb_basics(db)
        kuttidb_eviction_budget(db)
        work_queue(db)
        pubsub_fanout(db)
        topic_routing(db)
        atomic_cache_plus_event(db)
        singleflight_loading(db)
        singleflight_swr(db)
        streams(db)
        consumer_groups(db)
        print("ALL EXAMPLES OK")


if __name__ == "__main__":
    main()
