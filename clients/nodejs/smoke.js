// smoke.js — KuttiDB Node.js client smoke test (run by `make test`).
// Usage: node smoke.js [port]

"use strict";

const { Client, KuttiDBError } = require("./kuttidb_client");

const port = Number(process.argv[2] || 7379);

async function main() {
  const db = new Client({ port, poolSize: 2 });
  try {
    const caps = await db.capabilities();
    if (caps.major !== 1) throw new Error(`unexpected protocol major ${caps.major}`);

    // cache
    await db.put("greeting", Buffer.from("hello"), { ttl: 60 });
    const v = await db.get("greeting");
    if (v === null || v.toString() !== "hello") throw new Error("cache roundtrip failed");
    await db.putMany([["a", "1"], ["b", "2"]]);
    const many = await db.getMany(["a", "b", "missing"]);
    if (many[0].toString() !== "1" || many[1].toString() !== "2" || many[2] !== null)
      throw new Error("batch roundtrip failed");
    if (await db.delete("a") !== true) throw new Error("delete failed");

    // queues
    await db.queueDeclare("jobs", { durable: true, maxDepth: 100 });
    const id = await db.queuePublish("jobs", Buffer.from("task-1"));
    if (id <= 0n) throw new Error("queue publish failed");
    const msg = await db.queueConsume("jobs", { visibility: 5 });
    if (!msg || msg.value.toString() !== "task-1") throw new Error("queue consume failed");
    if (!(await db.queueAck("jobs", msg.id))) throw new Error("queue ack failed");
    const qs = await db.queueStats("jobs");
    console.log("queue stats:", JSON.stringify(qs));

    // exchanges
    await db.exchangeDeclare("events", { type: "topic" });
    await db.exchangeBind("events", "jobs", "order.*");
    const routed = await db.exchangePublish("events", "order.created", Buffer.from("evt"));
    if (routed !== 1) throw new Error(`exchange routed ${routed}, expected 1`);

    // atomic cache-plus-messaging
    const tx = await db.putAndEnqueue("job:1", Buffer.from("payload"), { queue: "jobs" });
    if (tx.unroutable || tx.txId === 0n) throw new Error("atomic put+enqueue failed");
    const upd = await db.updateAndEmit("job:1", Buffer.from("v2"), { exchange: "events", routingKey: "order.x" });
    if (upd.unroutable || upd.routed !== 1) throw new Error("atomic update+emit failed");
    const miss = await db.updateAndEmit("absent", Buffer.from("v"), { exchange: "events", routingKey: "order.x" });
    if (!miss.unroutable) throw new Error("atomic update on missing key must MISS");

    // singleflight + SWR
    const loaded = await db.getOrLoad("sf-key", () => Buffer.from("loaded"), { ttl: 30 });
    if (loaded.toString() !== "loaded") throw new Error("getOrLoad failed");
    await db.putSwr("swr-key", Buffer.from("v1"), { ttl: 30, staleFor: 60 });
    const swr = await db.getOrRefresh("swr-key");
    if (swr.state !== "value" || swr.value.toString() !== "v1") throw new Error("swr fresh read failed");

    // streams
    await db.streamDeclare("events-stream", { partitions: 2 });
    const rec = await db.streamAppend("events-stream", Buffer.from("record"), { key: "order:1" });
    const recs = await db.streamFetch("events-stream", { partition: Number(rec.partition), offset: 0 });
    if (!recs.length || recs[0].value.toString() !== "record") throw new Error("stream roundtrip failed");

    const stats = await db.stats();
    if (typeof stats.count !== "number" || stats.count < 1) throw new Error("stats wrong");

    console.log("NODE CLIENT OK");
  } finally {
    await db.close();
  }
}

main().catch((e) => {
  console.error("node smoke failed:", e && e.message ? e.message : e);
  process.exit(1);
});
