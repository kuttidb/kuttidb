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
    if (!(await db.health())) throw new Error("health check failed");

    // cache
    await db.put("greeting", Buffer.from("hello"), { ttl: 60 });
    const v = await db.get("greeting");
    if (v === null || v.toString() !== "hello") throw new Error("cache roundtrip failed");
    await db.putMany([["a", "1"], ["b", "2"]]);
    await db.putManyTTL([["ttl-a", "ta", 60], ["ttl-b", "tb", null]]);
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
    const queueInventory = await db.queueList();
    if (!queueInventory.some((q) => q.name === "jobs")) throw new Error("queue list failed");
    const batchIds = await db.queuePublishBatch("jobs", [Buffer.from("batch-1"), Buffer.from("batch-2")]);
    if (batchIds.length !== 2) throw new Error("queue batch publish failed");
    const batchMessages = await db.queueConsumeBatch("jobs", 2);
    if (batchMessages.length !== 2) throw new Error("queue batch consume failed");
    if (await db.queueAckBatch("jobs", batchMessages.map((m) => m.id)) !== 2)
      throw new Error("queue batch ACK failed");
    await db.queuePublishBatch("jobs", [Buffer.from("retry-1"), Buffer.from("retry-2")]);
    const retryMessages = await db.queueConsumeBatch("jobs", 2);
    if (await db.queueNackBatch("jobs", retryMessages.map((m) => m.id), { requeue: false }) !== 2)
      throw new Error("queue batch NACK failed");
    await db.queueConsumerRegister("worker-1");
    await db.queuePublish("jobs", Buffer.from("named-consumer"));
    const named = await db.queueConsumeAs("jobs", "worker-1", { visibility: 5 });
    if (!named || !(await db.queueNack("jobs", named.id, { requeue: false })))
      throw new Error("named consumer/NACK failed");
    await db.queueConsumerUnregister("worker-1");

    // exchanges
    await db.exchangeDeclare("events", { type: "topic" });
    await db.exchangeBind("events", "jobs", "order.*");
    const routed = await db.exchangePublish("events", "order.created", Buffer.from("evt"));
    if (routed !== 1) throw new Error(`exchange routed ${routed}, expected 1`);
    await db.queueDeclare("unrouted", { durable: true });
    await db.exchangeDeclare("fallback", { type: "fanout" });
    await db.exchangeBind("fallback", "unrouted");
    await db.exchangeDeclare("primary", { type: "direct", alternateExchange: "fallback" });
    if (await db.exchangePublish("primary", "missing", Buffer.from("fallback-event")) !== 1)
      throw new Error("alternate exchange routing failed");

    // atomic cache-plus-messaging
    const tx = await db.putAndEnqueue("job:1", Buffer.from("payload"), { queue: "jobs" });
    if (tx.unroutable || tx.txId === 0n) throw new Error("atomic put+enqueue failed");
    const published = await db.putAndPublish("job:2", Buffer.from("v1"),
      { exchange: "events", routingKey: "order.x" });
    if (published.unroutable || published.routed !== 1) throw new Error("atomic put+publish failed");
    const deleted = await db.deleteAndPublish("job:2",
      { exchange: "events", routingKey: "order.x", message: Buffer.from("deleted") });
    if (deleted.unroutable || deleted.routed !== 1) throw new Error("atomic delete+publish failed");
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
    const swrLoaded = await db.getOrLoadSwr("swr-loaded",
      () => Buffer.from("loaded-swr"), { ttl: 30, staleFor: 60 });
    if (swrLoaded.toString() !== "loaded-swr") throw new Error("getOrLoadSwr failed");

    // streams
    await db.streamDeclare("events-stream", { partitions: 2 });
    const rec = await db.streamAppend("events-stream", Buffer.from("record"), { key: "order:1" });
    const appended = await db.streamAppendMany("events-stream",
      [["order:2", "record-2"], ["order:3", "record-3"]], { partition: Number(rec.partition) });
    if (appended.length !== 2) throw new Error("stream batch append failed");
    const recs = await db.streamFetch("events-stream", { partition: Number(rec.partition), offset: 0 });
    if (!recs.length || recs[0].value.toString() !== "record" || recs[0].key.toString() !== "order:1")
      throw new Error("keyed stream roundtrip failed");
    const assignment = await db.streamGroupJoin("events-stream", "workers", { lease: 30 });
    if (!assignment.partitions.length) throw new Error("stream group join failed");
    await db.streamCommit("events-stream", "workers", Number(rec.partition), 1);
    if (await db.streamGroupOffset("events-stream", "workers", Number(rec.partition)) !== 1n)
      throw new Error("stream group offset failed");
    await db.streamCommitBatch("events-stream", "workers", [[Number(rec.partition), 2]]);
    const lag = await db.streamGroupLag("events-stream", "workers", Number(rec.partition));
    if (lag === null) throw new Error("stream group lag failed");
    const streamInventory = await db.streamList();
    if (!streamInventory.some((s) => s.topic === "events-stream")) throw new Error("stream list failed");
    const groupInventory = await db.streamGroupList();
    if (!groupInventory.some((g) => g.topic === "events-stream" && g.group === "workers"))
      throw new Error("stream group list failed");
    await db.streamGroupLeave("events-stream", "workers");

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
