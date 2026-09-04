// KuttiDB Node.js usage examples — cache, queues, exchanges, atomic
// operations, single-flight, and streams.
//
//   ./kuttidb 7391 /tmp/examples.wal 100
//   node examples/node_examples.js 7391

"use strict";

const { Client } = require("../clients/nodejs/kuttidb_client");

async function cacheBasics(db) {
  await db.put("user:1", Buffer.from('{"name":"ada"}'), { ttl: 60 });
  console.log("get:", (await db.get("user:1")).toString());
  await db.putMany([["user:2", "bob"], ["user:3", "eve"]]);
  console.log("batch:", (await db.getMany(["user:1", "user:2", "user:4"])).map(v => v && v.toString()));
  await db.delete("user:3");
}

async function workQueue(db) {
  await db.queueDeclare("emails", { durable: true, maxDepth: 10000,
                                    deadLetterQueue: "emails.dead", maxDeliveries: 3 });
  await db.queuePublish("emails", Buffer.from("send:welcome:42"));
  const delivery = await db.queueConsume("emails", { visibility: 5 });
  console.log("delivery:", delivery.value.toString(), "redelivered:", delivery.redelivered);
  await db.queueAck("emails", delivery.id);
}

async function pubsubFanout(db) {
  await db.exchangeDeclare("events.fanout", { type: "fanout" });
  for (const name of ["audit", "metrics"]) {
    await db.queueDeclare(name, { durable: true });
    await db.exchangeBind("events.fanout", name, "");
  }
  console.log("fanout copies:", await db.exchangePublish("events.fanout", "", Buffer.from("user signed up")));
  for (const name of ["audit", "metrics"]) {
    const d = await db.queueConsume(name);
    await db.queueAck(name, d.id);
  }
}

async function topicRouting(db) {
  await db.exchangeDeclare("events.topic", { type: "topic" });
  await db.queueDeclare("orders.all", { durable: true });
  await db.queueDeclare("orders.eu", { durable: true });
  await db.exchangeBind("events.topic", "orders.all", "order.#");
  await db.exchangeBind("events.topic", "orders.eu", "order.eu.*");
  console.log("all+eu:", await db.exchangePublish("events.topic", "order.eu.created", Buffer.from("x")));
  console.log("all only:", await db.exchangePublish("events.topic", "order.us.created", Buffer.from("x")));
  console.log("unroutable:", await db.exchangePublish("events.topic", "refund.created", Buffer.from("x")));
}

async function atomicCachePlusEvent(db) {
  await db.exchangeDeclare("orders.events", { type: "topic" });
  await db.queueDeclare("order.events", { durable: true });
  await db.exchangeBind("orders.events", "order.events", "order.created");
  // Cache mutation and message commit together: after a crash both exist or
  // neither does, under the returned durable commit id.
  const committed = await db.putAndPublish("order:123", Buffer.from('{"total":42}'),
    { exchange: "orders.events", routingKey: "order.created", ttl: 3600 });
  console.log("commit id:", committed.txId, "routed:", committed.routed);
  const updated = await db.updateAndEmit("order:123", Buffer.from('{"total":50}'),
    { exchange: "orders.events", routingKey: "order.created" });
  console.log("update commit:", updated.txId, "routed:", updated.routed);
  const missing = await db.updateAndEmit("order:404", Buffer.from("{}"),
    { exchange: "orders.events", routingKey: "order.created" });
  console.log("missing key commits nothing:", missing.unroutable);
  const delivery = await db.queueConsume("order.events", { visibility: 30 });
  await db.queueAck("order.events", delivery.id);
}

async function singleflight(db) {
  let calls = 0;
  const loaded = await db.getOrLoad("weather:istanbul", async () => {
    calls++;
    return Buffer.from("weather: 21C");
  }, { ttl: 30 });
  await db.getOrLoad("weather:istanbul", async () => { calls++; });
  console.log("loaded:", loaded.toString(), "loader runs:", calls);
}

async function singleflightSwr(db) {
  await db.putSwr("weather:paris", Buffer.from("18C"), { ttl: 30, staleFor: 60 });
  const r = await db.getOrRefresh("weather:paris");
  console.log("swr state:", r.state, "value:", r.value.toString());
  if ((r.state === "stale" || r.state === "refresh") && r.holder) {
    await db.putSwr("weather:paris", Buffer.from("19C"), { ttl: 30, staleFor: 60 });
    await db.releaseClaim("weather:paris");
  }
}

async function streams(db) {
  await db.streamDeclare("user.events", { partitions: 4 });
  for (let i = 0; i < 3; i++) {
    await db.streamAppend("user.events", Buffer.from("event-" + i), { key: "user:" + (i % 2) });
  }
  const records = await db.streamFetch("user.events", { partition: 0, offset: 0 });
  console.log("replay:", records.map(r => r.value.toString()));
}

async function consumerGroups(db) {
  await db.streamDeclare("clicks", { partitions: 2 });
  const join = await db.streamGroupJoin("clicks", "analytics");
  console.log("assignment:", join.partitions, "generation:", join.generation);
  for (const p of join.partitions) {
    for (const record of await db.streamFetch("clicks", { partition: p, offset: 0 })) {
      await db.streamCommit("clicks", "analytics", p, record.offset + 1n);
    }
  }
  console.log("lag:", await db.streamGroupLag("clicks", "analytics", 0));
  await db.streamGroupLeave("clicks", "analytics");
}

async function main() {
  const port = Number(process.argv[2] || 7391);
  const db = new Client({ port, poolSize: 2 });
  try {
    await cacheBasics(db);
    await workQueue(db);
    await pubsubFanout(db);
    await topicRouting(db);
    await atomicCachePlusEvent(db);
    await singleflight(db);
    await singleflightSwr(db);
    await streams(db);
    await consumerGroups(db);
    console.log("ALL EXAMPLES OK");
  } finally {
    await db.close();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
