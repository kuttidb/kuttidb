// job_smoke.js — KuttiDB Node.js atomic job completion smoke test.
// Usage: node job_smoke.js [kuttidb-executable]
//
// Spawns ./kuttidb <port> <wal> --job-completion on an ephemeral free port,
// exercises the typed job surface (manifest, completion-capable consume,
// intent serialization, atomic completion, receipts), restarts the server on
// the same WAL, and replays the persisted intent expecting the same commit.

"use strict";

const { spawn } = require("child_process");
const net = require("net");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { Client, JobCompletionIntent, JobIdempotencyConflictError,
        JobStateVersionConflictError, JobDeliveryExpiredError,
        JobUnsupportedFeatureError } = require("./kuttidb_client");

const executable = process.argv[2] || process.env.KUTTIDB_SERVER ||
  path.join(__dirname, "..", "..", "kuttidb");

function freeLoopbackPort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close((error) => error ? reject(error) : resolve(port));
    });
  });
}

function probeTcp(port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => { socket.destroy(); resolve(); });
    socket.once("error", (error) => { socket.destroy(); reject(error); });
  });
}

function startServer(walDir, port, jobs) {
  const args = [String(port), path.join(walDir, "kuttidb.wal")];
  if (jobs) args.push("--job-completion");
  return spawn(executable, args, { stdio: ["ignore", "pipe", "pipe"] });
}

async function waitReady(child, port) {
  const deadline = Date.now() + 10000;
  for (;;) {
    if (child.exitCode !== null)
      throw new Error(`server exited early (${child.exitCode})`);
    try {
      await probeTcp(port);
      return;
    } catch (e) {
      if (Date.now() > deadline) throw new Error("server did not start");
      await new Promise((r) => setTimeout(r, 50));
    }
  }
}

function stopServer(child) {
  return new Promise((resolve) => {
    if (child.exitCode !== null) return resolve();
    child.once("exit", resolve);
    child.kill("SIGTERM");
    setTimeout(() => { try { child.kill("SIGKILL"); } catch { /* gone */ } }, 5000);
  });
}

async function withClient(port, fn) {
  const db = new Client({ port, poolSize: 2 });
  try {
    return await fn(db);
  } finally {
    await db.close();
  }
}

async function main() {
  const port = await freeLoopbackPort();
  const walDir = fs.mkdtempSync(path.join(os.tmpdir(), "kuttidb-job-"));
  const plainDir = fs.mkdtempSync(path.join(os.tmpdir(), "kuttidb-plain-"));
  let server = startServer(walDir, port, true);
  try {
    await waitReady(server, port);
    await withClient(port, async (db) => {
      const caps = await db.capabilities();
      if (caps.major !== 1 || caps.minor < 8)
        throw new Error(`unexpected protocol ${caps.major}.${caps.minor}`);

      // Queues + stable identity discovery
      await db.queueDeclare("extract-pdf", { durable: true });
      await db.queueDeclare("index-text", { durable: true });
      const manifest = new Map((await db.queueManifest()).map((q) => [q.name, q]));
      if (!manifest.get("extract-pdf") || !manifest.get("extract-pdf").durable)
        throw new Error("queue manifest missing extract-pdf");
      const inInc = manifest.get("extract-pdf").incarnation;
      const outInc = manifest.get("index-text").incarnation;
      if (inInc <= 0n || outInc <= 0n) throw new Error("manifest incarnations missing");
      for (const key of ["depth", "inflight", "maxDepth", "revision"])
        if (typeof manifest.get("extract-pdf")[key] !== "bigint")
          throw new Error(`manifest ${key} must be BigInt`);

      await db.queueConsumerRegister("pdf-worker");
      const msgId = await db.queuePublish("extract-pdf", Buffer.from("pdf-bytes"));
      const delivery = await db.jobConsume("extract-pdf", "pdf-worker");
      if (!delivery || delivery.messageId !== msgId)
        throw new Error("job consume failed");
      if (delivery.queueIncarnation !== inInc)
        throw new Error("delivery incarnation mismatch");
      if (delivery.proof.length !== 16 || delivery.storeId.length !== 16)
        throw new Error("delivery identity shapes wrong");

      // Compose + persist the intent BEFORE submitting (recovery path).
      const intent = delivery.toIntent({
        stateKey: "pdf:42", expectedVersion: 0, stateValue: "extracted",
        outputQueue: "index-text", outputIncarnation: outInc,
        outputValue: "pdf:42",
      });
      const persisted = JSON.stringify(intent); // toJSON: lossless decimal strings
      const revived0 = JobCompletionIntent.fromJS(JSON.parse(persisted));
      if (!revived0.operationId.equals(intent.operationId) ||
          revived0.messageId !== intent.messageId ||
          revived0.queueIncarnation !== intent.queueIncarnation ||
          !revived0.stateValue.equals(intent.stateValue))
        throw new Error("intent JSON roundtrip mismatch");

      // 64-bit fields survive JSON beyond Number.MAX_SAFE_INTEGER.
      const huge = (1n << 53n) + 1n;
      const probe = new JobCompletionIntent({ operationId: null, queue: "q",
        queueIncarnation: huge, messageId: huge + 7n, stateKey: "k",
        expectedVersion: 0, stateValue: Buffer.alloc(0) });
      const revivedHuge = JobCompletionIntent.fromJS(JSON.parse(JSON.stringify(probe)));
      if (revivedHuge.messageId !== huge + 7n || revivedHuge.queueIncarnation !== huge)
        throw new Error("intent JSON lost 64-bit precision");

      const result = await db.jobComplete(intent, delivery.proof);
      if (result.replayed || result.commitId <= 0n)
        throw new Error("job completion not committed");
      if (result.outputMessageId <= 0n || result.stateVersion !== 1n)
        throw new Error("job completion result wrong");

      const state = await db.stateGet("pdf:42");
      if (!state || state.value.toString() !== "extracted" ||
          state.version !== result.stateVersion ||
          state.commitId !== result.commitId)
        throw new Error("durable state read mismatch");

      if ((await db.queueStats("extract-pdf")).depth !== 0)
        throw new Error("input queue did not drain");
      if ((await db.queueStats("index-text")).depth !== 1)
        throw new Error("output queue missing the published message");

      const receipt = await db.jobCompletion(intent.operationUuid);
      if (!receipt || receipt.commitId !== result.commitId)
        throw new Error("receipt lookup failed");
      if (await db.jobCompletion(randomUuid()) !== null)
        throw new Error("unknown receipt lookup must miss");

      // Restart: the same persisted intent replays its original result
      // (the proof is stale; receipt lookup precedes lease validation).
      await stopServer(server);
      server = startServer(walDir, port, true);
      await waitReady(server, port);
      await withClient(port, async (db2) => {
        const revived = JobCompletionIntent.fromJS(JSON.parse(persisted));
        const replay = await db2.jobComplete(revived, Buffer.alloc(16));
        if (!replay.replayed || replay.commitId !== result.commitId ||
            replay.stateVersion !== result.stateVersion ||
            replay.outputMessageId !== result.outputMessageId)
          throw new Error("replay did not return the original result");
        if ((await db2.queueStats("index-text")).depth !== 1)
          throw new Error("replay must not republish");

        // Direct state mutations with the shared receipt ledger.
        const put = await db2.statePut("pdf:42", "corrected", { expectedVersion: 1 });
        if (put.replayed || put.stateVersion !== 2n)
          throw new Error("state put failed");
        const putReplay = await db2.statePut("pdf:42", "corrected",
          { expectedVersion: 1, operationId: put.operationId });
        if (!putReplay.replayed || putReplay.commitId !== put.commitId)
          throw new Error("state put replay failed");

        let conflict = null;
        try {
          await db2.statePut("pdf:42", "different",
            { expectedVersion: 2, operationId: put.operationId });
        } catch (e) {
          if (e instanceof JobIdempotencyConflictError) conflict = e;
        }
        if (!conflict || conflict.code !== "idempotency_conflict" ||
            conflict.outcome !== "not_committed")
          throw new Error("idempotency conflict not typed");

        let stale = null;
        try {
          await db2.statePut("pdf:42", "x", { expectedVersion: 99 });
        } catch (e) {
          if (e instanceof JobStateVersionConflictError) stale = e;
        }
        if (!stale || stale.code !== "state_version_conflict")
          throw new Error("version conflict not typed");

        const lookup = await db2.durableOperation(put.operationId);
        if (!lookup || lookup.kind !== "state_put" || lookup.stateVersion !== 2n)
          throw new Error("durable operation lookup failed");

        const deletion = await db2.stateDelete("pdf:42", { expectedVersion: 2 });
        if (deletion.replayed || deletion.kind !== "state_delete")
          throw new Error("state delete failed");
        if (await db2.stateGet("pdf:42") !== null)
          throw new Error("deleted state still present");
        const deleteReplay = await db2.stateDelete("pdf:42",
          { expectedVersion: 2, operationId: deletion.operationId });
        if (!deleteReplay.replayed || deleteReplay.commitId !== deletion.commitId)
          throw new Error("state delete replay failed");

        // Fencing: a fresh delivery with a tiny lease expires.
        await db2.queueConsumerRegister("pdf-worker");
        await db2.queuePublish("extract-pdf", Buffer.from("second"));
        const second = await db2.jobConsume("extract-pdf", "pdf-worker",
          { visibility: 0.001 });
        if (!second) throw new Error("second consume failed");
        await new Promise((r) => setTimeout(r, 50));
        let expired = null;
        try {
          await db2.jobComplete(
            second.toIntent({ stateKey: "pdf:42", expectedVersion: 0,
                              stateValue: "late" }),
            second.proof);
        } catch (e) {
          if (e instanceof JobDeliveryExpiredError) expired = e;
        }
        if (!expired || expired.code !== "delivery_expired")
          throw new Error("expired delivery not fenced");
      });
    });

    // Feature disabled on a fresh directory: typed unsupported error.
    const plainPort = await freeLoopbackPort();
    let plain = startServer(plainDir, plainPort, false);
    try {
      await waitReady(plain, plainPort);
      await withClient(plainPort, async (db) => {
        let unsupported = null;
        try {
          await db.jobConsume("extract-pdf", "pdf-worker");
        } catch (e) {
          if (e instanceof JobUnsupportedFeatureError) unsupported = e;
        }
        if (!unsupported || unsupported.code !== "unsupported_feature")
          throw new Error("disabled feature not typed");
      });
    } finally {
      await stopServer(plain);
    }
  } finally {
    await stopServer(server);
    fs.rmSync(walDir, { recursive: true, force: true });
    fs.rmSync(plainDir, { recursive: true, force: true });
  }

  console.log("NODE JOB CLIENT OK");
}

function randomUuid() {
  const { randomBytes } = require("crypto");
  return randomBytes(16).toString("hex").replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/,
    "$1-$2-$3-$4-$5");
}

main().catch((e) => {
  console.error("node job smoke failed:", e && e.message ? e.message : e);
  process.exit(1);
});
