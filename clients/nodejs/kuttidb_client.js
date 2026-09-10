// kuttidb_client.js — Node.js client for the KuttiDB binary protocol.
//
// Zero dependencies. A fixed pool of pipelined connections; each request
// resolves in order per connection. TLS and AUTH supported. Node.js >= 16.
//
//   const { Client } = require("./kuttidb_client");
//   const db = new Client({ port: 7379 });
//   await db.put("greeting", Buffer.from("hello"), { ttl: 60 });
//   console.log(await db.get("greeting"));
//   await db.close();

"use strict";

const net = require("net");
const tls = require("tls");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { execFile } = require("child_process");
const { promisify } = require("util");
const execFileAsync = promisify(execFile);

const OP = {
  PUT: 0x01, GET: 0x02, DELETE: 0x03, STATS: 0x04, PUT_TTL: 0x05,
  AUTH: 0x06, HEALTH: 0x09, CAPABILITIES: 0x0a, PUT_SWR: 0x0b, SERVER_INFO: 0x0c,
  PUT_BATCH: 0x11, GET_BATCH: 0x12, PUT_BATCH_TTL: 0x13,
  QUEUE_DECLARE: 0x20, QUEUE_PUBLISH: 0x21, QUEUE_CONSUME: 0x22,
  QUEUE_ACK: 0x23, QUEUE_NACK: 0x24, QUEUE_PUBLISH_TTL: 0x25,
  QUEUE_STATS: 0x26, QUEUE_PREFETCH: 0x27, QUEUE_CANCEL: 0x28,
  QUEUE_CONSUMER_REGISTER: 0x29, QUEUE_CONSUMER_UNREGISTER: 0x2a,
  QUEUE_CONSUME_AS: 0x2b, QUEUE_LIST: 0x2c,
  QUEUE_PUBLISH_BATCH: 0x2d, QUEUE_CONSUME_BATCH: 0x2e,
  QUEUE_ACK_BATCH: 0x2f,
  EXCHANGE_DECLARE: 0x30, EXCHANGE_BIND: 0x31, EXCHANGE_UNBIND: 0x32,
  EXCHANGE_PUBLISH: 0x33,
  ATOMIC_PUT_PUBLISH: 0x40, ATOMIC_PUT_ENQUEUE: 0x41,
  ATOMIC_DELETE_PUBLISH: 0x42, ATOMIC_UPDATE_EMIT: 0x43,
  SF_GET_OR_CLAIM: 0x50, SF_WAIT_FOR_KEY: 0x51, SF_PUT_AND_RELEASE: 0x52,
  SF_RELEASE_CLAIM: 0x53, SF_GET_OR_REFRESH: 0x54,
  STREAM_DECLARE: 0x60, STREAM_APPEND: 0x61, STREAM_FETCH: 0x62,
  STREAM_COMMIT: 0x63, STREAM_GROUP_OFFSET: 0x64, STREAM_GROUP_JOIN: 0x65,
  STREAM_GROUP_LAG: 0x66, STREAM_APPEND_BATCH: 0x67, STREAM_GROUP_LEAVE: 0x68,
  STREAM_LIST: 0x69, STREAM_GROUP_LIST: 0x6a,
  STREAM_COMMIT_BATCH: 0x6b, STREAM_FETCH_KEYS: 0x6c,
  JOB_CONSUME: 0x70, JOB_COMPLETE: 0x71, JOB_RECEIPT: 0x72, STATE_GET: 0x73,
  STATE_PUT: 0x74, STATE_DELETE: 0x75, DURABLE_OPERATION: 0x76,
  QUEUE_MANIFEST: 0x77,
};

const STATUS_OK = 0x00, STATUS_MISS = 0x01, STATUS_ERR = 0x02;
const MAX_KEY = 65535;
const MAX_VALUE = 64 * 1024 * 1024;
const PROTOCOL_MAJOR = 1, PROTOCOL_MINOR = 8;
const EXCHANGE_TYPES = { direct: 0, fanout: 1, topic: 2 };
const SF_STATES = ["value", "claimed", "wait", "negative", "released",
  "timeout", "lost", "stale", "refresh"];
const CAP = {
  CACHE: 1n << 0n, QUEUES: 1n << 1n, EXCHANGES: 1n << 2n, ATOMIC: 1n << 3n,
  SINGLEFLIGHT: 1n << 4n, STREAMS: 1n << 5n, STREAM_BATCH: 1n << 6n,
  HEALTH: 1n << 7n, STREAM_GEN: 1n << 8n, QUEUE_CONSUMERS: 1n << 9n,
  ATOMIC_UPDATE: 1n << 10n, SWR: 1n << 11n,
  QUEUE_BATCH: 1n << 12n, STREAM_COMMIT_BATCH: 1n << 13n,
  STREAM_KEYS: 1n << 14n,
  SERVER_INFO: 1n << 15n,
  JOBS: 1n << 16n,
};

class KuttiDBError extends Error {}

function u16(v) { const b = Buffer.alloc(2); b.writeUInt16LE(v); return b; }
function u32(v) { const b = Buffer.alloc(4); b.writeUInt32LE(v >>> 0); return b; }
function u64(v) { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(v)); return b; }
function asBuf(v) { return Buffer.isBuffer(v) ? v : Buffer.from(v); }

function readInstanceId(dataDir) {
  try {
    const value = fs.readFileSync(path.join(dataDir, "instance.id"), "ascii").trim();
    return /^[0-9a-f]{32}$/.test(value) ? value : null;
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw new KuttiDBError("cannot read managed instance identity");
  }
}

function probeUnix(socketPath) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ path: socketPath });
    socket.once("connect", () => { socket.destroy(); resolve(); });
    socket.once("error", (error) => { socket.destroy(); reject(error); });
  });
}

function probeTcp(host, port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port });
    socket.once("connect", () => { socket.destroy(); resolve(); });
    socket.once("error", (error) => { socket.destroy(); reject(error); });
  });
}

// ---- atomic job completion: errors -----------------------------------------
// The native error envelope for the new opcodes is [code:1][outcome:1]
// [detail]. The outcome byte separates "definitely not committed" from
// "unknown" (a possibly committed append whose durability could not be
// resolved): never conflate a conflict, an absence, and an unknown outcome.

const JOB_STATUS_CODES = {
  1: "unsupported_feature", 2: "validation_failed", 3: "request_too_large",
  4: "idempotency_conflict", 5: "state_version_conflict", 6: "delivery_expired",
  7: "delivery_not_owned", 8: "resource_exhausted", 9: "operation_in_progress",
  10: "operation_in_doubt", 11: "persistence_unavailable", 12: "not_found",
};

class KuttiDBJobError extends KuttiDBError {
  /** code is the stable wire name, outcome is "not_committed" or "unknown",
   * and detail carries the server's optional text. On an unknown outcome the
   * original intent (operation id and semantic request) must be preserved
   * for reconciliation; the receipt lookup (jobCompletion) is the safe next
   * step. */
  constructor(code, outcome, detail = null) {
    const name = JOB_STATUS_CODES[code] || `code_${code}`;
    const resolved = outcome ? "unknown" : "not_committed";
    const text = detail && detail.length ? detail.toString() : null;
    super(`job operation failed: ${name} (${resolved})` + (text ? `: ${text}` : ""));
    this.code = name;
    this.outcome = resolved;
    this.detail = text;
  }
}

class JobUnsupportedFeatureError extends KuttiDBJobError {}
class JobValidationFailedError extends KuttiDBJobError {}
class JobRequestTooLargeError extends KuttiDBJobError {}
class JobIdempotencyConflictError extends KuttiDBJobError {}
class JobStateVersionConflictError extends KuttiDBJobError {}
class JobDeliveryExpiredError extends KuttiDBJobError {}
class JobDeliveryNotOwnedError extends KuttiDBJobError {}
class JobResourceExhaustedError extends KuttiDBJobError {}
class JobOperationInDoubtError extends KuttiDBJobError {}
class JobPersistenceUnavailableError extends KuttiDBJobError {}

const JOB_ERROR_TYPES = {
  1: JobUnsupportedFeatureError, 2: JobValidationFailedError,
  3: JobRequestTooLargeError, 4: JobIdempotencyConflictError,
  5: JobStateVersionConflictError, 6: JobDeliveryExpiredError,
  7: JobDeliveryNotOwnedError, 8: JobResourceExhaustedError,
  10: JobOperationInDoubtError, 11: JobPersistenceUnavailableError,
};

// ---- atomic job completion: values -----------------------------------------
// 64-bit wire identity/version fields are BigInt; the JSON intent encoding
// carries them as lossless decimal strings and byte spans as base64.

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function uuidString(bytes) {
  const h = bytes.toString("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function uuidBytes(uuid) {
  const hex = typeof uuid === "string" ? uuid.toLowerCase() : "";
  if (!UUID_RE.test(hex)) throw new KuttiDBError("operation id must be 16 bytes or a UUID string");
  return Buffer.from(hex.replace(/-/g, ""), "hex");
}

// RFC 4122 v4 identity from 16 random bytes (dependency-free). The caller
// owns the operation id: it is generated ONCE at intent composition and
// must be preserved across retries.
function randomOperationId() {
  const b = crypto.randomBytes(16);
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  return b;
}

function operationIdBytes(operationId) {
  if (operationId == null) return randomOperationId();
  if (Buffer.isBuffer(operationId) && operationId.length === 16)
    return Buffer.from(operationId);
  if (typeof operationId === "string") return uuidBytes(operationId);
  throw new KuttiDBError("operation id must be 16 bytes or a UUID string");
}

function toU64(value, what) {
  const n = BigInt(value);
  if (n < 0n || n > 0xffffffffffffffffn) throw new KuttiDBError(what);
  return n;
}

class JobDelivery {
  /** Completion-capable delivery. The opaque proof is the only credential;
   * native owner tokens and delivery tags stay private to the server. The
   * lease deadline is a wall-clock mirror for display and logging only —
   * fencing uses the server's monotonic lease. */
  constructor({ storeId, queue, queueIncarnation, messageId, attempts,
                redelivered, leaseDeadlineMs, proof, value }) {
    this.storeId = storeId;                    // Buffer(16): stable store identity
    this.queue = queue;                        // input queue name
    this.queueIncarnation = queueIncarnation;  // BigInt, stable across restarts
    this.messageId = messageId;                // BigInt
    this.attempts = attempts;                  // number
    this.redelivered = redelivered;            // boolean
    this.leaseDeadlineMs = leaseDeadlineMs;    // BigInt wall clock
    this.proof = proof;                        // Buffer(16), one-use credential
    this.value = value;                        // Buffer payload
  }

  /** Compose one completion intent from this delivery. The operation id is
   * generated here, once (pass operationId to pin your own); persist the
   * intent before submitting and reuse the exact same intent on retries. */
  toIntent({ stateKey, expectedVersion = 0, stateValue = Buffer.alloc(0),
             outputQueue = null, outputIncarnation = 0,
             outputValue = Buffer.alloc(0), operationId = null } = {}) {
    if (stateKey == null) throw new KuttiDBError("stateKey is required");
    return new JobCompletionIntent({
      operationId: operationIdBytes(operationId),
      queue: this.queue,
      queueIncarnation: this.queueIncarnation,
      messageId: this.messageId,
      stateKey,
      expectedVersion,
      stateValue,
      outputQueue,
      outputIncarnation,
      outputValue,
    });
  }
}

class JobCompletionIntent {
  /** One stable logical completion: caller-owned operation id plus the full
   * semantic request. Serializing this object before submission is the
   * supported recovery path for lost responses; retries must reuse the same
   * id and the same fields. toJS()/fromJS() are lossless: 64-bit identity
   * and version fields are decimal strings, byte spans are base64. The
   * ephemeral delivery proof is deliberately not serialized. */
  constructor({ operationId, queue, queueIncarnation, messageId, stateKey,
                expectedVersion, stateValue, outputQueue = null,
                outputIncarnation = 0, outputValue = Buffer.alloc(0) }) {
    this.operationId = operationIdBytes(operationId);
    this.queue = queue;
    this.queueIncarnation = toU64(queueIncarnation, "invalid queue incarnation");
    this.messageId = toU64(messageId, "invalid message id");
    this.stateKey = asBuf(stateKey);
    this.expectedVersion = toU64(expectedVersion, "invalid expected version");
    this.stateValue = asBuf(stateValue || Buffer.alloc(0));
    this.outputQueue = outputQueue == null ? null : String(outputQueue);
    this.outputIncarnation = toU64(outputIncarnation, "invalid output queue incarnation");
    this.outputValue = asBuf(outputValue || Buffer.alloc(0));
  }

  get operationUuid() { return uuidString(this.operationId); }

  toJS() {
    return {
      operationId: this.operationUuid,
      input: { queue: this.queue,
               queueIncarnation: this.queueIncarnation.toString(),
               messageId: this.messageId.toString() },
      state: { key: this.stateKey.toString("base64"),
               expectedVersion: this.expectedVersion.toString(),
               value: this.stateValue.toString("base64") },
      output: this.outputQueue == null ? null :
        { queue: this.outputQueue,
          queueIncarnation: this.outputIncarnation.toString(),
          value: this.outputValue.toString("base64") },
    };
  }

  toJSON() { return this.toJS(); }

  static fromJS(data) {
    const output = data.output;
    return new JobCompletionIntent({
      operationId: uuidBytes(data.operationId),
      queue: data.input.queue,
      queueIncarnation: BigInt(data.input.queueIncarnation),
      messageId: BigInt(data.input.messageId),
      stateKey: Buffer.from(data.state.key, "base64"),
      expectedVersion: BigInt(data.state.expectedVersion),
      stateValue: Buffer.from(data.state.value, "base64"),
      outputQueue: output == null ? null : output.queue,
      outputIncarnation: output == null ? 0 : BigInt(output.queueIncarnation),
      outputValue: output == null ? Buffer.alloc(0)
        : Buffer.from(output.value, "base64"),
    });
  }
}

class JobCompletionResult {
  /** Immutable original result of one committed completion. replayed may
   * differ between the first success and a matched retry; every other field
   * is identical across retries while the receipt is retained. */
  constructor({ commitId, stateVersion, outputMessageId, completedAtMs,
                receiptExpiresMs, replayed }) {
    this.commitId = commitId;                  // BigInt
    this.stateVersion = stateVersion;          // BigInt
    this.outputMessageId = outputMessageId;    // BigInt, 0n = no output
    this.completedAtMs = completedAtMs;        // BigInt wall clock
    this.receiptExpiresMs = receiptExpiresMs;  // BigInt wall clock
    this.replayed = replayed;                  // boolean
  }
}

class JobMutationReceipt {
  /** Receipt of one direct durable-state mutation. */
  constructor({ operationId, kind, commitId, stateVersion, completedAtMs,
                receiptExpiresMs, replayed }) {
    this.operationId = operationId;            // Buffer(16)
    this.kind = kind;                          // "state_put" | "state_delete"
    this.commitId = commitId;                  // BigInt
    this.stateVersion = stateVersion;          // BigInt
    this.completedAtMs = completedAtMs;        // BigInt wall clock
    this.receiptExpiresMs = receiptExpiresMs;  // BigInt wall clock
    this.replayed = replayed;                  // boolean
  }
}

class JobReceipt {
  /** Retained receipt of a committed completion, returned by lookup. */
  constructor({ operationId, commitId, stateVersion, outputMessageId,
                completedAtMs, receiptExpiresMs }) {
    this.operationId = operationId;            // Buffer(16)
    this.commitId = commitId;                  // BigInt
    this.stateVersion = stateVersion;          // BigInt
    this.outputMessageId = outputMessageId;    // BigInt
    this.completedAtMs = completedAtMs;        // BigInt wall clock
    this.receiptExpiresMs = receiptExpiresMs;  // BigInt wall clock
  }
}

// ---- one pipelined connection: frames in, responses resolved in order -----
class Conn {
  constructor(client) {
    this.client = client;
    this.pending = [];   // resolvers, resolved in response order
    this.buf = Buffer.alloc(0);
    this.closed = false;
    const opts = client.socketPath ? { path: client.socketPath } : { host: client.host, port: client.port };
    const tlsOptions = client.tls && typeof client.tls === "object" ? client.tls : {};
    this.sock = client.tls
      ? tls.connect({ ...opts, ...tlsOptions,
          servername: tlsOptions.servername || (client.socketPath ? undefined : client.host) })
      : net.connect(opts);
    if (!client.socketPath) this.sock.setNoDelay(true);
    this.sock.on("data", (d) => this._onData(d));
    this.sock.on("error", (e) => this._fail(e));
    this.sock.on("close", () => this._fail(new Error("connection closed")));
  }

  // PUT_BATCH answers with a single raw status byte (no envelope).
  requestRawByte(frame) {
    return new Promise((resolve, reject) => {
      if (this.closed) { reject(new Error("connection closed")); return; }
      this.rawByte = { resolve, reject };
      this.sock.write(frame);
    });
  }

  _onData(d) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    if (this.rawByte) {
      const rb = this.rawByte;
      this.rawByte = null;
      this.buf = this.buf.subarray(1);
      rb.resolve(d[0]);
      return;
    }
    if (this.batch) return this._onBatchData();
    for (;;) {
      if (this.buf.length < 5) return;
      const status = this.buf[0];
      const len = this.buf.readUInt32LE(1);
      if (len > MAX_VALUE + 1024) { this._fail(new Error("bad response")); return; }
      if (this.buf.length < 5 + len) return;
      const payload = Buffer.from(this.buf.subarray(5, 5 + len));
      this.buf = this.buf.subarray(5 + len);
      const p = this.pending.shift();
      if (p) p.resolve({ status, payload });
    }
  }

  _fail(err) {
    if (this.closed) return;
    this.closed = true;
    while (this.pending.length) this.pending.shift().reject(err);
    if (this.batch) { const b = this.batch; this.batch = null; b.reject(err); }
    if (this.rawByte) { const rb = this.rawByte; this.rawByte = null; rb.reject(err); }
    this.client._drop(this);
  }

  _frame(op, key, payload) {
    const kb = key || Buffer.alloc(0);
    const vb = payload || Buffer.alloc(0);
    const hdr = Buffer.alloc(7);
    hdr[0] = op;
    hdr.writeUInt16LE(kb.length, 1);
    hdr.writeUInt32LE(vb.length, 3);
    return Buffer.concat([hdr, kb, vb]);
  }

  request(op, key, payload) {
    return new Promise((resolve, reject) => {
      if (this.closed) { reject(new Error("connection closed")); return; }
      this.pending.push({ resolve, reject });
      this.sock.write(this._frame(op, key, payload));
    });
  }

  // PUT_SWR frames metadata between the header and the key.
  requestMeta(op, key, meta, value) {
    return new Promise((resolve, reject) => {
      if (this.closed) { reject(new Error("connection closed")); return; }
      const kb = key || Buffer.alloc(0);
      const hdr = Buffer.alloc(7);
      hdr[0] = op;
      hdr.writeUInt16LE(kb.length, 1);
      hdr.writeUInt32LE((value || Buffer.alloc(0)).length, 3);
      this.pending.push({ resolve, reject });
      this.sock.write(Buffer.concat([hdr, meta, kb, value || Buffer.alloc(0)]));
    });
  }

  // GET_BATCH responses stream without an envelope: [count:4] then
  // count * [status:1][vlen:4][value]. Exclusive on the connection while
  // in flight (the pool guarantees that).
  requestBatch(frame) {
    return new Promise((resolve, reject) => {
      if (this.closed) { reject(new Error("connection closed")); return; }
      this.batch = { buf: Buffer.alloc(0), phase: "count", items: [], resolve, reject };
      this.sock.write(frame);
    });
  }

  _onBatchData() {
    const b = this.batch;
    b.buf = Buffer.concat([b.buf, this.buf]);
    this.buf = Buffer.alloc(0);
    try {
      for (;;) {
        if (b.phase === "count") {
          if (b.buf.length < 4) return;
          b.count = b.buf.readUInt32LE(0);
          b.at = 4;
          if (b.items.length >= b.count) { this._batchDone(); return; }
          b.phase = "header";
        } else if (b.phase === "header") {
          if (b.buf.length - b.at < 5) return;
          const status = b.buf[b.at];
          const vlen = b.buf.readUInt32LE(b.at + 1);
          b.at += 5;
          if (status === 0 && vlen > 0) {
            b.cur = { status, vlen };
            b.phase = "value";
          } else {
            b.items.push(status === 0 ? Buffer.alloc(0) : null);
            if (b.items.length >= b.count) { this._batchDone(); return; }
            b.phase = "header";
          }
        } else { // value
          if (b.buf.length - b.at < b.cur.vlen) return;
          b.items.push(Buffer.from(b.buf.subarray(b.at, b.at + b.cur.vlen)));
          b.at += b.cur.vlen;
          b.cur = null;
          b.phase = "header";
          if (b.items.length >= b.count) { this._batchDone(); return; }
        }
      }
    } catch (e) {
      this.batch = null;
      b.reject(e);
    }
  }

  _batchDone() {
    const b = this.batch;
    this.batch = null;
    this.buf = Buffer.concat([b.buf.subarray(b.at), this.buf]);
    b.resolve(b.items);
  }

  requestRaw(frame) {
    return new Promise((resolve, reject) => {
      if (this.closed) { reject(new Error("connection closed")); return; }
      this.pending.push({ resolve, reject });
      this.sock.write(frame);
    });
  }

  destroy() { this.closed = true; this.sock.destroy(); }
}

class Client {
  /**
   * options: { host, port, socketPath, poolSize, token, tls }
   * tls may be true (system trust and hostname verification) or an object of
   * Node tls.connect options such as { ca, cert, key, servername }.
   */
  constructor(options = {}) {
    this.host = options.host || "127.0.0.1";
    this.port = options.port || 7379;
    this.socketPath = options.socketPath || null;
    this.poolSize = Math.max(1, options.poolSize || 4);
    this.token = options.token ? Buffer.from(options.token) : null;
    this.tls = options.tls || false;
    this.idle = [];
    this.waiters = [];
    this.closed = false;
    this._caps = null;
    this._conns = 0; // connections created so far (lazily grown to poolSize)
  }

  /** Create an eagerly-connected managed local client. Unix is the secure
   * default; explicit TCP is limited to literal IPv4 loopback. Existing
   * constructors remain connect-only and lazy. */
  static async managed(options = {}) {
    const requestedDataDir = options.dataDir || options.data_dir;
    if (!requestedDataDir || options.tls)
      throw new KuttiDBError("managed mode requires dataDir and does not support TLS without managed certificate settings");
    const dataDir = path.resolve(requestedDataDir);
    const transport = options.transport || "unix";
    const tcp = transport === "tcp";
    if (transport !== "unix" && !tcp)
      throw new KuttiDBError("managed transport must be 'unix' or 'tcp'");
    const host = options.host || "127.0.0.1";
    const port = Number(options.port || 7379);
    if (tcp && (!net.isIP(host) || !host.startsWith("127.") || !Number.isInteger(port) || port < 1 || port > 65535))
      throw new KuttiDBError("managed TCP requires a literal IPv4 loopback endpoint");
    const socketPath = tcp ? null : path.join(dataDir, "kuttidb.sock");
    const endpoint = tcp ? `tcp:${host}:${port}` : `unix:${socketPath}`;
    let expected = readInstanceId(dataDir);
    let absent = false;
    try { await (tcp ? probeTcp(host, port) : probeUnix(socketPath)); }
    catch (error) {
      if (error.code !== "ECONNREFUSED" && (!socketPath || error.code !== "ENOENT")) throw new KuttiDBError("managed endpoint is occupied or unavailable");
      absent = true;
    }
    if (absent) {
      const executable = options.executable || process.env.KUTTIDB_SERVER || "kuttidb";
      const timeout = Number(options.startupTimeout || options.startup_timeout || 10) * 1000;
      const args = ["ensure", "--data-dir", dataDir, "--listen", endpoint,
        "--idle-timeout-ms", String(Math.max(1, Number(options.idleTimeout || options.idle_timeout || 60) * 1000)),
        "--startup-timeout-ms", String(Math.max(1, timeout)), "--json"];
      // Atomic job completion flags (allowlisted by `ensure`): the boolean
      // enablement flag takes no value; the budget flags take positive
      // integers. The durable Queue WAL is the completion commit authority,
      // so enabling the feature requires an explicit queueWal.
      if (options.jobCompletion) {
        const queueWal = options.queueWal || options.queue_wal;
        if (!queueWal)
          throw new KuttiDBError("managed jobCompletion requires an explicit queueWal (the durable Queue WAL is the completion commit authority)");
        args.push("--job-completion", "--queue-wal", String(queueWal));
      }
      const jobSettings = [
        ["jobStateMaxMemoryMb", "--job-state-max-memory-mb", 1],
        ["jobReceiptsMaxMemoryMb", "--job-receipts-max-memory-mb", 1],
        ["jobReceiptsMaxCount", "--job-receipts-max-count", 1],
        ["jobReceiptRetentionMs", "--job-receipt-retention-ms", 1000],
        ["jobCompletionMaxBytes", "--job-completion-max-bytes", 1],
      ];
      for (const [key, flag, minimum] of jobSettings) {
        const value = options[key];
        if (value == null) continue;
        if (!Number.isInteger(value) || value < minimum)
          throw new KuttiDBError(`managed ${key} must be an integer >= ${minimum}`);
        args.push(flag, String(value));
      }
      let response;
      try {
        const result = await execFileAsync(executable, args, { timeout: timeout + 1000, maxBuffer: 8192 });
        response = JSON.parse(result.stdout);
      } catch (error) { throw new KuttiDBError("managed server startup failed"); }
      if (!response || !/^[0-9a-f]{32}$/.test(response.instance_id)) throw new KuttiDBError("invalid managed launcher response");
      expected = response.instance_id;
    }
    if (!expected) throw new KuttiDBError("managed endpoint is occupied by an unverifiable server");
    const client = new Client({ ...options, host, port, socketPath, tls: false });
    const lease = await client._acquire(); client._release(lease); // eager lifetime lease
    const caps = await client.capabilities();
    if (!(caps.features & CAP.SERVER_INFO)) { await client.close(); throw new KuttiDBError("managed endpoint uses an incompatible protocol"); }
    const info = await client._req(OP.SERVER_INFO, Buffer.alloc(0));
    if (info.status !== STATUS_OK || info.payload.length !== 52 || info.payload.subarray(2, 34).toString("ascii") !== expected) {
      await client.close(); throw new KuttiDBError("managed endpoint belongs to another instance");
    }
    return client;
  }

  async _newConn() {
    const conn = new Conn(this);
    if (conn.sock.readyState !== "open") {
      await new Promise((res, rej) => {
        conn.sock.once(this.tls ? "secureConnect" : "connect", res);
        conn.sock.once("error", rej);
        conn.sock.once("close", () => rej(new Error("connection closed")));
      });
    }
    if (this.token) {
      const r = await conn.request(OP.AUTH, this.token);
      if (r.status !== STATUS_OK) throw new Error("authentication failed");
    }
    return conn;
  }

  async _acquire() {
    if (this.closed) throw new KuttiDBError("client closed");
    if (this.idle.length) return this.idle.pop();
    if (this._conns < this.poolSize) {
      this._conns++;
      try {
        return await this._newConn();
      } catch (e) {
        this._conns--;
        throw new KuttiDBError(e.message);
      }
    }
    return new Promise((res, rej) => this.waiters.push({ res, rej }));
  }

  _release(conn) {
    const w = this.waiters.shift();
    if (w) w.res(conn); else this.idle.push(conn);
  }

  _drop(conn) {
    this._conns--; // the lost slot is recreated lazily by the next _acquire
  }

  async _req(op, key, payload) {
    const conn = await this._acquire();
    try {
      return await conn.request(op, key, payload);
    } catch (e) {
      if (e instanceof KuttiDBError) throw e;
      throw new KuttiDBError(e.message);
    } finally {
      if (!conn.closed) this._release(conn);
      else this._conns--;
    }
  }

  async _reqRaw(frame) {
    const conn = await this._acquire();
    try {
      return await conn.requestRaw(frame);
    } catch (e) {
      if (e instanceof KuttiDBError) throw e;
      throw new KuttiDBError(e.message);
    } finally {
      if (!conn.closed) this._release(conn);
      else this._conns--;
    }
  }

  _ok(r, what) {
    if (r.status !== STATUS_OK) throw new KuttiDBError(`${what} failed`);
    return r;
  }

  _checkKey(kb) {
    if (!kb.length || kb.length > MAX_KEY) throw new KuttiDBError("invalid key");
  }

  _checkVal(vb) {
    if (vb.length > MAX_VALUE) throw new KuttiDBError("value too large");
  }

  // ---- capabilities --------------------------------------------------------
  async capabilities() {
    if (this._caps) return this._caps;
    const r = await this._req(OP.CAPABILITIES, Buffer.alloc(0),
      Buffer.concat([u16(PROTOCOL_MAJOR), u16(PROTOCOL_MINOR)]));
    if (r.status === STATUS_MISS)
      throw new KuttiDBError("incompatible protocol major version");
    this._ok(r, "capabilities");
    if (r.payload.length !== 12) throw new KuttiDBError("invalid capabilities response");
    return (this._caps = {
      major: r.payload.readUInt16LE(0),
      minor: r.payload.readUInt16LE(2),
      features: r.payload.readBigUInt64LE(4),
    });
  }

  async _require(feature, what) {
    const caps = await this.capabilities();
    if (!(caps.features & feature)) throw new KuttiDBError(`server does not support ${what}`);
  }

  // ---- cache ---------------------------------------------------------------
  async put(key, value, { ttl = null } = {}) {
    const kb = asBuf(key), vb = asBuf(value);
    this._checkKey(kb); this._checkVal(vb);
    if (ttl == null) {
      this._ok(await this._req(OP.PUT, kb, vb), "put");
    } else {
      // PUT_TTL frames metadata before the key: [op][klen][vlen][ttl:4][key][value]
      const conn = await this._acquire();
      try {
        const r = await conn.requestMeta(OP.PUT_TTL, kb,
          u32(Math.max(0, Math.round(ttl * 1000))), vb);
        this._ok(r, "put");
      } catch (e) {
        if (e instanceof KuttiDBError) throw e;
        throw new KuttiDBError(e.message);
      } finally {
        if (!conn.closed) this._release(conn);
        else this._conns--;
      }
    }
  }

  async putSwr(key, value, { ttl, staleFor, refreshAfter = null } = {}) {
    await this._require(CAP.SWR, "stale-while-revalidate");
    const kb = asBuf(key), vb = asBuf(value);
    this._checkKey(kb); this._checkVal(vb);
    const ttlMs = Math.round(ttl * 1000), staleMs = Math.round(staleFor * 1000);
    const refreshMs = refreshAfter == null ? 0 : Math.round(refreshAfter * 1000);
    if (ttlMs <= 0 || staleMs <= 0)
      throw new KuttiDBError("putSwr requires ttl > 0 and staleFor > 0");
    if (staleMs > 7 * 24 * 3600 * 1000 || refreshMs > 7 * 24 * 3600 * 1000)
      throw new KuttiDBError("swr windows must be <= 7 days");
    const conn = await this._acquire();
    try {
      const r = await conn.requestMeta(OP.PUT_SWR, kb,
        Buffer.concat([u32(ttlMs), u32(staleMs), u32(refreshMs)]), vb);
      this._ok(r, "putSwr");
    } catch (e) {
      if (e instanceof KuttiDBError) throw e;
      throw new KuttiDBError(e.message);
    } finally {
      if (!conn.closed) this._release(conn);
      else this._conns--;
    }
  }

  async get(key) {
    const r = await this._req(OP.GET, asBuf(key));
    if (r.status === STATUS_MISS) return null;
    if (r.status !== STATUS_OK) throw new KuttiDBError("get failed");
    return r.payload;
  }

  async delete(key) {
    const r = await this._req(OP.DELETE, asBuf(key));
    if (r.status === STATUS_ERR) throw new KuttiDBError("delete failed");
    return r.status === STATUS_OK;
  }

  async stats() { return JSON.parse(this._ok(await this._req(OP.STATS), "stats").payload.toString()); }
  async health() { return (await this._req(OP.HEALTH)).status === STATUS_OK; }

  async putMany(items) { // items: [key, value][]
    // Batch frames carry the item count in the header vlen field, then the
    // items themselves: [op][klen=0][count][count * [klen:2][vlen:4][key][value]]
    if (!Array.isArray(items) || items.length > 65536)
      throw new KuttiDBError("invalid putMany batch");
    const parts = [];
    for (const [k, v] of items) {
      const kb = asBuf(k), vb = asBuf(v);
      this._checkKey(kb); this._checkVal(vb);
      parts.push(Buffer.concat([u16(kb.length), u32(vb.length)]), kb, vb);
    }
    const hdr = Buffer.alloc(7);
    hdr[0] = OP.PUT_BATCH;
    hdr.writeUInt32LE(items.length, 3);
    const r = await this._reqRawByte(Buffer.concat([hdr, ...parts]));
    if (r !== STATUS_OK) throw new KuttiDBError("putMany failed");
  }

  async putManyTTL(items) { // items: [key, value, ttlSeconds][]
    if (!Array.isArray(items) || items.length > 65536)
      throw new KuttiDBError("invalid putManyTTL batch");
    const parts = [];
    for (const [k, v, ttl = null] of items) {
      const kb = asBuf(k), vb = asBuf(v);
      this._checkKey(kb); this._checkVal(vb);
      const ttlMs = ttl == null ? 0 : Math.round(ttl * 1000);
      if (!Number.isSafeInteger(ttlMs) || ttlMs < 0 || ttlMs > 0xffffffff)
        throw new KuttiDBError("invalid TTL");
      parts.push(Buffer.concat([u16(kb.length), u32(vb.length), u32(ttlMs)]), kb, vb);
    }
    const hdr = Buffer.alloc(7);
    hdr[0] = OP.PUT_BATCH_TTL;
    hdr.writeUInt32LE(items.length, 3);
    const status = await this._reqRawByte(Buffer.concat([hdr, ...parts]));
    if (status !== STATUS_OK) throw new KuttiDBError("putManyTTL failed");
  }

  async _reqRawByte(frame) {
    const conn = await this._acquire();
    try {
      return await conn.requestRawByte(frame);
    } catch (e) {
      if (e instanceof KuttiDBError) throw e;
      throw new KuttiDBError(e.message);
    } finally {
      if (!conn.closed) this._release(conn);
      else this._conns--;
    }
  }

  async getMany(keys) {
    // [op][klen=0][count][count * [klen:2][key]]; responses stream back.
    const parts = [];
    for (const k of keys) {
      const kb = asBuf(k);
      this._checkKey(kb);
      parts.push(u16(kb.length), kb);
    }
    const hdr = Buffer.alloc(7);
    hdr[0] = OP.GET_BATCH;
    hdr.writeUInt32LE(keys.length, 3);
    return this._reqBatch(Buffer.concat([hdr, ...parts]));
  }

  async _reqBatch(frame) {
    const conn = await this._acquire();
    try {
      return await conn.requestBatch(frame);
    } catch (e) {
      if (e instanceof KuttiDBError) throw e;
      throw new KuttiDBError(e.message);
    } finally {
      if (!conn.closed) this._release(conn);
      else this._conns--;
    }
  }

  // ---- queues ---------------------------------------------------------------
  async queueDeclare(name, { durable = true, maxDepth = 0,
                             deadLetterQueue = null, maxDeliveries = 0 } = {}) {
    const kb = asBuf(name);
    this._checkKey(kb);
    let value = Buffer.concat([Buffer.from([durable ? 1 : 0]), u64(maxDepth)]);
    if (deadLetterQueue != null) {
      const dlq = asBuf(deadLetterQueue);
      const ext = Buffer.concat([u16(dlq.length), dlq, u32(maxDeliveries)]);
      value = Buffer.concat([value, u16(ext.length), ext]);
    }
    this._ok(await this._req(OP.QUEUE_DECLARE, kb, value), "queueDeclare");
  }

  async queueList() {
    const r = this._ok(await this._req(OP.QUEUE_LIST), "queueList");
    if (r.payload.length < 2) throw new KuttiDBError("invalid queueList response");
    const count = r.payload.readUInt16LE(0), queues = [];
    let at = 2;
    for (let i = 0; i < count; i++) {
      if (at + 2 > r.payload.length) throw new KuttiDBError("invalid queueList response");
      const len = r.payload.readUInt16LE(at); at += 2;
      if (at + len + 16 > r.payload.length) throw new KuttiDBError("invalid queueList response");
      queues.push({ name: r.payload.subarray(at, at + len).toString(),
        depth: r.payload.readBigUInt64LE(at + len),
        inflight: r.payload.readBigUInt64LE(at + len + 8) });
      at += len + 16;
    }
    if (at !== r.payload.length) throw new KuttiDBError("invalid queueList response");
    return queues;
  }

  async queuePublish(name, value, { ttl = null } = {}) {
    const kb = asBuf(name), vb = asBuf(value);
    this._checkKey(kb); this._checkVal(vb);
    let r;
    if (ttl == null) {
      r = this._ok(await this._req(OP.QUEUE_PUBLISH, kb, vb), "queuePublish");
    } else {
      r = this._ok(await this._req(OP.QUEUE_PUBLISH_TTL, kb,
        Buffer.concat([u64(Math.max(0, Math.round(ttl * 1000))), vb])), "queuePublish");
    }
    return r.payload.readBigUInt64LE(0);
  }

  async queueConsume(name, { visibility = 30.0, consumer = null } = {}) {
    const vis = u64(Math.max(0, Math.round(visibility * 1000)));
    let r;
    if (consumer != null) {
      const gb = asBuf(consumer);
      r = await this._req(OP.QUEUE_CONSUME_AS, asBuf(name), Buffer.concat([u16(gb.length), gb, vis]));
    } else {
      r = await this._req(OP.QUEUE_CONSUME, asBuf(name), vis);
    }
    if (r.status === STATUS_MISS) return null;
    if (r.status !== STATUS_OK || r.payload.length < 21) throw new KuttiDBError("queueConsume failed");
    return {
      id: r.payload.readBigUInt64LE(0),
      messageId: r.payload.readBigUInt64LE(8),
      redelivered: r.payload[16] !== 0,
      deliveryCount: r.payload.readUInt32LE(17),
      value: r.payload.subarray(21),
    };
  }

  async queueAck(name, deliveryTag) {
    const r = await this._req(OP.QUEUE_ACK, asBuf(name), u64(deliveryTag));
    if (r.status === STATUS_ERR) throw new KuttiDBError("queueAck failed");
    return r.status === STATUS_OK;
  }

  async queueNack(name, deliveryTag, { requeue = true, delay = 0 } = {}) {
    let p = Buffer.concat([u64(deliveryTag), Buffer.from([requeue ? 1 : 0])]);
    if (delay) p = Buffer.concat([p, u64(Math.round(delay * 1000))]);
    const r = await this._req(OP.QUEUE_NACK, asBuf(name), p);
    if (r.status === STATUS_ERR) throw new KuttiDBError("queueNack failed");
    return r.status === STATUS_OK;
  }

  async queueStats(name) {
    const r = await this._req(OP.QUEUE_STATS, asBuf(name));
    if (r.status === STATUS_MISS) return null;
    this._ok(r, "queueStats");
    return { depth: Number(r.payload.readBigUInt64LE(0)), inflight: Number(r.payload.readBigUInt64LE(8)) };
  }

  async queuePrefetch(count) {
    this._ok(await this._req(OP.QUEUE_PREFETCH, Buffer.from("_"), u32(count)), "queuePrefetch");
  }

  async queueCancel() {
    this._ok(await this._req(OP.QUEUE_CANCEL, Buffer.from("_")), "queueCancel");
  }

  async queueConsumerRegister(consumer) {
    const r = this._ok(await this._req(OP.QUEUE_CONSUMER_REGISTER, asBuf(consumer)), "queueConsumerRegister");
    return r.payload.readBigUInt64LE(0);
  }

  async queueConsumerUnregister(consumer) {
    this._ok(await this._req(OP.QUEUE_CONSUMER_UNREGISTER, asBuf(consumer)), "queueConsumerUnregister");
  }

  queueConsumeAs(name, consumer, { visibility = 30.0 } = {}) {
    return this.queueConsume(name, { visibility, consumer });
  }

  async queuePublishBatch(name, values) {
    await this._require(CAP.QUEUE_BATCH, "queue batch operations");
    if (!Array.isArray(values) || values.length < 1 || values.length > 256)
      throw new KuttiDBError("queue batch size must be 1-256");
    const parts = [u32(values.length)];
    for (const value of values) {
      const vb = asBuf(value); this._checkVal(vb);
      parts.push(u32(vb.length), vb);
    }
    const r = this._ok(await this._req(OP.QUEUE_PUBLISH_BATCH, asBuf(name),
      Buffer.concat(parts)), "queuePublishBatch");
    if (r.payload.length < 4) throw new KuttiDBError("invalid queuePublishBatch response");
    const count = r.payload.readUInt32LE(0);
    if (count !== values.length || r.payload.length !== 4 + count * 8)
      throw new KuttiDBError("invalid queuePublishBatch response");
    return Array.from({ length: count }, (_, i) => r.payload.readBigUInt64LE(4 + i * 8));
  }

  async queueConsumeBatch(name, maxCount) {
    await this._require(CAP.QUEUE_BATCH, "queue batch operations");
    if (!Number.isInteger(maxCount) || maxCount < 1 || maxCount > 256)
      throw new KuttiDBError("queue batch size must be 1-256");
    const r = await this._req(OP.QUEUE_CONSUME_BATCH, asBuf(name), u32(maxCount));
    if (r.status === STATUS_MISS) return [];
    this._ok(r, "queueConsumeBatch");
    if (r.payload.length < 4) throw new KuttiDBError("invalid queueConsumeBatch response");
    const count = r.payload.readUInt32LE(0), messages = [];
    let at = 4;
    for (let i = 0; i < count; i++) {
      if (at + 25 > r.payload.length) throw new KuttiDBError("invalid queueConsumeBatch response");
      const len = r.payload.readUInt32LE(at + 21);
      if (at + 25 + len > r.payload.length) throw new KuttiDBError("invalid queueConsumeBatch response");
      messages.push({ id: r.payload.readBigUInt64LE(at),
        messageId: r.payload.readBigUInt64LE(at + 8),
        deliveryCount: r.payload.readUInt32LE(at + 16),
        redelivered: r.payload[at + 20] !== 0,
        value: r.payload.subarray(at + 25, at + 25 + len) });
      at += 25 + len;
    }
    if (at !== r.payload.length) throw new KuttiDBError("invalid queueConsumeBatch response");
    return messages;
  }

  async _queueDispositionBatch(name, deliveryTags, mode, what) {
    await this._require(CAP.QUEUE_BATCH, "queue batch operations");
    const tags = Array.from(deliveryTags);
    if (tags.length < 1 || tags.length > 256)
      throw new KuttiDBError("queue batch size must be 1-256");
    const payload = Buffer.concat([Buffer.from([mode]), u32(tags.length), ...tags.map(u64)]);
    const r = this._ok(await this._req(OP.QUEUE_ACK_BATCH, asBuf(name), payload), what);
    if (r.payload.length !== 4) throw new KuttiDBError(`invalid ${what} response`);
    return r.payload.readUInt32LE(0);
  }

  queueAckBatch(name, deliveryTags) {
    return this._queueDispositionBatch(name, deliveryTags, 0, "queueAckBatch");
  }

  queueNackBatch(name, deliveryTags, { requeue = true } = {}) {
    return this._queueDispositionBatch(name, deliveryTags, requeue ? 1 : 2, "queueNackBatch");
  }

  // ---- exchanges --------------------------------------------------------------
  async exchangeDeclare(name, { type = "direct", durable = true,
                                alternateExchange = null } = {}) {
    const kb = asBuf(name);
    this._checkKey(kb);
    if (!Object.prototype.hasOwnProperty.call(EXCHANGE_TYPES, type))
      throw new KuttiDBError("unknown exchange type");
    let value = Buffer.from([durable ? 1 : 0, EXCHANGE_TYPES[type]]);
    if (alternateExchange != null) {
      const alternate = asBuf(alternateExchange);
      const ext = Buffer.concat([u16(alternate.length), alternate]);
      value = Buffer.concat([value, u16(ext.length), ext]);
    }
    this._ok(await this._req(OP.EXCHANGE_DECLARE, kb, value), "exchangeDeclare");
  }

  async exchangeBind(exchange, queue, routingKey = "") {
    this._ok(await this._req(OP.EXCHANGE_BIND, asBuf(exchange),
      Buffer.concat([u16(asBuf(queue).length), asBuf(queue),
                     u16(asBuf(routingKey).length), asBuf(routingKey)])), "exchangeBind");
  }

  async exchangeUnbind(exchange, queue, routingKey = "") {
    this._ok(await this._req(OP.EXCHANGE_UNBIND, asBuf(exchange),
      Buffer.concat([u16(asBuf(queue).length), asBuf(queue),
                     u16(asBuf(routingKey).length), asBuf(routingKey)])), "exchangeUnbind");
  }

  async exchangePublish(exchange, routingKey, value, { ttl = null } = {}) {
    const rk = asBuf(routingKey || ""), vb = asBuf(value);
    this._checkVal(vb);
    const t = ttl == null ? Buffer.alloc(8) : u64(Math.max(0, Math.round(ttl * 1000)));
    const r = await this._req(OP.EXCHANGE_PUBLISH, asBuf(exchange),
      Buffer.concat([u16(rk.length), t, rk, vb]));
    if (r.status === STATUS_MISS) return 0; // unroutable: nothing routed
    this._ok(r, "exchangePublish");
    return r.payload.readUInt32LE(0);
  }

  // ---- atomic cache-plus-messaging ---------------------------------------------
  async _atomic(op, key, payload) {
    const r = await this._req(op, asBuf(key), payload);
    if (r.status === STATUS_MISS) return { txId: 0n, routed: 0, unroutable: true };
    if (r.status !== STATUS_OK || r.payload.length !== 12)
      throw new KuttiDBError("atomic operation failed");
    return { txId: r.payload.readBigUInt64LE(0), routed: r.payload.readUInt32LE(8), unroutable: false };
  }

  putAndPublish(key, value, { exchange, routingKey = "", ttl = null } = {}) {
    const vb = asBuf(value);
    this._checkVal(vb);
    const ttlMs = ttl == null ? 0 : Math.max(0, Math.round(ttl * 1000));
    return this._atomic(OP.ATOMIC_PUT_PUBLISH, key,
      Buffer.concat([u16(asBuf(exchange).length), asBuf(exchange),
                     u16(asBuf(routingKey).length), asBuf(routingKey),
                     u32(ttlMs), vb]));
  }

  putAndEnqueue(key, value, { queue, ttl = null } = {}) {
    const vb = asBuf(value);
    this._checkVal(vb);
    const ttlMs = ttl == null ? 0 : Math.max(0, Math.round(ttl * 1000));
    return this._atomic(OP.ATOMIC_PUT_ENQUEUE, key,
      Buffer.concat([u16(asBuf(queue).length), asBuf(queue), u32(ttlMs), vb]));
  }

  deleteAndPublish(key, { exchange, routingKey = "", message = null } = {}) {
    const vb = message == null ? Buffer.alloc(0) : asBuf(message);
    return this._atomic(OP.ATOMIC_DELETE_PUBLISH, key,
      Buffer.concat([u16(asBuf(exchange).length), asBuf(exchange),
                     u16(asBuf(routingKey).length), asBuf(routingKey),
                     u32(vb.length), vb]));
  }

  updateAndEmit(key, value, { exchange, routingKey = "", ttl = null } = {}) {
    const vb = asBuf(value);
    this._checkVal(vb);
    const ttlMs = ttl == null ? 0 : Math.max(0, Math.round(ttl * 1000));
    return this._atomic(OP.ATOMIC_UPDATE_EMIT, key,
      Buffer.concat([u16(asBuf(exchange).length), asBuf(exchange),
                     u16(asBuf(routingKey).length), asBuf(routingKey),
                     u32(ttlMs), vb]));
  }

  // ---- singleflight --------------------------------------------------------------
  _sfResult(r, withHolder) {
    if (r.status === STATUS_ERR) throw new KuttiDBError("singleflight operation refused");
    if (r.status !== STATUS_OK || r.payload.length < 1)
      throw new KuttiDBError("singleflight operation failed");
    const state = SF_STATES[r.payload[0]] || `unknown-${r.payload[0]}`;
    const out = { state };
    const skip = withHolder ? 2 : 1;
    if (withHolder) out.holder = r.payload[1] === 1;
    if (r.payload[0] === 0 || r.payload[0] === 7 || r.payload[0] === 8)
      out.value = r.payload.subarray(skip);
    return out;
  }

  async getOrClaim(key, { lease = 5.0 } = {}) {
    const r = this._ok(await this._req(OP.SF_GET_OR_CLAIM, asBuf(key),
      u32(Math.max(1, Math.round(lease * 1000)))), "getOrClaim");
    return this._sfResult(r, false);
  }

  async waitFor(key, { timeout = 10.0 } = {}) {
    const r = this._ok(await this._req(OP.SF_WAIT_FOR_KEY, asBuf(key),
      u32(Math.max(1, Math.round(timeout * 1000)))), "waitFor");
    return this._sfResult(r, false);
  }

  async putAndRelease(key, value, { ttl = null, negative = false } = {}) {
    const vb = asBuf(value);
    this._checkVal(vb);
    const p = Buffer.concat([u32(ttl == null ? 0 : Math.max(0, Math.round(ttl * 1000))),
                             Buffer.from([negative ? 1 : 0]), vb]);
    this._ok(await this._req(OP.SF_PUT_AND_RELEASE, asBuf(key), p), "putAndRelease");
  }

  async releaseClaim(key) {
    this._ok(await this._req(OP.SF_RELEASE_CLAIM, asBuf(key)), "releaseClaim");
  }

  async getOrRefresh(key, { lease = 5.0 } = {}) {
    await this._require(CAP.SWR, "stale-while-revalidate");
    const r = this._ok(await this._req(OP.SF_GET_OR_REFRESH, asBuf(key),
      u32(Math.max(1, Math.round(lease * 1000)))), "getOrRefresh");
    return this._sfResult(r, true);
  }

  async getOrLoad(key, loader, { ttl = 60.0, lease = 5.0, wait = 10.0 } = {}) {
    let r = await this.getOrClaim(key, { lease });
    if (r.state === "value") return r.value;
    if (r.state === "negative") return null;
    if (r.state === "claimed") {
      // load below
    } else if (r.state === "wait") {
      for (let i = 0; i < 3; i++) {
        const w = await this.waitFor(key, { timeout: wait });
        if (w.state === "value") return w.value;
        if (w.state === "negative" || w.state === "timeout") return null;
        r = await this.getOrClaim(key, { lease });
        if (r.state === "value") return r.value;
        if (r.state === "negative") return null;
        if (r.state === "claimed") break;
      }
    } else {
      r = await this.getOrClaim(key, { lease });
      if (r.state === "value") return r.value;
      if (r.state === "negative") return null;
    }
    if (r.state !== "claimed") return null;
    let loaded;
    try {
      loaded = await loader();
    } catch (e) {
      await this.releaseClaim(key);
      throw e;
    }
    if (loaded == null) {
      await this.putAndRelease(key, Buffer.alloc(0), { ttl, negative: true });
      return null;
    }
    await this.putAndRelease(key, loaded, { ttl });
    return loaded;
  }

  async getOrLoadSwr(key, loader, { ttl = 60.0, staleFor = 300.0,
                                    refreshAfter = null, lease = 5.0,
                                    wait = 10.0 } = {}) {
    let r = await this.getOrRefresh(key, { lease });
    if (r.state === "value") return r.value;
    if (r.state === "negative") return null;
    if ((r.state === "stale" || r.state === "refresh") && !r.holder) return r.value;
    if (r.state === "wait") {
      for (let i = 0; i < 3; i++) {
        const w = await this.waitFor(key, { timeout: wait });
        if (w.state === "value") return w.value;
        if (w.state === "negative" || w.state === "timeout") return null;
        r = await this.getOrRefresh(key, { lease });
        if (r.state === "value") return r.value;
        if (r.state === "negative") return null;
        if ((r.state === "stale" || r.state === "refresh") && !r.holder) return r.value;
        if (["claimed", "stale", "refresh"].includes(r.state)) break;
      }
    } else if (!["claimed", "stale", "refresh"].includes(r.state)) {
      r = await this.getOrRefresh(key, { lease });
      if (r.state === "value") return r.value;
      if (r.state === "negative") return null;
      if ((r.state === "stale" || r.state === "refresh") && !r.holder) return r.value;
    }
    if (!["claimed", "stale", "refresh"].includes(r.state)) return null;
    let loaded;
    try {
      loaded = await loader();
    } catch (e) {
      await this.releaseClaim(key);
      throw e;
    }
    if (loaded == null) {
      await this.putAndRelease(key, Buffer.alloc(0), { ttl, negative: true });
      return null;
    }
    await this.putSwr(key, loaded, { ttl, staleFor, refreshAfter });
    await this.releaseClaim(key);
    return asBuf(loaded);
  }

  // ---- streams --------------------------------------------------------------
  async streamList() {
    const r = this._ok(await this._req(OP.STREAM_LIST), "streamList");
    if (r.payload.length < 2) throw new KuttiDBError("invalid streamList response");
    const count = r.payload.readUInt16LE(0), streams = [];
    let at = 2;
    for (let i = 0; i < count; i++) {
      if (at + 2 > r.payload.length) throw new KuttiDBError("invalid streamList response");
      const len = r.payload.readUInt16LE(at); at += 2;
      if (at + len + 20 > r.payload.length) throw new KuttiDBError("invalid streamList response");
      streams.push({ topic: r.payload.subarray(at, at + len).toString(),
        partitions: r.payload.readUInt32LE(at + len),
        records: r.payload.readBigUInt64LE(at + len + 4),
        bytes: r.payload.readBigUInt64LE(at + len + 12) });
      at += len + 20;
    }
    if (at !== r.payload.length) throw new KuttiDBError("invalid streamList response");
    return streams;
  }

  async streamGroupList() {
    const r = this._ok(await this._req(OP.STREAM_GROUP_LIST), "streamGroupList");
    if (r.payload.length < 2) throw new KuttiDBError("invalid streamGroupList response");
    const count = r.payload.readUInt16LE(0), groups = [];
    let at = 2;
    for (let i = 0; i < count; i++) {
      if (at + 2 > r.payload.length) throw new KuttiDBError("invalid streamGroupList response");
      const topicLen = r.payload.readUInt16LE(at); at += 2;
      if (at + topicLen + 2 > r.payload.length) throw new KuttiDBError("invalid streamGroupList response");
      const topic = r.payload.subarray(at, at + topicLen).toString(); at += topicLen;
      const groupLen = r.payload.readUInt16LE(at); at += 2;
      if (at + groupLen + 12 > r.payload.length) throw new KuttiDBError("invalid streamGroupList response");
      groups.push({ topic, group: r.payload.subarray(at, at + groupLen).toString(),
        generation: r.payload.readBigUInt64LE(at + groupLen),
        members: r.payload.readUInt32LE(at + groupLen + 8) });
      at += groupLen + 12;
    }
    if (at !== r.payload.length) throw new KuttiDBError("invalid streamGroupList response");
    return groups;
  }

  async streamDeclare(topic, { partitions = 1, maxBytes = 0, maxAge = null } = {}) {
    const p = Buffer.concat([u32(partitions), u64(maxBytes),
      u64(maxAge == null ? 0 : Math.round(maxAge * 1000))]);
    this._ok(await this._req(OP.STREAM_DECLARE, asBuf(topic), p), "streamDeclare");
  }

  async streamAppend(topic, value, { key = Buffer.alloc(0), partition = null } = {}) {
    const kb = asBuf(key || ""), vb = asBuf(value);
    this._checkVal(vb);
    const hint = partition == null ? 0xffffffff : partition >>> 0;
    const r = this._ok(await this._req(OP.STREAM_APPEND, asBuf(topic),
      Buffer.concat([u32(hint), u16(kb.length), kb, vb])), "streamAppend");
    if (r.payload.length !== 16) throw new KuttiDBError("invalid streamAppend response");
    return { partition: r.payload.readBigUInt64LE(0), offset: r.payload.readBigUInt64LE(8) };
  }

  async streamAppendMany(topic, items, { partition = null } = {}) {
    await this._require(CAP.STREAM_BATCH, "stream batch append");
    const entries = Array.from(items);
    if (entries.length < 1 || entries.length > 1024)
      throw new KuttiDBError("stream batch size must be 1-1024");
    const hint = partition == null ? 0xffffffff : partition;
    if (!Number.isInteger(hint) || hint < 0 || hint > 0xffffffff)
      throw new KuttiDBError("invalid stream partition");
    const parts = [u32(hint), u32(entries.length)];
    for (const item of entries) {
      const pair = Array.isArray(item) && item.length === 2 ? item : [Buffer.alloc(0), item];
      const key = asBuf(pair[0]), value = asBuf(pair[1]);
      if (key.length > MAX_KEY) throw new KuttiDBError("stream key too large");
      this._checkVal(value);
      parts.push(u16(key.length), u32(value.length), key, value);
    }
    const r = this._ok(await this._req(OP.STREAM_APPEND_BATCH, asBuf(topic),
      Buffer.concat(parts)), "streamAppendMany");
    if (r.payload.length < 4) throw new KuttiDBError("invalid streamAppendMany response");
    const count = r.payload.readUInt32LE(0);
    if (count !== entries.length || r.payload.length !== 4 + count * 16)
      throw new KuttiDBError("invalid streamAppendMany response");
    return Array.from({ length: count }, (_, i) => ({
      partition: r.payload.readBigUInt64LE(4 + i * 16),
      offset: r.payload.readBigUInt64LE(12 + i * 16),
    }));
  }

  async streamFetch(topic, { partition = 0, offset = 0, maxRecords = 100 } = {}) {
    const caps = await this.capabilities();
    const keyed = Boolean(caps.features & CAP.STREAM_KEYS);
    const p = Buffer.concat([u32(partition), u64(offset), u32(maxRecords)]);
    const r = await this._req(keyed ? OP.STREAM_FETCH_KEYS : OP.STREAM_FETCH, asBuf(topic), p);
    if (r.status === STATUS_MISS) return [];
    this._ok(r, "streamFetch");
    if (r.payload.length < 4) throw new KuttiDBError("invalid streamFetch response");
    const count = r.payload.readUInt32LE(0);
    const records = [];
    let at = 4;
    for (let i = 0; i < count; i++) {
      const header = keyed ? 14 : 12;
      if (at + header > r.payload.length) throw new KuttiDBError("invalid streamFetch response");
      const recOffset = r.payload.readBigUInt64LE(at);
      const keyLen = keyed ? r.payload.readUInt16LE(at + 8) : 0;
      const len = r.payload.readUInt32LE(at + (keyed ? 10 : 8));
      at += header;
      if (at + keyLen + len > r.payload.length) throw new KuttiDBError("invalid streamFetch response");
      const record = { offset: recOffset,
        value: r.payload.subarray(at + keyLen, at + keyLen + len) };
      if (keyed) record.key = r.payload.subarray(at, at + keyLen);
      records.push(record);
      at += keyLen + len;
    }
    if (at !== r.payload.length) throw new KuttiDBError("invalid streamFetch response");
    return records;
  }

  async streamCommit(topic, group, partition, offset) {
    const gb = asBuf(group);
    this._ok(await this._req(OP.STREAM_COMMIT, asBuf(topic),
      Buffer.concat([u16(gb.length), gb, u32(partition), u64(offset)])), "streamCommit");
  }

  async streamCommitBatch(topic, group, commits) {
    await this._require(CAP.STREAM_COMMIT_BATCH, "stream batch offset commit");
    const entries = Array.from(commits), gb = asBuf(group);
    if (entries.length < 1 || entries.length > 256)
      throw new KuttiDBError("stream commit batch size must be 1-256");
    const parts = [u16(gb.length), gb, u32(entries.length)];
    for (const [partition, offset] of entries) parts.push(u32(partition), u64(offset));
    this._ok(await this._req(OP.STREAM_COMMIT_BATCH, asBuf(topic), Buffer.concat(parts)),
      "streamCommitBatch");
  }

  async streamGroupOffset(topic, group, partition) {
    const gb = asBuf(group);
    const r = await this._req(OP.STREAM_GROUP_OFFSET, asBuf(topic),
      Buffer.concat([u16(gb.length), gb, u32(partition)]));
    if (r.status === STATUS_MISS) return null;
    this._ok(r, "streamGroupOffset");
    if (r.payload.length !== 8) throw new KuttiDBError("invalid streamGroupOffset response");
    return r.payload.readBigUInt64LE(0);
  }

  async streamGroupJoin(topic, group, { lease = 30.0 } = {}) {
    const gb = asBuf(group);
    const r = this._ok(await this._req(OP.STREAM_GROUP_JOIN, asBuf(topic),
      Buffer.concat([u16(gb.length), gb, u32(Math.round(lease * 1000))])), "streamGroupJoin");
    const count = r.payload.readUInt32LE(0);
    const partitions = [];
    for (let i = 0; i < count; i++) partitions.push(r.payload.readUInt32LE(4 + i * 4));
    const genAt = 4 + count * 4;
    const generation = r.payload.length >= genAt + 8 ? r.payload.readBigUInt64LE(genAt) : 0n;
    return { partitions, generation };
  }

  async streamGroupLag(topic, group, partition) {
    const gb = asBuf(group);
    const r = await this._req(OP.STREAM_GROUP_LAG, asBuf(topic),
      Buffer.concat([u16(gb.length), gb, u32(partition)]));
    if (r.status === STATUS_MISS) return null;
    this._ok(r, "streamGroupLag");
    return Number(r.payload.readBigUInt64LE(0));
  }

  async streamGroupLeave(topic, group) {
    const gb = asBuf(group);
    this._ok(await this._req(OP.STREAM_GROUP_LEAVE, asBuf(topic),
      Buffer.concat([u16(gb.length), gb])), "streamGroupLeave");
  }

  // ---- atomic job completion (durable state + completion) ---------------------
  _jobErrorFromBody(body) {
    const code = body.length >= 1 ? body[0] : 0;
    const outcome = body.length >= 2 ? body[1] : 0;
    const detail = body.length > 2 ? body.subarray(2) : null;
    const ErrorType = JOB_ERROR_TYPES[code] || KuttiDBJobError;
    return new ErrorType(code, outcome, detail);
  }

  async _jobReq(op, key, payload) {
    const r = await this._req(op, key, payload);
    if (r.status === STATUS_ERR) throw this._jobErrorFromBody(r.payload);
    return r;
  }

  // Require the feature explicitly rather than assume it: without it the
  // server answers the typed unsupported_feature envelope and never emulates
  // the operation with separate writes.
  async _requireJobs() {
    const caps = await this.capabilities();
    if (!(caps.features & CAP.JOBS)) throw new JobUnsupportedFeatureError(1, 0);
  }

  async queueManifest() {
    // Additive Queue discovery: stable identity (incarnation), durability,
    // capacity, and revision per live Queue. Bounded at 256 entries.
    // Incarnation ids are required to compose completion intents for output
    // queues and are stable across restarts, changing only when a Queue is
    // deleted and recreated.
    await this._requireJobs();
    const r = this._ok(await this._jobReq(OP.QUEUE_MANIFEST), "queueManifest");
    if (r.payload.length < 2) throw new KuttiDBError("invalid queueManifest response");
    const count = r.payload.readUInt16LE(0), queues = [];
    let at = 2;
    for (let i = 0; i < count; i++) {
      if (at + 2 > r.payload.length) throw new KuttiDBError("invalid queueManifest response");
      const len = r.payload.readUInt16LE(at); at += 2;
      if (at + len + 41 > r.payload.length) throw new KuttiDBError("invalid queueManifest response");
      queues.push({ name: r.payload.subarray(at, at + len).toString(),
        durable: r.payload[at + len] !== 0,
        incarnation: r.payload.readBigUInt64LE(at + len + 1),
        depth: r.payload.readBigUInt64LE(at + len + 9),
        inflight: r.payload.readBigUInt64LE(at + len + 17),
        maxDepth: r.payload.readBigUInt64LE(at + len + 25),
        revision: r.payload.readBigUInt64LE(at + len + 33) });
      at += len + 41;
    }
    if (at !== r.payload.length) throw new KuttiDBError("invalid queueManifest response");
    return queues;
  }

  async jobConsume(name, consumer, { visibility = 30.0 } = {}) {
    // Deliver one message with a completion proof. Requires a durable Queue
    // and a registered named consumer (queueConsumerRegister); the
    // consumer's stable owner token owns the delivery, so pooled
    // connections stay interchangeable and a disconnected worker's
    // deliveries follow their visibility deadlines. The returned proof is
    // one-use: a committed completion retires it. Closing the connection
    // does not unregister the consumer.
    await this._requireJobs();
    const kb = asBuf(name), gb = asBuf(consumer);
    this._checkKey(kb);
    if (!gb.length || gb.length > 255 || visibility < 0)
      throw new KuttiDBError("invalid jobConsume request");
    const r = await this._jobReq(OP.JOB_CONSUME, kb,
      Buffer.concat([u16(gb.length), gb, u64(Math.round(visibility * 1000))]));
    if (r.status === STATUS_MISS) return null;
    if (r.payload.length < 61) throw new KuttiDBError("jobConsume failed");
    return new JobDelivery({
      storeId: r.payload.subarray(0, 16),
      queue: name,
      queueIncarnation: r.payload.readBigUInt64LE(16),
      messageId: r.payload.readBigUInt64LE(24),
      attempts: r.payload.readUInt32LE(32),
      redelivered: r.payload[36] !== 0,
      leaseDeadlineMs: r.payload.readBigUInt64LE(37),
      proof: r.payload.subarray(45, 61),
      value: r.payload.subarray(61),
    });
  }

  async jobComplete(intent, proof) {
    // Submit one atomic completion: durable-state PUT + input ACK + optional
    // output publish + receipt, committed together. The intent carries the
    // stable identity and the full semantic request; proof is the opaque
    // credential from the current jobConsume delivery. On a timeout or
    // disconnect keep the exact intent and id, then retry the same call or
    // use jobCompletion to query the receipt — never regenerate the id and
    // never issue a separate ACK after a success.
    await this._requireJobs();
    const pb = asBuf(proof);
    if (pb.length !== 16) throw new KuttiDBError("delivery proof must be 16 bytes");
    const outInc = BigInt(intent.outputIncarnation);
    if (intent.outputQueue != null && outInc === 0n)
      throw new KuttiDBError("output intent requires its queue incarnation");
    const q = asBuf(intent.queue), sk = asBuf(intent.stateKey),
          sv = asBuf(intent.stateValue);
    this._checkVal(sv);
    const parts = [intent.operationId, u64(intent.queueIncarnation),
      u64(intent.messageId), pb, u16(sk.length), sk,
      u64(intent.expectedVersion), u32(sv.length), sv];
    if (intent.outputQueue == null) {
      parts.push(Buffer.from([0]));
    } else {
      const oq = asBuf(intent.outputQueue), ov = asBuf(intent.outputValue);
      this._checkVal(ov);
      parts.push(Buffer.from([1]), u16(oq.length), oq, u64(outInc),
        u32(ov.length), ov);
    }
    const r = await this._jobReq(OP.JOB_COMPLETE, q, Buffer.concat(parts));
    if (r.payload.length !== 41) throw new KuttiDBError("jobComplete failed");
    return new JobCompletionResult({
      commitId: r.payload.readBigUInt64LE(0),
      stateVersion: r.payload.readBigUInt64LE(8),
      outputMessageId: r.payload.readBigUInt64LE(16),
      completedAtMs: r.payload.readBigUInt64LE(24),
      receiptExpiresMs: r.payload.readBigUInt64LE(32),
      replayed: r.payload[40] !== 0,
    });
  }

  async jobCompletion(operationId) {
    // Look up a retained completion receipt by operation id. Authenticated
    // lookup never requires the (now stale) delivery proof and works after
    // a restart. A miss means "no retained receipt" — absence is never
    // proof that the operation never executed.
    await this._requireJobs();
    const op = operationIdBytes(operationId);
    const r = await this._jobReq(OP.JOB_RECEIPT, Buffer.alloc(0), op);
    if (r.status === STATUS_MISS) return null;
    if (r.payload.length !== 41) throw new KuttiDBError("jobCompletion lookup failed");
    return new JobReceipt({
      operationId: op,
      commitId: r.payload.readBigUInt64LE(0),
      stateVersion: r.payload.readBigUInt64LE(8),
      outputMessageId: r.payload.readBigUInt64LE(16),
      completedAtMs: r.payload.readBigUInt64LE(24),
      receiptExpiresMs: r.payload.readBigUInt64LE(32),
    });
  }

  async stateGet(key) {
    // Read one durable-state entry: exact value bytes, its version, and the
    // commit id that last wrote it. The "durable" keyspace is fixed,
    // non-evictable, and never expires.
    await this._requireJobs();
    const kb = asBuf(key);
    if (!kb.length || kb.length > MAX_KEY) throw new KuttiDBError("invalid durable state key");
    const r = await this._jobReq(OP.STATE_GET, kb);
    if (r.status === STATUS_MISS) return null;
    if (r.payload.length < 16) throw new KuttiDBError("stateGet failed");
    return { version: r.payload.readBigUInt64LE(0),
             commitId: r.payload.readBigUInt64LE(8),
             value: r.payload.subarray(16) };
  }

  async statePut(key, value, { expectedVersion = 0, operationId = null } = {}) {
    // Version-checked direct durable-state PUT with its own receipt.
    // expectedVersion=0 creates only; a positive value must match the
    // current version exactly (no unchecked overwrite path exists). The
    // same operation id may be retried unchanged to reconcile a lost
    // response; a reused id with different content raises
    // JobIdempotencyConflictError.
    await this._requireJobs();
    const kb = asBuf(key), vb = asBuf(value || Buffer.alloc(0));
    if (!kb.length || kb.length > MAX_KEY || expectedVersion < 0)
      throw new KuttiDBError("invalid statePut request");
    this._checkVal(vb);
    const op = operationIdBytes(operationId);
    const r = await this._jobReq(OP.STATE_PUT, kb,
      Buffer.concat([op, u64(expectedVersion), vb]));
    if (r.payload.length !== 33) throw new KuttiDBError("statePut failed");
    return new JobMutationReceipt({
      operationId: op,
      kind: "state_put",
      commitId: r.payload.readBigUInt64LE(0),
      stateVersion: r.payload.readBigUInt64LE(8),
      completedAtMs: r.payload.readBigUInt64LE(16),
      receiptExpiresMs: r.payload.readBigUInt64LE(24),
      replayed: r.payload[32] !== 0,
    });
  }

  async stateDelete(key, { expectedVersion, operationId = null } = {}) {
    // Version-checked direct durable-state DELETE with its own receipt.
    // Requires the entry's current positive version. Retrying a committed
    // delete with the same id returns its retained receipt even though the
    // entry is already absent; deleting an absent key without a retained
    // receipt is a definite not-found.
    await this._requireJobs();
    const kb = asBuf(key);
    if (!kb.length || kb.length > MAX_KEY ||
        !(toU64(expectedVersion, "invalid expected version") > 0n))
      throw new KuttiDBError("invalid stateDelete request");
    const op = operationIdBytes(operationId);
    const r = await this._jobReq(OP.STATE_DELETE, kb,
      Buffer.concat([op, u64(expectedVersion)]));
    if (r.status === STATUS_MISS || r.payload.length !== 33)
      throw new KuttiDBError("stateDelete failed");
    return new JobMutationReceipt({
      operationId: op,
      kind: "state_delete",
      commitId: r.payload.readBigUInt64LE(0),
      stateVersion: r.payload.readBigUInt64LE(8),
      completedAtMs: r.payload.readBigUInt64LE(16),
      receiptExpiresMs: r.payload.readBigUInt64LE(24),
      replayed: r.payload[32] !== 0,
    });
  }

  async durableOperation(operationId) {
    // Look up a retained direct-state mutation receipt (shared
    // operation-id ledger). kind is "state_put" or "state_delete".
    await this._requireJobs();
    const op = operationIdBytes(operationId);
    const r = await this._jobReq(OP.DURABLE_OPERATION, Buffer.alloc(0), op);
    if (r.status === STATUS_MISS) return null;
    if (r.payload.length !== 33) throw new KuttiDBError("durableOperation lookup failed");
    const kind = { 2: "state_put", 3: "state_delete" }[r.payload[0]] ||
      `kind_${r.payload[0]}`;
    return { kind,
             commitId: r.payload.readBigUInt64LE(1),
             stateVersion: r.payload.readBigUInt64LE(9),
             completedAtMs: r.payload.readBigUInt64LE(17),
             receiptExpiresMs: r.payload.readBigUInt64LE(25) };
  }

  async close() {
    this.closed = true;
    const all = this.idle;
    this.idle = [];
    for (const w of this.waiters) w.rej(new KuttiDBError("client closed"));
    this.waiters = [];
    for (const c of all) { try { c.destroy(); } catch { /* ignore */ } }
  }
}

module.exports = {
  Client, KuttiDBError,
  KuttiDBJobError,
  JobUnsupportedFeatureError, JobValidationFailedError, JobRequestTooLargeError,
  JobIdempotencyConflictError, JobStateVersionConflictError,
  JobDeliveryExpiredError, JobDeliveryNotOwnedError, JobResourceExhaustedError,
  JobOperationInDoubtError, JobPersistenceUnavailableError,
  JobDelivery, JobCompletionIntent, JobCompletionResult, JobMutationReceipt,
  JobReceipt,
  OP, CAP,
};
