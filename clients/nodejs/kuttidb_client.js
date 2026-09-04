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

  async close() {
    this.closed = true;
    const all = this.idle;
    this.idle = [];
    for (const w of this.waiters) w.rej(new KuttiDBError("client closed"));
    this.waiters = [];
    for (const c of all) { try { c.destroy(); } catch { /* ignore */ } }
  }
}

module.exports = { Client, KuttiDBError, OP, CAP };
