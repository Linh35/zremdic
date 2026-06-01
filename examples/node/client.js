// A zremdic client for Node, over UDP, using only the built-in dgram module. It speaks the same
// binary protocol as the Zig client: a request datagram carries a 32-bit id, op byte, key, value,
// and a ttl; the reply carries the id, a status byte, and a value. Requests are matched to replies
// by id and retransmitted with exponential backoff. Run this file directly for a demo:
// node client.js [port].

const dgram = require("node:dgram");

const OP = { get: 1, set: 2, del: 3, ping: 4, subscribe: 5, unsubscribe: 6, update: 7, stats: 8, batch: 9 };
const STATUS = { ok: 0, not_found: 1, too_large: 2, bad_request: 3 };
const REQ_HEADER = 15; // id(4) op(1) key_len(2) val_len(4) ttl(4)
const RESP_HEADER = 9; // id(4) status(1) val_len(4)
const SUB_REQ_HEADER = 11; // op(1) key_len(2) val_len(4) ttl(4)
const SUB_RES_HEADER = 5; // status(1) val_len(4)
const STATS_FIELDS = ["gets", "sets", "dels", "hits", "misses", "pushes", "expired", "keys", "subscribers"];

class ZremdicClient {
  constructor(host = "127.0.0.1", port = 6380, { timeoutMs = 200, retries = 3, backoffCapMs = 2000 } = {}) {
    this.host = host;
    this.port = port;
    this.timeoutMs = timeoutMs;
    this.retries = retries;
    this.backoffCapMs = Math.max(timeoutMs, backoffCapMs);
    this.nextId = 1;
    this.pending = new Map(); // id -> { resolve, reject, attempts, buf, timer }
    this.onUpdate = null; // optional (key, value) => void, called when a subscribed key changes
    this.sock = dgram.createSocket("udp4");
    this.sock.on("message", (msg) => this._receive(msg));
    this.sock.on("error", () => {});
    this.sock.on("listening", () => {
      try {
        this.sock.setSendBufferSize(1 << 20); // room for large values
        this.sock.setRecvBufferSize(1 << 20);
      } catch {}
    });
    this.sock.bind();
  }

  close() {
    for (const p of this.pending.values()) clearTimeout(p.timer);
    this.pending.clear();
    this.sock.close();
  }

  ping() {
    return this._request(OP.ping, "").then(expectOk);
  }
  set(key, value) {
    return this.setEx(key, value, 0);
  }
  setEx(key, value, ttlMs) {
    return this._request(OP.set, key, value, ttlMs).then((r) => {
      if (r.status === STATUS.too_large) throw new Error("zremdic: key or value too large");
      expectOk(r);
    });
  }
  get(key) {
    return this._request(OP.get, key).then((r) => {
      if (r.status === STATUS.not_found) return null;
      expectOk(r);
      return r.value.toString("utf8");
    });
  }
  del(key) {
    return this._request(OP.del, key).then(expectOk);
  }
  subscribe(key) {
    return this._request(OP.subscribe, key).then(expectOk);
  }
  unsubscribe(key) {
    return this._request(OP.unsubscribe, key).then(expectOk);
  }
  stats() {
    return this._request(OP.stats, "").then((r) => {
      expectOk(r);
      const s = {};
      STATS_FIELDS.forEach((name, i) => (s[name] = Number(r.value.readBigUInt64LE(i * 8))));
      return s;
    });
  }

  // Build a batch with .set/.setEx/.get/.del, then await .send() for the results in order.
  batch() {
    return new Batch(this);
  }

  _encode(id, op, key, value, ttlMs) {
    const k = Buffer.from(key);
    const v = value ? Buffer.from(value) : Buffer.alloc(0);
    const buf = Buffer.allocUnsafe(REQ_HEADER + k.length + v.length);
    buf.writeUInt32LE(id, 0);
    buf.writeUInt8(op, 4);
    buf.writeUInt16LE(k.length, 5);
    buf.writeUInt32LE(v.length, 7);
    buf.writeUInt32LE(ttlMs || 0, 11);
    k.copy(buf, REQ_HEADER);
    v.copy(buf, REQ_HEADER + k.length);
    return buf;
  }

  _receive(msg) {
    if (msg.length < 4) return;
    const id = msg.readUInt32LE(0);
    if (id === 0) {
      // A change push: request-shaped, op=update, with the key and its new value (empty = deleted).
      if (msg.length >= REQ_HEADER && msg.readUInt8(4) === OP.update && this.onUpdate) {
        const klen = msg.readUInt16LE(5);
        const vlen = msg.readUInt32LE(7);
        const key = msg.toString("utf8", REQ_HEADER, REQ_HEADER + klen);
        const value = msg.toString("utf8", REQ_HEADER + klen, REQ_HEADER + klen + vlen);
        this.onUpdate(key, value);
      }
      return;
    }
    const p = this.pending.get(id);
    if (!p || msg.length < RESP_HEADER) return;
    const status = msg.readUInt8(4);
    const vlen = msg.readUInt32LE(5);
    const value = msg.subarray(RESP_HEADER, RESP_HEADER + vlen);
    clearTimeout(p.timer);
    this.pending.delete(id);
    p.resolve({ status, value });
  }

  _send(id, buf) {
    return new Promise((resolve, reject) => {
      let attempt = 0;
      const fire = () => {
        this.sock.send(buf, this.port, this.host);
        let wait = this.timeoutMs * 2 ** attempt;
        if (wait > this.backoffCapMs) wait = this.backoffCapMs;
        wait += Math.floor(Math.random() * (wait / 2 + 1)); // jitter
        attempt += 1;
        const timer = setTimeout(() => {
          if (attempt >= this.retries) {
            this.pending.delete(id);
            reject(new Error("zremdic: timeout"));
          } else {
            fire();
          }
        }, wait);
        this.pending.set(id, { resolve, reject, timer });
      };
      fire();
    });
  }

  _request(op, key, value, ttlMs) {
    const id = this.nextId;
    this.nextId = this.nextId >= 0xffffffff ? 1 : this.nextId + 1;
    return this._send(id, this._encode(id, op, key, value, ttlMs));
  }

  _sendBatch(body) {
    const id = this.nextId;
    this.nextId = this.nextId >= 0xffffffff ? 1 : this.nextId + 1;
    const buf = Buffer.allocUnsafe(REQ_HEADER + body.length);
    buf.writeUInt32LE(id, 0);
    buf.writeUInt8(OP.batch, 4);
    buf.writeUInt16LE(0, 5);
    buf.writeUInt32LE(body.length, 7);
    buf.writeUInt32LE(0, 11);
    body.copy(buf, REQ_HEADER);
    return this._send(id, buf);
  }
}

class Batch {
  constructor(client) {
    this.client = client;
    this.parts = [];
  }
  set(key, value) {
    return this._add(OP.set, key, value, 0);
  }
  setEx(key, value, ttlMs) {
    return this._add(OP.set, key, value, ttlMs);
  }
  get(key) {
    return this._add(OP.get, key, "", 0);
  }
  del(key) {
    return this._add(OP.del, key, "", 0);
  }
  _add(op, key, value, ttlMs) {
    const k = Buffer.from(key);
    const v = value ? Buffer.from(value) : Buffer.alloc(0);
    const part = Buffer.allocUnsafe(SUB_REQ_HEADER + k.length + v.length);
    part.writeUInt8(op, 0);
    part.writeUInt16LE(k.length, 1);
    part.writeUInt32LE(v.length, 3);
    part.writeUInt32LE(ttlMs || 0, 7);
    k.copy(part, SUB_REQ_HEADER);
    v.copy(part, SUB_REQ_HEADER + k.length);
    this.parts.push(part);
    return this;
  }
  // Send the queued operations and resolve to their results in order: [{ status, value }, ...].
  async send() {
    const r = await this.client._sendBatch(Buffer.concat(this.parts));
    if (r.status !== STATUS.ok) throw new Error("zremdic: server error");
    const results = [];
    let pos = 0;
    while (pos + SUB_RES_HEADER <= r.value.length) {
      const status = r.value.readUInt8(pos);
      const vlen = r.value.readUInt32LE(pos + 1);
      const value = r.value.subarray(pos + SUB_RES_HEADER, pos + SUB_RES_HEADER + vlen);
      results.push({ status, value });
      pos += SUB_RES_HEADER + vlen;
    }
    return results;
  }
}

function expectOk(r) {
  if (r.status !== STATUS.ok) throw new Error("zremdic: server error");
}

module.exports = { ZremdicClient, OP, STATUS };

if (require.main === module) {
  (async () => {
    const port = parseInt(process.argv[2] || "6380", 10);
    const c = new ZremdicClient("127.0.0.1", port);

    // Pushed changes are best effort: a datagram can be dropped, and a slow set then a fast one can
    // arrive out of order. Treat an update as a hint, and dedupe by remembering the last value seen.
    const lastSeen = new Map();
    c.onUpdate = (key, value) => {
      if (lastSeen.get(key) === value) return; // duplicate or stale, ignore
      lastSeen.set(key, value);
      console.log(`changed: ${key} = ${value === "" ? "<deleted>" : value}`);
    };

    await c.ping();
    await c.set("user:1", "ada");
    console.log("get user:1  =", await c.get("user:1"));
    console.log("get missing =", await c.get("missing"));

    await c.setEx("session", "token", 1000); // expires in 1s
    await c.subscribe("score");
    await c.set("score", "42"); // pushed back to us
    await new Promise((r) => setTimeout(r, 50));

    const results = await c.batch().set("a", "1").set("b", "2").get("a").get("nope").send();
    console.log("batch results:", results.map((r) => ({ status: r.status, value: r.value.toString() })));

    console.log("stats:", await c.stats());

    await c.del("user:1");
    c.close();
  })().catch((err) => {
    console.error(err.message);
    process.exit(1);
  });
}
