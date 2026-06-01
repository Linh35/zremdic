// A zremdic client for Node, over UDP, using only the built-in dgram module. It speaks the same
// binary protocol as the Zig client: a request datagram carries a 32-bit id, op byte, key, and
// value; the reply carries the id, a status byte, and a value. Requests are matched to replies by
// id and retransmitted on timeout. Run this file directly for a short demo: node client.js [port].

const dgram = require("node:dgram");

const OP = { get: 1, set: 2, del: 3, ping: 4, subscribe: 5, unsubscribe: 6, update: 7 };
const STATUS = { ok: 0, not_found: 1, too_large: 2, bad_request: 3 };
const REQ_HEADER = 11;
const RESP_HEADER = 9;

class ZremdicClient {
  constructor(host = "127.0.0.1", port = 6380, { timeoutMs = 200, retries = 3 } = {}) {
    this.host = host;
    this.port = port;
    this.timeoutMs = timeoutMs;
    this.retries = retries;
    this.nextId = 1;
    this.pending = new Map(); // id -> { resolve, reject, timer }
    this.onUpdate = null; // optional (key, value) => void, called when a subscribed key changes
    this.sock = dgram.createSocket("udp4");
    this.sock.on("message", (msg) => this._receive(msg));
    this.sock.on("error", () => {});
  }

  close() {
    for (const p of this.pending.values()) clearTimeout(p.timer);
    this.pending.clear();
    this.sock.close();
  }

  ping() {
    return this._request(OP.ping, "").then((r) => {
      if (r.status !== STATUS.ok) throw new Error("zremdic: server error");
    });
  }

  set(key, value) {
    return this._request(OP.set, key, value).then((r) => {
      if (r.status === STATUS.too_large) throw new Error("zremdic: key or value too large");
      if (r.status !== STATUS.ok) throw new Error("zremdic: server error");
    });
  }

  get(key) {
    return this._request(OP.get, key).then((r) => {
      if (r.status === STATUS.not_found) return null;
      if (r.status !== STATUS.ok) throw new Error("zremdic: server error");
      return r.value.toString("utf8");
    });
  }

  del(key) {
    return this._request(OP.del, key).then((r) => {
      if (r.status !== STATUS.ok) throw new Error("zremdic: server error");
    });
  }

  subscribe(key) {
    return this._request(OP.subscribe, key).then((r) => {
      if (r.status !== STATUS.ok) throw new Error("zremdic: server error");
    });
  }

  unsubscribe(key) {
    return this._request(OP.unsubscribe, key).then((r) => {
      if (r.status !== STATUS.ok) throw new Error("zremdic: server error");
    });
  }

  _encode(id, op, key, value) {
    const k = Buffer.from(key);
    const v = value ? Buffer.from(value) : Buffer.alloc(0);
    const buf = Buffer.allocUnsafe(REQ_HEADER + k.length + v.length);
    buf.writeUInt32LE(id, 0);
    buf.writeUInt8(op, 4);
    buf.writeUInt16LE(k.length, 5);
    buf.writeUInt32LE(v.length, 7);
    k.copy(buf, REQ_HEADER);
    v.copy(buf, REQ_HEADER + k.length);
    return buf;
  }

  _receive(msg) {
    if (msg.length < 4) return;
    const id = msg.readUInt32LE(0);
    if (id === 0) {
      // A change push: request-shaped, op=update, then key and value.
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

  _request(op, key, value) {
    const id = this.nextId;
    this.nextId = this.nextId >= 0xffffffff ? 1 : this.nextId + 1;
    const buf = this._encode(id, op, key, value);
    return new Promise((resolve, reject) => {
      let attempts = 0;
      const send = () => {
        attempts += 1;
        this.sock.send(buf, this.port, this.host);
        const timer = setTimeout(() => {
          if (attempts >= this.retries) {
            this.pending.delete(id);
            reject(new Error("zremdic: timeout"));
          } else {
            send();
          }
        }, this.timeoutMs);
        this.pending.set(id, { resolve, reject, timer });
      };
      send();
    });
  }
}

module.exports = { ZremdicClient, OP, STATUS };

if (require.main === module) {
  (async () => {
    const port = parseInt(process.argv[2] || "6380", 10);
    const c = new ZremdicClient("127.0.0.1", port);
    c.onUpdate = (key, value) => console.log(`pushed: ${key} = ${value}`);

    await c.ping();
    console.log("ping ok");

    await c.set("user:1", "ada");
    console.log("get user:1  =", await c.get("user:1"));
    console.log("get missing =", await c.get("missing"));

    await c.subscribe("score");
    await c.set("score", "42"); // the server pushes this change back to us
    await new Promise((r) => setTimeout(r, 100));

    await c.del("user:1");
    console.log("get user:1  =", await c.get("user:1"));

    c.close();
  })().catch((err) => {
    console.error(err.message);
    process.exit(1);
  });
}
