# zremdic

The zig remote dictionary: a small, latency-focused key-value cache served over UDP. It is meant to run inside a trusted network, so it has no authentication and no encryption. Each request is one datagram and each reply is one datagram, which keeps a lookup to a single round trip. The store is sharded and the server runs a worker thread per core, so it scales across a machine.

This is a cache, not a database. Values live in memory, nothing is written to disk, and a server restart starts empty. Datagrams can be lost, so a value can be missed under heavy load. That is the right trade for shared state that can be recomputed or refetched.

## At a glance

```zig
const zremdic = @import("zremdic");

var server = try zremdic.Server.init(gpa, .{ .port = 6380, .threads = 8, .shards = 64 });
defer server.deinit();
try server.start();

var client = try zremdic.Client.init("127.0.0.1", 6380, .{});
defer client.deinit();

try client.set("user:1", "ada");
var buf: [256]u8 = undefined;
const name = try client.get("user:1", &buf); // ?[]u8, null when absent
try client.del("user:1");
```

## Building and running

You need Zig 0.16.

```sh
zig build              # build the server binary into zig-out/bin/zremdic-server
zig build run          # build and run the server (binds 0.0.0.0:6380, a thread per core)
zig build test         # protocol, store, and end-to-end tests
zig build example      # a runnable tour of the client API against an in-process server
zig build bench        # a loopback throughput and latency benchmark
```

The server binary takes optional positional arguments for its initial size:

```sh
zremdic-server [port] [threads] [shards] [capacity]
```

- `port` to bind on `0.0.0.0` (default 6380)
- `threads`, one worker socket each (default: CPU count)
- `shards`, the store's concurrency width (default 64)
- `capacity`, the number of keys to pre-size the store for so it does not rehash as it fills (default 0, grow on demand)

With no arguments it binds `0.0.0.0:6380`, runs one worker thread per CPU, and serves until stopped. Through `zig build`, pass arguments after `--`, as in `zig build run -- 6380 8 128 1000000`.

## The store

Keys and values are bytes. A key is at most 256 bytes and a value at most 16 KiB. Set replaces, get reads, del removes, and setting a key over the limits returns a `too_large` status rather than truncating. A small value and its reply each fit in one datagram below a typical MTU and never fragment; a value larger than the MTU travels in one fragmented datagram, which is fine on a low-loss network and is the reason this is a cache, not a store for irreplaceable blobs.

A set may carry a time to live in milliseconds. The key then expires that long after the write, and the first read past the deadline finds it gone and drops it. Expiry is lazy, so it costs nothing for keys that have no TTL.

The keyspace is split into shards by key hash, each shard a hash map behind its own spinlock. Worker threads contend only when they touch the same shard, and a reader copies the value out under the lock, so it never observes memory a writer is freeing. More shards means less contention; the server binary uses 64. The shard count is rounded up to a power of two, so a key maps to its shard with a single bit-and rather than a modulo. The server's `capacity` argument pre-sizes the shards so the store does not rehash as it fills.

## Watching a key

A client can ask to be told when a key changes. After `subscribe(key)`, every later `set` of that key makes the server push the new value to the subscriber as an `update` datagram. The client reads those with `pollUpdate`.

```zig
try watcher.subscribe("score");
// in another client, or another process:
try writer.set("score", "42");

var kbuf: [64]u8 = undefined;
var vbuf: [256]u8 = undefined;
if (try watcher.pollUpdate(&kbuf, &vbuf)) |change| {
    // change.key == "score", change.value == "42"
}
```

A delete pushes too, with an empty value, so a watcher learns the key is gone. Pushes are best effort: a dropped push is not retransmitted, and two quick writes can arrive in either order, so treat an update as a hint to read the key rather than as a guaranteed event log. Because a push can repeat or arrive stale, dedupe on the client by remembering the last value seen for a key and ignoring a push that matches it. A key holds at most 32 subscribers; further subscriptions to a full key are dropped.

## The client

`Client.init(ip, port, options)` opens a UDP socket and connects it to the server, so each call uses send and recv with no per-call address and the kernel drops datagrams from any other host. Options are `timeout_ms` (default 200, the first wait before a retransmit), `retries` (default 3), and `backoff_cap_ms` (default 2000). Each call sends one request and waits for the reply with the matching id. If the wait elapses it retransmits, doubling the wait each attempt up to the cap and adding jitter, so a busy server is not buried under synchronized retries. Because get, set, and del are idempotent, a retransmit is harmless.

- `ping() !void`
- `set(key, value) !void`, errors `error.TooLarge` if over the limits
- `setEx(key, value, ttl_ms) !void`, the same with an expiry (0 means none)
- `get(key, out: []u8) !?[]u8`, copies the value into `out` and returns that slice, or null if absent
- `del(key) !void`
- `subscribe(key) !void`, `unsubscribe(key) !void`
- `pollUpdate(key_out, val_out) !?Update`, waits up to one timeout for a pushed change
- `stats() !Stats`, the server's counters
- `batch()` returns a `Batch`; queue `set`/`setEx`/`get`/`del`, then `send(resp_buf)` for the results in order
- If no reply arrives within the retry budget, the call returns `error.Timeout`

A batch is the way to go fast. One datagram carries many operations and comes back as one reply, so the round trip is paid once for the whole batch instead of once per operation. The queued operations and their combined results each fit in one value (16 KiB), so a batch is for many small operations.

```zig
var b = client.batch();
try b.set("a", "1");
try b.get("a");
var rbuf: [zremdic.proto.max_datagram]u8 = undefined;
var results = try b.send(&rbuf);
while (results.next()) |r| {
    // r.status, r.value, in the order the operations were queued
}
```

## Using it from Node

`examples/node/client.js` is a complete client in one file, using only the built-in `dgram` module. It implements the same wire format, matches replies by id, and retransmits on timeout.

```js
const { ZremdicClient } = require("./examples/node/client.js");

const c = new ZremdicClient("127.0.0.1", 6380);

// Dedupe pushed changes: remember the last value and ignore a push that repeats it.
const lastSeen = new Map();
c.onUpdate = (key, value) => {
  if (lastSeen.get(key) === value) return;
  lastSeen.set(key, value);
  // value === "" means the key was deleted
};

await c.set("user:1", "ada");
await c.setEx("session", "token", 1000); // expires in 1 second
console.log(await c.get("user:1")); // "ada"
await c.subscribe("score");

const results = await c.batch().set("a", "1").get("a").send(); // one round trip
console.log(await c.stats()); // { gets, sets, hits, misses, keys, ... }
c.close();
```

Run the file directly for a demo against a running server: `node examples/node/client.js 6380`.

## The wire protocol

Every integer is little-endian. A request is one datagram:

```
offset  size  field
0       4     id          a value the client chooses to match the reply
4       1     op          1 get, 2 set, 3 del, 4 ping, 5 subscribe, 6 unsubscribe, 8 stats, 9 batch
5       2     key_len
7       4     val_len     the value for set, or the body for batch
11      4     ttl_ms      for set: lifetime in milliseconds, 0 for none
15      ..    key bytes
..      ..    value bytes
```

A reply is one datagram:

```
offset  size  field
0       4     id          echoes the request id
4       1     status      0 ok, 1 not_found, 2 too_large, 3 bad_request
5       4     val_len     the value for a get, the stats row, or the batch results
9       ..    value bytes
```

A change push has the request layout with `op` 7 (`update`) and `id` 0, carrying the key and its new value (empty when the key was deleted). A client tells a push from a reply by the zero id, since a client never sends id 0.

A `stats` reply's value is a row of nine little-endian u64 counters: gets, sets, dels, hits, misses, pushes, expired, keys, subscribers.

A `batch` request's value is a sequence of sub-requests, each `op(1) key_len(2) val_len(4) ttl(4)` then the key and value. The reply's value is the matching sequence of sub-results, each `status(1) val_len(4)` then the value, in the same order.

## How fast

From `zig build bench -Doptimize=ReleaseFast` on an Apple M3 over loopback, with four client threads and four server threads:

```
800000 round trips in ~3.8s
~210000 ops/sec, ~19 us average round trip
```

Each client runs its operations one at a time, so this measures round-trip latency, which on loopback is dominated by the socket syscalls and the thread wakeup per round trip. On a real network the round trip is dominated by the network itself. Three things raise throughput. Aggregate rises with the number of concurrent clients and server threads, because `SO_REUSEPORT` lets the kernel spread datagrams across the worker sockets. On Linux each worker receives and replies to a batch of datagrams per syscall with `recvmmsg` and `sendmmsg`, which cuts the syscall count under load. And a batch pays one round trip for many operations, so a batch of twenty small operations does roughly twenty times the work of one operation for the same latency.

## Handling the rough edges

UDP and best-effort delivery leave a few cases for a client to handle, and the bundled clients show each.

A call can time out. If no reply arrives within the retry budget, the call returns `error.Timeout`. Retry it, or treat it as a miss; the backoff already spaces the retransmits.

A pushed change can be dropped, duplicated, or arrive out of order. Dedupe by remembering the last value seen for each key and ignoring a push that matches it, and treat a push as a prompt to read the key rather than as the value itself. An empty value in a push means the key was deleted.

A large value fragments. On a low-loss network that is fine; under heavy loss it lowers the odds the whole value arrives, since losing one fragment loses the datagram. On macOS the default per-datagram limit (`net.inet.udp.maxdgram`, near 9 KiB) caps values before the 16 KiB protocol limit, so raise that sysctl, or stay under it, for the largest values. Linux handles the full 16 KiB with the roomy socket buffers the client and server set.

## Scaling and what is next

The server binds one socket per worker with `SO_REUSEPORT` and shards the store, so it uses every core; on Linux each worker batches its receives and sends with `recvmmsg` and `sendmmsg`; a get writes the stored value straight into the reply datagram with no intermediate copy; a key maps to its shard with one bit-and rather than a modulo; and the client connects its socket so each call skips the per-call address and a route lookup. The remaining steps for higher packet rates are larger socket buffers under bursty load and, for read-heavy keys, a sequence-lock read path that lets readers avoid the shard lock. Horizontal scale beyond one box is client-side consistent hashing across several servers.

A few limits are deliberate. There is no authentication, so bind it only to a trusted network. Values are capped at 16 KiB, and one over the MTU is sent fragmented. Operations that are not idempotent, such as a counter, would need server-side request deduplication before they could ride the same retransmitting client, so they are left out until that exists.

## Layout

```
build.zig            build, with server, bench, example, and test steps
build.zig.zon        the package manifest
src/proto.zig        the wire protocol: encode and decode
src/store.zig        the sharded in-memory dictionary and subscriber registry
src/server.zig       the UDP server: reuseport sockets, worker threads, change pushes
src/client.zig       the client: round trips, retransmit, pushed updates
src/zremdic.zig      the library root and end-to-end tests
src/main.zig         the server binary
src/bench.zig        the loopback benchmark
examples/usage.zig   a runnable tour of the client API
examples/node/client.js  a Node client over dgram
```
