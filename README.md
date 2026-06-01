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

Keys and values are bytes. A key is at most 256 bytes and a value at most 1024, so a request and its reply each fit in one datagram below a typical 1500-byte path MTU and never fragment. Set replaces, get reads, del removes. Setting a key over its limit returns a `too_large` status rather than truncating.

The keyspace is split into shards by key hash, each shard a hash map behind its own spinlock. Worker threads contend only when they touch the same shard, and a reader copies the value out under the lock, so it never observes memory a writer is freeing. More shards means less contention; the server binary uses 64.

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

Pushes are best effort. A dropped push is not retransmitted, so treat an update as a hint to read the key, not as a guaranteed event log. A key holds at most 32 subscribers; further subscriptions to a full key are dropped.

## The client

`Client.init(ip, port, options)` opens a UDP socket and sets a receive timeout. Options are `timeout_ms` (default 200) and `retries` (default 3). Each call sends one request and waits for the reply with the matching id, retransmitting if the receive times out. Because get, set, and del are idempotent, a retransmit is harmless.

- `ping() !void`
- `set(key, value) !void`, errors `error.TooLarge` if over the limits
- `get(key, out: []u8) !?[]u8`, copies the value into `out` and returns that slice, or null if absent
- `del(key) !void`
- `subscribe(key) !void`, `unsubscribe(key) !void`
- `pollUpdate(key_out, val_out) !?Update`, waits up to one receive timeout for a pushed change
- If no reply arrives within the retry budget, the call returns `error.Timeout`

## Using it from Node

`examples/node/client.js` is a complete client in one file, using only the built-in `dgram` module. It implements the same wire format, matches replies by id, and retransmits on timeout.

```js
const { ZremdicClient } = require("./examples/node/client.js");

const c = new ZremdicClient("127.0.0.1", 6380);
c.onUpdate = (key, value) => console.log(`pushed: ${key} = ${value}`);

await c.set("user:1", "ada");
console.log(await c.get("user:1")); // "ada"
console.log(await c.get("missing")); // null
await c.subscribe("score");
await c.del("user:1");
c.close();
```

Run the file directly for a demo against a running server: `node examples/node/client.js 6380`.

## The wire protocol

Every integer is little-endian. A request is one datagram:

```
offset  size  field
0       4     id          a value the client chooses to match the reply
4       1     op          1 get, 2 set, 3 del, 4 ping, 5 subscribe, 6 unsubscribe
5       2     key_len
7       4     val_len     used by set; zero otherwise
11      ..    key bytes
..      ..    value bytes
```

A reply is one datagram:

```
offset  size  field
0       4     id          echoes the request id
4       1     status      0 ok, 1 not_found, 2 too_large, 3 bad_request
5       4     val_len     the value length for a get reply
9       ..    value bytes
```

A change push has the request layout with `op` 7 (`update`) and `id` 0, carrying the key and its new value. A client tells a push from a reply by the zero id, since a client never sends id 0.

## How fast

From `zig build bench -Doptimize=ReleaseFast` on an Apple M3 over loopback, with four client threads and four server threads:

```
800000 round trips in ~4.1s
~194000 ops/sec, ~20 us average round trip
```

Each client runs its operations one at a time, so this measures round-trip latency, which on loopback is dominated by the four socket syscalls and the thread wakeup per round trip. On a real network the round trip is dominated by the network itself. Aggregate throughput rises with the number of concurrent clients and server threads, because `SO_REUSEPORT` lets the kernel spread datagrams across the worker sockets.

## Scaling and what is next

The server already binds one socket per worker with `SO_REUSEPORT` and shards the store, so it uses every core. The next steps for higher packet rates, in order of payoff, are `recvmmsg` and `sendmmsg` to handle many datagrams per syscall on Linux, larger socket buffers under bursty load, and request batching so several operations share a datagram. Horizontal scale beyond one box is client-side consistent hashing across several servers.

A few limits are deliberate. There is no authentication, so bind it only to a trusted network. Values above 1024 bytes are rejected rather than fragmented. Operations that are not idempotent, such as a counter, would need server-side request deduplication before they could ride the same retransmitting client.

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
