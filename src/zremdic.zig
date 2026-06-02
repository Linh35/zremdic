//! zremdic, the zig remote dictionary: a latency-focused key-value cache served over UDP for use
//! inside a trusted network. This file is the library root; it re-exports the server, the client,
//! the store, and the wire protocol. See README.md for the design and usage.

const std = @import("std");

pub const proto = @import("proto.zig");
pub const Store = @import("store.zig").Store;
pub const Server = @import("server.zig").Server;
pub const ServerOptions = @import("server.zig").Options;
pub const Client = @import("client.zig").Client;
pub const ClientOptions = @import("client.zig").Options;
pub const Update = @import("client.zig").Update;
pub const Batch = @import("client.zig").Batch;
pub const Results = @import("client.zig").Results;
pub const Stats = proto.Stats;

test {
    _ = proto;
    _ = @import("store.zig");
}

// --- end-to-end tests ----------------------------------------------------------------------------

const testing = std.testing;

test "client and server: ping, set, get, miss, delete over UDP" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 2, .shards = 4 });
    defer server.deinit();
    try server.start();

    var client = try Client.init("127.0.0.1", server.port(), .{ .timeout_ms = 500, .retries = 5 });
    defer client.deinit();

    try client.ping();

    var out: [256]u8 = undefined;
    try testing.expect((try client.get("greeting", &out)) == null); // absent

    try client.set("greeting", "hello");
    try testing.expectEqualStrings("hello", (try client.get("greeting", &out)).?);

    try client.set("greeting", "hi again"); // overwrite
    try testing.expectEqualStrings("hi again", (try client.get("greeting", &out)).?);

    try client.del("greeting");
    try testing.expect((try client.get("greeting", &out)) == null);
}

test "many keys round-trip through a multi-threaded server" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 4, .shards = 16 });
    defer server.deinit();
    try server.start();

    var client = try Client.init("127.0.0.1", server.port(), .{ .timeout_ms = 500, .retries = 8 });
    defer client.deinit();

    var kbuf: [32]u8 = undefined;
    var vbuf: [32]u8 = undefined;
    var out: [32]u8 = undefined;
    var i: usize = 0;
    while (i < 500) : (i += 1) {
        const key = try std.fmt.bufPrint(&kbuf, "k{d}", .{i});
        const val = try std.fmt.bufPrint(&vbuf, "v{d}", .{i});
        try client.set(key, val);
    }
    i = 0;
    while (i < 500) : (i += 1) {
        const key = try std.fmt.bufPrint(&kbuf, "k{d}", .{i});
        const val = try std.fmt.bufPrint(&vbuf, "v{d}", .{i});
        try testing.expectEqualStrings(val, (try client.get(key, &out)).?);
    }
}

test "a subscriber is pushed the new value when a key changes" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 2, .shards = 4 });
    defer server.deinit();
    try server.start();
    const p = server.port();

    var sub = try Client.init("127.0.0.1", p, .{ .timeout_ms = 800, .retries = 5 });
    defer sub.deinit();
    var writer = try Client.init("127.0.0.1", p, .{ .timeout_ms = 800, .retries = 5 });
    defer writer.deinit();

    try sub.subscribe("room");
    try writer.set("room", "occupied");

    var kbuf: [64]u8 = undefined;
    var vbuf: [256]u8 = undefined;
    const upd = (try sub.pollUpdate(&kbuf, &vbuf)) orelse return error.NoUpdate;
    try testing.expectEqualStrings("room", upd.key);
    try testing.expectEqualStrings("occupied", upd.value);

    // After unsubscribing, a later change is not delivered.
    try sub.unsubscribe("room");
    try writer.set("room", "empty");
    try testing.expect((try sub.pollUpdate(&kbuf, &vbuf)) == null);
}

test "a key set with a ttl is gone once it expires" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 2, .shards = 4 });
    defer server.deinit();
    try server.start();

    var client = try Client.init("127.0.0.1", server.port(), .{ .timeout_ms = 500, .retries = 5 });
    defer client.deinit();

    var out: [64]u8 = undefined;
    try client.setEx("temp", "here", 50);
    try testing.expectEqualStrings("here", (try client.get("temp", &out)).?);
    sleepMs(120);
    try testing.expect((try client.get("temp", &out)) == null);
}

test "stats reflect the operations served" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 2, .shards = 4 });
    defer server.deinit();
    try server.start();

    var client = try Client.init("127.0.0.1", server.port(), .{ .timeout_ms = 500, .retries = 5 });
    defer client.deinit();

    var out: [64]u8 = undefined;
    try client.set("a", "1");
    try client.set("b", "2");
    _ = try client.get("a", &out); // a hit
    _ = try client.get("nope", &out); // a miss
    try client.del("a");

    const s = try client.stats();
    try testing.expect(s.sets >= 2);
    try testing.expect(s.gets >= 2);
    try testing.expect(s.hits >= 1);
    try testing.expect(s.misses >= 1);
    try testing.expect(s.dels >= 1);
    try testing.expectEqual(@as(u64, 1), s.keys); // only b remains
}

test "a batch applies its operations in order and returns their results" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 2, .shards = 4 });
    defer server.deinit();
    try server.start();

    var client = try Client.init("127.0.0.1", server.port(), .{ .timeout_ms = 500, .retries = 5 });
    defer client.deinit();

    var b = client.batch();
    try b.set("a", "1");
    try b.set("b", "2");
    try b.get("a");
    try b.get("missing");
    try b.del("a");
    try b.get("a");

    var rbuf: [proto.max_datagram]u8 = undefined;
    var results = try b.send(&rbuf);
    try testing.expectEqual(proto.Status.ok, results.next().?.status); // set a
    try testing.expectEqual(proto.Status.ok, results.next().?.status); // set b
    const ga = results.next().?;
    try testing.expectEqual(proto.Status.ok, ga.status);
    try testing.expectEqualStrings("1", ga.value); // get a
    try testing.expectEqual(proto.Status.not_found, results.next().?.status); // get missing
    try testing.expectEqual(proto.Status.ok, results.next().?.status); // del a
    try testing.expectEqual(proto.Status.not_found, results.next().?.status); // get a after del
    try testing.expect(results.next() == null);
}

test "a large value round-trips whole" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 2, .shards = 4 });
    defer server.deinit();
    try server.start();

    var client = try Client.init("127.0.0.1", server.port(), .{ .timeout_ms = 800, .retries = 8 });
    defer client.deinit();

    // 8 KiB stays under the default macOS per-datagram limit; Linux handles up to the 16 KiB cap.
    var big: [8000]u8 = undefined;
    for (&big, 0..) |*x, i| x.* = @truncate(i);
    try client.set("blob", &big);

    var out: [proto.max_value]u8 = undefined;
    const got = (try client.get("blob", &out)).?;
    try testing.expectEqualSlices(u8, &big, got);
}

test "atomic operations over the wire" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 2, .shards = 4 });
    defer server.deinit();
    try server.start();

    var client = try Client.init("127.0.0.1", server.port(), .{ .timeout_ms = 500, .retries = 5 });
    defer client.deinit();

    try testing.expectEqual(@as(i64, 1), try client.incr("hits"));
    try testing.expectEqual(@as(i64, 6), try client.incrBy("hits", 5));
    try testing.expectEqual(@as(i64, 5), try client.decr("hits"));

    try testing.expectEqual(@as(u64, 5), try client.append("name", "hello"));
    try testing.expectEqual(@as(u64, 8), try client.append("name", "!!!"));

    try testing.expect(try client.setNx("lock", "me", 0));
    try testing.expect(!try client.setNx("lock", "you", 0));
    try testing.expect(try client.cas("lock", "me", "owned", 0));
    try testing.expect(!try client.cas("lock", "me", "nope", 0));

    var out: [64]u8 = undefined;
    try testing.expectEqualStrings("owned", (try client.getSet("lock", "fresh", &out)).?);
    try testing.expectEqualStrings("fresh", (try client.getDel("lock", &out)).?);
    try testing.expect((try client.getDel("lock", &out)) == null);

    try client.set("word", "abc");
    try testing.expectError(error.NotANumber, client.incr("word"));
}

test "the server evicts under a byte budget" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 1, .shards = 1, .max_bytes = 4096 });
    defer server.deinit();
    try server.start();

    var client = try Client.init("127.0.0.1", server.port(), .{ .timeout_ms = 500, .retries = 8 });
    defer client.deinit();

    var kbuf: [32]u8 = undefined;
    const value = "0123456789" ** 6; // 60 bytes
    var i: usize = 0;
    while (i < 500) : (i += 1) {
        const key = try std.fmt.bufPrint(&kbuf, "key-{d}", .{i});
        try client.set(key, value);
    }

    const s = try client.stats();
    try testing.expect(s.bytes <= 4096); // budget held
    try testing.expect(s.evicted > 0); // and it evicted to get there
    try testing.expect(s.keys < 500); // so not everything survived
}

test "a retransmitted mutating request is applied once" {
    var server = try Server.init(testing.allocator, .{ .ip = "127.0.0.1", .port = 0, .threads = 1, .shards = 4 });
    defer server.deinit();
    try server.start();

    const c = std.c;
    const posix = std.posix;
    const fd = c.socket(posix.AF.INET, posix.SOCK.DGRAM, 0);
    try testing.expect(fd >= 0);
    defer _ = c.close(fd);
    var sa: posix.sockaddr.in = .{
        .family = posix.AF.INET,
        .port = std.mem.nativeToBig(u16, server.port()),
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
        .zero = [_]u8{0} ** 8,
    };
    try testing.expect(c.connect(fd, @ptrCast(&sa), @sizeOf(posix.sockaddr.in)) == 0);

    var delta: [8]u8 = undefined;
    std.mem.writeInt(i64, &delta, 1, .little);
    var req: [proto.max_datagram]u8 = undefined;
    var rbuf: [proto.max_datagram]u8 = undefined;

    // The same datagram (same id) sent twice stands in for a retransmit. The counter must end at 1.
    const n = try proto.encodeRequest(&req, .{ .id = 777, .op = .incrby, .key = "counter", .value = &delta });
    var attempt: usize = 0;
    while (attempt < 2) : (attempt += 1) {
        _ = c.send(fd, &req, n, 0);
        var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
        try testing.expect(c.poll(&pfd, 1, 1000) > 0);
        const got = c.recv(fd, &rbuf, rbuf.len, 0);
        try testing.expect(got > 0);
        const resp = try proto.decodeResponse(rbuf[0..@intCast(got)]);
        try testing.expectEqualStrings("1", resp.value); // both replies are 1, not 1 then 2
    }

    // A fresh id is a new logical request, so it does increment.
    const m = try proto.encodeRequest(&req, .{ .id = 778, .op = .incrby, .key = "counter", .value = &delta });
    _ = c.send(fd, &req, m, 0);
    var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
    try testing.expect(c.poll(&pfd, 1, 1000) > 0);
    const got = c.recv(fd, &rbuf, rbuf.len, 0);
    const resp = try proto.decodeResponse(rbuf[0..@intCast(got)]);
    try testing.expectEqualStrings("2", resp.value);
}

// Sleep for `ms` by polling no descriptors.
fn sleepMs(ms: c_int) void {
    var none: [0]std.posix.pollfd = .{};
    _ = std.c.poll(&none, 0, ms);
}
