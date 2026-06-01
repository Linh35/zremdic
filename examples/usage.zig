//! A runnable tour of the client API against an in-process server. `zig build example` runs it.

const std = @import("std");
const zremdic = @import("zremdic");

pub fn main() !void {
    const gpa = std.heap.smp_allocator;

    var server = try zremdic.Server.init(gpa, .{ .ip = "127.0.0.1", .port = 0, .threads = 2, .shards = 8 });
    defer server.deinit();
    try server.start();
    const port = server.port();
    std.debug.print("server on 127.0.0.1:{d}\n", .{port});

    var client = try zremdic.Client.init("127.0.0.1", port, .{});
    defer client.deinit();

    var out: [256]u8 = undefined;

    try client.set("user:1", "ada");
    std.debug.print("get user:1   = {s}\n", .{(try client.get("user:1", &out)).?});
    std.debug.print("get missing  = {?s}\n", .{try client.get("missing", &out)});

    try client.setEx("session", "token", 1000); // expires in 1 second

    // A batch: several operations in one datagram, one round trip, results in order.
    var b = client.batch();
    try b.set("a", "1");
    try b.set("b", "2");
    try b.get("a");
    try b.get("nope");
    var rbuf: [zremdic.proto.max_datagram]u8 = undefined;
    var results = try b.send(&rbuf);
    var i: usize = 0;
    while (results.next()) |r| : (i += 1) {
        std.debug.print("batch[{d}]     = status {d}, value '{s}'\n", .{ i, @intFromEnum(r.status), r.value });
    }

    // A watcher is pushed the new value whenever the key changes. Pushes are best effort, so dedupe
    // by remembering the last value and treat a push as a hint to read the key when it matters.
    var watcher = try zremdic.Client.init("127.0.0.1", port, .{ .timeout_ms = 500 });
    defer watcher.deinit();
    try watcher.subscribe("score");
    try client.set("score", "42");
    var kbuf: [64]u8 = undefined;
    var vbuf: [64]u8 = undefined;
    if (try watcher.pollUpdate(&kbuf, &vbuf)) |u| {
        std.debug.print("watcher saw  {s} = {s}\n", .{ u.key, u.value });
    }

    const s = try client.stats();
    std.debug.print("stats        = gets {d}, sets {d}, hits {d}, misses {d}, keys {d}\n", .{ s.gets, s.sets, s.hits, s.misses, s.keys });

    try client.del("user:1");
    std.debug.print("after delete = {?s}\n", .{try client.get("user:1", &out)});
}
