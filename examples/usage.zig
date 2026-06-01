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

    // A watcher is pushed the new value whenever the key changes.
    var watcher = try zremdic.Client.init("127.0.0.1", port, .{ .timeout_ms = 500 });
    defer watcher.deinit();
    try watcher.subscribe("score");

    try client.set("score", "42");
    var kbuf: [64]u8 = undefined;
    var vbuf: [64]u8 = undefined;
    if (try watcher.pollUpdate(&kbuf, &vbuf)) |u| {
        std.debug.print("watcher saw  {s} = {s}\n", .{ u.key, u.value });
    }

    try client.del("user:1");
    std.debug.print("after delete = {?s}\n", .{try client.get("user:1", &out)});
}
