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
