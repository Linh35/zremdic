//! A loopback throughput benchmark: an in-process server plus several client threads, each doing a
//! stream of SET and GET round trips. It reports aggregate operations per second.

const std = @import("std");
const zremdic = @import("zremdic");

const server_threads = 4;
const shards = 32;
const client_threads = 4;
const ops_per_client = 100_000;
const payload = "value-payload-0123456789abcdef";

fn clientWork(port: u16, done: *std.atomic.Value(usize)) void {
    var client = zremdic.Client.init("127.0.0.1", port, .{ .timeout_ms = 500, .retries = 5 }) catch return;
    defer client.deinit();

    var kbuf: [32]u8 = undefined;
    var out: [64]u8 = undefined;
    var ok: usize = 0;
    var i: usize = 0;
    while (i < ops_per_client) : (i += 1) {
        const key = std.fmt.bufPrint(&kbuf, "k{d}", .{i % 4096}) catch unreachable;
        if (client.set(key, payload)) |_| ok += 1 else |_| {}
        if (client.get(key, &out)) |_| ok += 1 else |_| {}
    }
    _ = done.fetchAdd(ok, .monotonic);
}

pub fn main() !void {
    const gpa = std.heap.smp_allocator;

    var server = try zremdic.Server.init(gpa, .{ .ip = "127.0.0.1", .port = 0, .threads = server_threads, .shards = shards });
    defer server.deinit();
    try server.start();
    const port = server.port();

    var done = std.atomic.Value(usize).init(0);
    var threads: [client_threads]std.Thread = undefined;

    const start = nowNs();
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, clientWork, .{ port, &done });
    for (&threads) |t| t.join();
    const ns = nowNs() - start;

    const total = done.load(.monotonic);
    const secs = @as(f64, @floatFromInt(ns)) / 1e9;
    // Each client runs its ops serially, so wall time over ops-per-client is the round-trip latency.
    const rtt_us = secs / @as(f64, @floatFromInt(ops_per_client * 2)) * 1e6;
    std.debug.print("zremdic loopback benchmark\n", .{});
    std.debug.print("  {d} round trips in {d:.2}s  ({d} client threads, {d} server threads, {d} shards)\n", .{ total, secs, client_threads, server_threads, shards });
    std.debug.print("  {d:.0} ops/sec, {d:.1} us average round trip\n", .{ @as(f64, @floatFromInt(total)) / secs, rtt_us });
}

fn nowNs() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}
