//! The zremdic server binary.
//!
//!   zremdic-server [port] [threads] [shards] [capacity]
//!
//! port      UDP port to bind on 0.0.0.0          (default 6380)
//! threads   worker threads, one socket each       (default: CPU count)
//! shards    store shards, the concurrency width   (default 64)
//! capacity  keys to pre-size the store for         (default 0, grow on demand)
//!
//! It binds, starts the workers, and serves until the process is stopped.

const std = @import("std");
const zremdic = @import("zremdic");

pub fn main(init: std.process.Init.Minimal) !void {
    const gpa = std.heap.smp_allocator;

    var port: u16 = 6380;
    var threads: usize = std.Thread.getCpuCount() catch 1;
    var shards: usize = 64;
    var capacity: usize = 0;

    var args = std.process.Args.Iterator.init(init.args);
    _ = args.skip(); // program name
    if (args.next()) |a| port = std.fmt.parseInt(u16, a, 10) catch port;
    if (args.next()) |a| threads = std.fmt.parseInt(usize, a, 10) catch threads;
    if (args.next()) |a| shards = std.fmt.parseInt(usize, a, 10) catch shards;
    if (args.next()) |a| capacity = std.fmt.parseInt(usize, a, 10) catch capacity;

    var server = try zremdic.Server.init(gpa, .{
        .ip = "0.0.0.0",
        .port = port,
        .threads = threads,
        .shards = shards,
        .capacity = capacity,
    });
    defer server.deinit();
    try server.start();

    std.debug.print("zremdic listening on 0.0.0.0:{d}  threads={d} shards={d} capacity={d}\n", .{ server.port(), threads, shards, capacity });
    server.join(); // serve until the process is stopped
}
