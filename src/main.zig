//! The zremdic server binary. It binds 0.0.0.0:6380 with one worker thread per CPU, then serves
//! until the process is stopped.

const std = @import("std");
const zremdic = @import("zremdic");

const port = 6380;
const shards = 64;

pub fn main() !void {
    const gpa = std.heap.smp_allocator;
    const threads = std.Thread.getCpuCount() catch 1;

    var server = try zremdic.Server.init(gpa, .{ .ip = "0.0.0.0", .port = port, .threads = threads, .shards = shards });
    defer server.deinit();
    try server.start();

    std.debug.print("zremdic listening on 0.0.0.0:{d}  threads={d} shards={d}\n", .{ server.port(), threads, shards });
    server.join(); // serve until the process is stopped
}
