//! The UDP server. It binds one socket per worker thread with SO_REUSEPORT, so the kernel spreads
//! incoming datagrams across threads and the store scales with cores. Each worker reads a datagram,
//! applies it to the sharded store, and replies. A successful SET also pushes the new value to every
//! address subscribed to that key.

const std = @import("std");
const c = std.c;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const proto = @import("proto.zig");
const Store = @import("store.zig").Store;
const Addr = @import("store.zig").Addr;

pub const Options = struct {
    ip: []const u8 = "0.0.0.0",
    port: u16 = 6380,
    threads: usize = 1,
    shards: usize = 16,
    /// Pre-size the store for roughly this many keys, so it does not rehash as it fills. Zero
    /// leaves the store to grow on demand.
    capacity: usize = 0,
    recv_buf_bytes: c_int = 4 << 20,
};

// Per-worker counters. Each worker is the only writer of its own counters, so the increments are
// uncontended and the only sharing is the relaxed read a STATS request does to sum them.
const WorkerStats = struct {
    gets: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    sets: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    dels: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    hits: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    misses: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    pushes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    fn bump(field: *std.atomic.Value(u64), by: u64) void {
        if (by != 0) _ = field.fetchAdd(by, .monotonic);
    }
};

pub const Server = struct {
    gpa: Allocator,
    store: Store,
    sockets: []c.fd_t,
    workers: []std.Thread,
    worker_stats: []WorkerStats,
    addr: Addr,
    running: std.atomic.Value(bool),
    started: bool = false,

    pub fn init(gpa: Allocator, opts: Options) !Server {
        var addr: Addr = .{
            .family = posix.AF.INET,
            .port = std.mem.nativeToBig(u16, opts.port),
            .addr = std.mem.nativeToBig(u32, try parseIp4(opts.ip)),
            .zero = [_]u8{0} ** 8,
        };

        const n = @max(1, opts.threads);
        const sockets = try gpa.alloc(c.fd_t, n);
        errdefer gpa.free(sockets);

        // Bind the first socket to learn the actual port (relevant when the caller asked for 0),
        // then bind the rest to that resolved address so SO_REUSEPORT shares one port.
        sockets[0] = try bindSocket(&addr, opts.recv_buf_bytes);
        var len: posix.socklen_t = @sizeOf(Addr);
        _ = c.getsockname(sockets[0], @ptrCast(&addr), &len);
        for (sockets[1..]) |*s| s.* = try bindSocket(&addr, opts.recv_buf_bytes);

        var store = try Store.init(gpa, opts.shards);
        errdefer store.deinit();
        if (opts.capacity > 0) try store.reserve(opts.capacity);

        const stats = try gpa.alloc(WorkerStats, n);
        for (stats) |*s| s.* = .{};

        return .{
            .gpa = gpa,
            .store = store,
            .sockets = sockets,
            .workers = try gpa.alloc(std.Thread, n),
            .worker_stats = stats,
            .addr = addr,
            .running = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *Server) void {
        self.stop();
        self.join();
        for (self.sockets) |s| _ = c.close(s);
        self.gpa.free(self.sockets);
        self.gpa.free(self.workers);
        self.gpa.free(self.worker_stats);
        self.store.deinit();
    }

    /// The port the server is actually bound to.
    pub fn port(self: *Server) u16 {
        return std.mem.bigToNative(u16, self.addr.port);
    }

    /// Spawn the worker threads and begin serving.
    pub fn start(self: *Server) !void {
        self.running.store(true, .release);
        for (self.workers, self.sockets, self.worker_stats) |*w, sock, *stats| {
            w.* = try std.Thread.spawn(.{}, worker, .{ self, sock, stats });
        }
        self.started = true;
    }

    /// Ask the workers to stop after their current receive returns.
    pub fn stop(self: *Server) void {
        self.running.store(false, .release);
    }

    /// Block until all workers have exited.
    pub fn join(self: *Server) void {
        if (!self.started) return;
        self.started = false;
        for (self.workers) |w| w.join();
    }

    fn worker(self: *Server, sock: c.fd_t, stats: *WorkerStats) void {
        var rbuf: [proto.max_datagram]u8 = undefined;
        var sbuf: [proto.max_datagram]u8 = undefined;
        var pbuf: [proto.max_datagram]u8 = undefined;
        var subs: [@import("store.zig").max_subscribers]Addr = undefined;

        while (self.running.load(.acquire)) {
            var from: Addr = undefined;
            var flen: posix.socklen_t = @sizeOf(Addr);
            const got = c.recvfrom(sock, &rbuf, rbuf.len, 0, @ptrCast(&from), &flen);
            if (got <= 0) continue; // a receive timeout lets the loop notice a stop request
            const req = proto.decodeRequest(rbuf[0..@intCast(got)]) catch continue;

            const reply = self.apply(req, from, &sbuf, &pbuf, &subs, sock, stats) catch continue;
            if (reply > 0) _ = c.sendto(sock, &sbuf, reply, 0, @ptrCast(&from), flen);
        }
    }

    // Apply one request, write its reply into `sbuf`, and return the reply length (0 means no reply).
    fn apply(self: *Server, req: proto.Request, from: Addr, sbuf: []u8, pbuf: []u8, subs: []Addr, sock: c.fd_t, stats: *WorkerStats) !usize {
        switch (req.op) {
            .ping => return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok }),
            .get, .set, .del => {
                var vbuf: [proto.max_value]u8 = undefined;
                const res = self.applySub(.{ .op = req.op, .key = req.key, .value = req.value, .ttl_ms = req.ttl_ms }, &vbuf, pbuf, subs, sock, stats);
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = res.status, .value = res.value });
            },
            .subscribe => {
                self.store.subscribe(req.key, from) catch {};
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok });
            },
            .unsubscribe => {
                self.store.unsubscribe(req.key, from);
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok });
            },
            .stats => {
                var sbytes: [proto.stats_size]u8 = undefined;
                proto.encodeStats(&sbytes, self.snapshotStats());
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok, .value = &sbytes });
            },
            .batch => {
                var body: [proto.max_value]u8 = undefined;
                var vbuf: [proto.max_value]u8 = undefined;
                var pos: usize = 0;
                var reader = proto.SubRequestReader{ .buf = req.value };
                while (reader.next()) |sub| {
                    const res = self.applySub(sub, &vbuf, pbuf, subs, sock, stats);
                    pos = proto.appendSubResult(&body, pos, res) catch break; // result body is full
                }
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok, .value = body[0..pos] });
            },
            .update => return 0, // a server-to-client message; clients never send it
        }
    }

    // Apply one get, set, or del, writing any returned value into `vbuf`. Used for both single
    // requests and the entries of a batch. Counts the operation and pushes changes to subscribers.
    fn applySub(self: *Server, sub: proto.SubRequest, vbuf: []u8, pbuf: []u8, subs: []Addr, sock: c.fd_t, stats: *WorkerStats) proto.SubResult {
        switch (sub.op) {
            .get => {
                WorkerStats.bump(&stats.gets, 1);
                if (self.store.get(sub.key, vbuf)) |len| {
                    WorkerStats.bump(&stats.hits, 1);
                    return .{ .status = .ok, .value = vbuf[0..len] };
                }
                WorkerStats.bump(&stats.misses, 1);
                return .{ .status = .not_found };
            },
            .set => {
                if (sub.key.len > proto.max_key or sub.value.len > proto.max_value) return .{ .status = .too_large };
                self.store.set(sub.key, sub.value, sub.ttl_ms) catch return .{ .status = .bad_request };
                WorkerStats.bump(&stats.sets, 1);
                WorkerStats.bump(&stats.pushes, self.pushChange(sub.key, sub.value, pbuf, subs, sock));
                return .{ .status = .ok };
            },
            .del => {
                WorkerStats.bump(&stats.dels, 1);
                if (self.store.del(sub.key)) {
                    // tell watchers the key is gone: an update with an empty value
                    WorkerStats.bump(&stats.pushes, self.pushChange(sub.key, "", pbuf, subs, sock));
                }
                return .{ .status = .ok };
            },
            else => return .{ .status = .bad_request }, // only get, set, and del are valid in a batch
        }
    }

    // Push `value` for `key` to its subscribers as an `update` datagram. Returns the number sent.
    fn pushChange(self: *Server, key: []const u8, value: []const u8, pbuf: []u8, subs: []Addr, sock: c.fd_t) u64 {
        const n = self.store.subscribersOf(key, subs);
        if (n == 0) return 0;
        const len = proto.encodeRequest(pbuf, .{ .id = 0, .op = .update, .key = key, .value = value }) catch return 0;
        for (subs[0..n]) |*addr| {
            _ = c.sendto(sock, pbuf.ptr, len, 0, @ptrCast(addr), @sizeOf(Addr));
        }
        return n;
    }

    // Sum the workers' counters and read the live key and subscriber counts from the store.
    fn snapshotStats(self: *Server) proto.Stats {
        var s: proto.Stats = .{};
        for (self.worker_stats) |*w| {
            s.gets += w.gets.load(.monotonic);
            s.sets += w.sets.load(.monotonic);
            s.dels += w.dels.load(.monotonic);
            s.hits += w.hits.load(.monotonic);
            s.misses += w.misses.load(.monotonic);
            s.pushes += w.pushes.load(.monotonic);
        }
        s.expired = self.store.expiredCount();
        s.keys = self.store.count();
        s.subscribers = self.store.subscriberCount();
        return s;
    }
};

fn bindSocket(addr: *const Addr, recv_buf_bytes: c_int) !c.fd_t {
    const fd = c.socket(posix.AF.INET, posix.SOCK.DGRAM, 0);
    if (fd < 0) return error.SocketFailed;
    errdefer _ = c.close(fd);

    setOpt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, @as(c_int, 1));
    setOpt(fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, @as(c_int, 1));
    setOpt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, recv_buf_bytes);
    setOpt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, recv_buf_bytes); // also lets large replies leave
    // A receive timeout so a worker blocked in recvfrom wakes up to see a stop request.
    setOpt(fd, posix.SOL.SOCKET, posix.SO.RCVTIMEO, posix.timeval{ .sec = 0, .usec = 200 * 1000 });

    if (c.bind(fd, @ptrCast(addr), @sizeOf(Addr)) != 0) return error.BindFailed;
    return fd;
}

fn setOpt(fd: c.fd_t, level: i32, name: u32, value: anytype) void {
    _ = c.setsockopt(fd, level, name, @ptrCast(&value), @sizeOf(@TypeOf(value)));
}

fn parseIp4(s: []const u8) !u32 {
    var octets: [4]u8 = undefined;
    var it = std.mem.splitScalar(u8, s, '.');
    var i: usize = 0;
    while (it.next()) |part| : (i += 1) {
        if (i >= 4) return error.BadAddress;
        octets[i] = std.fmt.parseInt(u8, part, 10) catch return error.BadAddress;
    }
    if (i != 4) return error.BadAddress;
    return (@as(u32, octets[0]) << 24) | (@as(u32, octets[1]) << 16) | (@as(u32, octets[2]) << 8) | octets[3];
}
