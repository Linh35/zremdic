//! The UDP server. It binds one socket per worker thread with SO_REUSEPORT, so the kernel spreads
//! incoming datagrams across threads and the store scales with cores. Each worker reads a datagram,
//! applies it to the sharded store, and replies. A successful SET also pushes the new value to every
//! address subscribed to that key.

const std = @import("std");
const builtin = @import("builtin");
const c = std.c;
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const proto = @import("proto.zig");
const store = @import("store.zig");
const Store = store.Store;
const Addr = store.Addr;
const max_subscribers = store.max_subscribers;
const Dedup = @import("dedup.zig").Dedup;

// On Linux a worker receives and replies to a whole batch of datagrams per syscall with recvmmsg and
// sendmmsg. Elsewhere it falls back to one recvfrom and one sendto per datagram.
const recv_batch = 16;

pub const Options = struct {
    ip: []const u8 = "0.0.0.0",
    port: u16 = 6380,
    threads: usize = 1,
    shards: usize = 16,
    /// Pre-size the store for roughly this many keys, so it does not rehash as it fills. Zero
    /// leaves the store to grow on demand.
    capacity: usize = 0,
    /// Total key and value budget in bytes across all shards. When the store would exceed it, the
    /// least recently used keys are evicted. Zero leaves the store unbounded.
    max_bytes: usize = 0,
    /// Lock stripes for the retransmit-dedup cache, and how long a cached reply is kept.
    dedup_stripes: usize = 256,
    dedup_ttl_ms: u64 = 5000,
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
    dedup: Dedup,
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

        var kv = try Store.init(gpa, opts.shards, opts.max_bytes);
        errdefer kv.deinit();
        if (opts.capacity > 0) try kv.reserve(opts.capacity);

        var dedup = try Dedup.init(gpa, opts.dedup_stripes, opts.dedup_ttl_ms);
        errdefer dedup.deinit();

        const stats = try gpa.alloc(WorkerStats, n);
        for (stats) |*s| s.* = .{};

        return .{
            .gpa = gpa,
            .store = kv,
            .dedup = dedup,
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
        self.dedup.deinit();
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
        comptime {
            _ = &workerBatched; // keep the Linux path type-checked when building for other targets
        }
        if (builtin.os.tag == .linux) {
            // The batched loop only errors if it cannot allocate its buffers; then fall back below.
            self.workerBatched(sock, stats) catch {};
            if (!self.running.load(.acquire)) return;
        }
        self.workerSimple(sock, stats);
    }

    // One recvfrom and one sendto per datagram. The portable path, and the fallback on Linux.
    fn workerSimple(self: *Server, sock: c.fd_t, stats: *WorkerStats) void {
        var rbuf: [proto.max_datagram]u8 = undefined;
        var sbuf: [proto.max_datagram]u8 = undefined;
        var pbuf: [proto.max_datagram]u8 = undefined;
        var subs: [max_subscribers]Addr = undefined;

        while (self.running.load(.acquire)) {
            var from: Addr = undefined;
            var flen: posix.socklen_t = @sizeOf(Addr);
            const got = c.recvfrom(sock, &rbuf, rbuf.len, 0, @ptrCast(&from), &flen);
            if (got <= 0) continue; // a receive timeout lets the loop notice a stop request
            const reply = self.handleOne(rbuf[0..@intCast(got)], &sbuf, &pbuf, &subs, from, sock, stats);
            if (reply > 0) _ = c.sendto(sock, &sbuf, reply, 0, @ptrCast(&from), flen);
        }
    }

    // Receive up to `recv_batch` datagrams per recvmmsg, build their replies, and send them with one
    // sendmmsg. Each received datagram has its own receive and send slot, so the reply for slot i is
    // assembled in place while the others are still being handled. Returns an error only if the
    // per-worker buffers cannot be allocated, leaving the caller to use the simple path.
    fn workerBatched(self: *Server, sock: c.fd_t, stats: *WorkerStats) !void {
        const gpa = self.gpa;
        const rstore = try gpa.alloc(u8, recv_batch * proto.max_datagram);
        defer gpa.free(rstore);
        const sstore = try gpa.alloc(u8, recv_batch * proto.max_datagram);
        defer gpa.free(sstore);
        const addrs = try gpa.alloc(Addr, recv_batch);
        defer gpa.free(addrs);
        const riov = try gpa.alloc(posix.iovec, recv_batch);
        defer gpa.free(riov);
        const siov = try gpa.alloc(posix.iovec, recv_batch);
        defer gpa.free(siov);
        const rmsg = try gpa.alloc(linux.mmsghdr, recv_batch);
        defer gpa.free(rmsg);
        const smsg = try gpa.alloc(linux.mmsghdr, recv_batch);
        defer gpa.free(smsg);
        var pbuf: [proto.max_datagram]u8 = undefined;
        var subs: [max_subscribers]Addr = undefined;

        // The receive vector is set up once: slot i reads into its own buffer and records its sender.
        for (riov, 0..) |*v, i| v.* = .{ .base = rstore[i * proto.max_datagram ..].ptr, .len = proto.max_datagram };
        for (rmsg, 0..) |*m, i| m.* = .{ .hdr = msghdrFor(&addrs[i], riov[i..][0..1].ptr), .len = 0 };

        while (self.running.load(.acquire)) {
            for (rmsg) |*m| m.hdr.namelen = @sizeOf(Addr); // recvmmsg overwrites this; reset each round
            var ts: linux.timespec = .{ .sec = 0, .nsec = 200 * std.time.ns_per_ms }; // wake to see a stop
            const ret = linux.recvmmsg(sock, rmsg.ptr, recv_batch, linux.MSG.WAITFORONE, &ts);
            if (linux.errno(ret) != .SUCCESS) continue;

            var nsend: usize = 0;
            var i: usize = 0;
            while (i < ret) : (i += 1) {
                const rbuf = rstore[i * proto.max_datagram ..][0..rmsg[i].len];
                const sslot = sstore[i * proto.max_datagram ..][0..proto.max_datagram];
                const reply = self.handleOne(rbuf, sslot, &pbuf, &subs, addrs[i], sock, stats);
                if (reply == 0) continue;
                siov[nsend] = .{ .base = sslot.ptr, .len = reply };
                smsg[nsend] = .{ .hdr = msghdrFor(&addrs[i], siov[nsend..][0..1].ptr), .len = 0 };
                nsend += 1;
            }
            if (nsend > 0) _ = linux.sendmmsg(sock, smsg.ptr, @intCast(nsend), 0);
        }
    }

    // Decode one request, apply it, and write any reply into `sbuf`. Returns the reply length, or 0
    // for a malformed datagram or an op that sends nothing back.
    fn handleOne(self: *Server, rbuf: []const u8, sbuf: []u8, pbuf: []u8, subs: []Addr, from: Addr, sock: c.fd_t, stats: *WorkerStats) usize {
        const req = proto.decodeRequest(rbuf) catch return 0;
        return self.apply(req, from, sbuf, pbuf, subs, sock, stats) catch 0;
    }

    // Apply one request, write its reply into `sbuf`, and return the reply length (0 means no reply).
    fn apply(self: *Server, req: proto.Request, from: Addr, sbuf: []u8, pbuf: []u8, subs: []Addr, sock: c.fd_t, stats: *WorkerStats) !usize {
        switch (req.op) {
            .ping => return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok }),
            .get => {
                // Single-copy read: the store writes the value straight after the reply header in
                // `sbuf`, then the header is stamped in front of it, with no intermediate buffer.
                WorkerStats.bump(&stats.gets, 1);
                if (self.store.get(req.key, sbuf[proto.resp_header..])) |len| {
                    WorkerStats.bump(&stats.hits, 1);
                    return try proto.finishResponse(sbuf, req.id, .ok, len);
                }
                WorkerStats.bump(&stats.misses, 1);
                return try proto.finishResponse(sbuf, req.id, .not_found, 0);
            },
            .set, .del => {
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
            .incrby, .append, .setnx, .cas, .getset, .getdel => return try self.applyMutating(req, from, sbuf, pbuf, subs, sock, stats),
            .update => return 0, // a server-to-client message; clients never send it
        }
    }

    // Apply a non-idempotent op through the dedup cache so a retransmit replays the first reply
    // instead of mutating twice. Idempotent ops never reach here, so they pay nothing for dedup.
    fn applyMutating(self: *Server, req: proto.Request, from: Addr, sbuf: []u8, pbuf: []u8, subs: []Addr, sock: c.fd_t, stats: *WorkerStats) !usize {
        var vbuf: [proto.max_value]u8 = undefined;
        switch (self.dedup.lookup(from, req.id, &vbuf)) {
            // A retransmit replays the first reply and does not push again; the change already fired.
            .hit => |r| return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = r.status, .value = r.value }),
            .miss => |ticket| {
                const res = self.runMutating(req, &vbuf, stats);
                self.dedup.commit(ticket, res.status, vbuf[0..res.len]);
                self.pushMutation(req, res, vbuf[0..res.len], pbuf, subs, sock, stats);
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = res.status, .value = vbuf[0..res.len] });
            },
        }
    }

    // Notify watchers of a key's new value after a successful mutating op, the same best-effort push
    // a set or del makes. The new value is known from the request for every op but append, which is
    // re-read, and only when the key has a subscriber so the read is skipped otherwise.
    fn pushMutation(self: *Server, req: proto.Request, res: store.OpResult, reply: []const u8, pbuf: []u8, subs: []Addr, sock: c.fd_t, stats: *WorkerStats) void {
        const changed = switch (req.op) {
            .incrby, .append => res.status == .ok,
            .getset => res.status == .ok or res.status == .not_found, // getset always writes the new value
            .getdel => res.status == .ok, // ok means the key existed and was removed
            .setnx, .cas => reply.len == 1 and reply[0] == '1', // only when it actually set or swapped
            else => false,
        };
        if (!changed) return;
        const n = self.store.subscribersOf(req.key, subs);
        if (n == 0) return;

        var vbuf: [proto.max_value]u8 = undefined;
        const value: []const u8 = switch (req.op) {
            .incrby => reply, // the new counter is the new value
            .getset, .setnx => req.value,
            .cas => if (proto.decodeCasValue(req.value)) |cv| cv.new_value else return,
            .getdel => "", // an empty value tells watchers the key is gone, as with del
            .append => if (self.store.get(req.key, &vbuf)) |k| vbuf[0..k] else "",
            else => return,
        };
        const len = proto.encodeRequest(pbuf, .{ .id = 0, .op = .update, .key = req.key, .value = value }) catch return;
        for (subs[0..n]) |*addr| {
            _ = c.sendto(sock, pbuf.ptr, len, 0, @ptrCast(addr), @sizeOf(Addr));
        }
        WorkerStats.bump(&stats.pushes, n);
    }

    // Run one non-idempotent op against the store, writing any reply value into `out`.
    fn runMutating(self: *Server, req: proto.Request, out: []u8, stats: *WorkerStats) store.OpResult {
        switch (req.op) {
            .incrby => {
                if (req.value.len != 8) return .{ .status = .bad_request };
                const delta = std.mem.readInt(i64, req.value[0..8], .little);
                WorkerStats.bump(&stats.sets, 1);
                return self.store.incrBy(req.key, delta, out);
            },
            .append => {
                WorkerStats.bump(&stats.sets, 1);
                return self.store.append(req.key, req.value, out);
            },
            .setnx => {
                if (req.value.len > proto.max_value) return .{ .status = .too_large };
                WorkerStats.bump(&stats.sets, 1);
                return self.store.setNx(req.key, req.value, req.ttl_ms, out);
            },
            .cas => {
                const c2 = proto.decodeCasValue(req.value) orelse return .{ .status = .bad_request };
                if (c2.new_value.len > proto.max_value) return .{ .status = .too_large };
                WorkerStats.bump(&stats.sets, 1);
                return self.store.cas(req.key, c2.expected, c2.new_value, req.ttl_ms, out);
            },
            .getset => {
                if (req.value.len > proto.max_value) return .{ .status = .too_large };
                WorkerStats.bump(&stats.sets, 1);
                return self.store.getSet(req.key, req.value, req.ttl_ms, out);
            },
            .getdel => {
                WorkerStats.bump(&stats.dels, 1);
                return self.store.getDel(req.key, out);
            },
            else => return .{ .status = .bad_request },
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
        s.evicted = self.store.evictedCount();
        s.keys = self.store.count();
        s.bytes = self.store.byteCount();
        s.subscribers = self.store.subscriberCount();
        s.dedup_hits = self.dedup.hitCount();
        return s;
    }
};

// Build a message header pointing at one address and a single-entry iovec, for recvmmsg or sendmmsg.
fn msghdrFor(name: *Addr, iov: [*]posix.iovec) linux.msghdr {
    return .{
        .name = @ptrCast(name),
        .namelen = @sizeOf(Addr),
        .iov = iov,
        .iovlen = 1,
        .control = null,
        .controllen = 0,
        .flags = 0,
    };
}

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
