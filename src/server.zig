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
    recv_buf_bytes: c_int = 4 << 20,
};

pub const Server = struct {
    gpa: Allocator,
    store: Store,
    sockets: []c.fd_t,
    workers: []std.Thread,
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

        return .{
            .gpa = gpa,
            .store = try Store.init(gpa, opts.shards),
            .sockets = sockets,
            .workers = try gpa.alloc(std.Thread, n),
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
        self.store.deinit();
    }

    /// The port the server is actually bound to.
    pub fn port(self: *Server) u16 {
        return std.mem.bigToNative(u16, self.addr.port);
    }

    /// Spawn the worker threads and begin serving.
    pub fn start(self: *Server) !void {
        self.running.store(true, .release);
        for (self.workers, self.sockets) |*w, sock| {
            w.* = try std.Thread.spawn(.{}, worker, .{ self, sock });
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

    fn worker(self: *Server, sock: c.fd_t) void {
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

            const reply = self.apply(req, from, &sbuf, &pbuf, &subs, sock) catch continue;
            if (reply > 0) _ = c.sendto(sock, &sbuf, reply, 0, @ptrCast(&from), flen);
        }
    }

    // Apply one request, write its reply into `sbuf`, and return the reply length (0 means no reply).
    fn apply(self: *Server, req: proto.Request, from: Addr, sbuf: []u8, pbuf: []u8, subs: []Addr, sock: c.fd_t) !usize {
        switch (req.op) {
            .ping => return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok }),
            .get => {
                var vbuf: [proto.max_value]u8 = undefined;
                if (self.store.get(req.key, &vbuf)) |len| {
                    return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok, .value = vbuf[0..len] });
                }
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .not_found });
            },
            .set => {
                if (req.key.len > proto.max_key or req.value.len > proto.max_value) {
                    return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .too_large });
                }
                self.store.set(req.key, req.value) catch {
                    return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .bad_request });
                };
                self.pushChange(req.key, req.value, pbuf, subs, sock);
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok });
            },
            .del => {
                _ = self.store.del(req.key);
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok });
            },
            .subscribe => {
                self.store.subscribe(req.key, from) catch {};
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok });
            },
            .unsubscribe => {
                self.store.unsubscribe(req.key, from);
                return try proto.encodeResponse(sbuf, .{ .id = req.id, .status = .ok });
            },
            .update => return 0, // a server-to-client message; clients never send it
        }
    }

    // Push the new value of `key` to its subscribers as an `update` datagram.
    fn pushChange(self: *Server, key: []const u8, value: []const u8, pbuf: []u8, subs: []Addr, sock: c.fd_t) void {
        const n = self.store.subscribersOf(key, subs);
        if (n == 0) return;
        const len = proto.encodeRequest(pbuf, .{ .id = 0, .op = .update, .key = key, .value = value }) catch return;
        for (subs[0..n]) |*addr| {
            _ = c.sendto(sock, pbuf.ptr, len, 0, @ptrCast(addr), @sizeOf(Addr));
        }
    }
};

fn bindSocket(addr: *const Addr, recv_buf_bytes: c_int) !c.fd_t {
    const fd = c.socket(posix.AF.INET, posix.SOCK.DGRAM, 0);
    if (fd < 0) return error.SocketFailed;
    errdefer _ = c.close(fd);

    setOpt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, @as(c_int, 1));
    setOpt(fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, @as(c_int, 1));
    setOpt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, recv_buf_bytes);
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
