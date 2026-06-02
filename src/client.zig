//! The client. Each call sends one request datagram and waits, using poll, for the reply that
//! carries the same id. If the reply does not arrive within the current window, it retransmits with
//! an exponentially growing, jittered backoff, so a busy server is not buried under retries. GET,
//! SET, and DEL are idempotent, so a retransmit is harmless. `subscribe` asks the server to push
//! later changes to this socket, which `pollUpdate` reads.

const std = @import("std");
const c = std.c;
const posix = std.posix;
const proto = @import("proto.zig");

pub const Options = struct {
    timeout_ms: u32 = 200, // initial wait before the first retransmit
    retries: u32 = 3,
    backoff_cap_ms: u32 = 2000, // the wait never grows past this
};

pub const Update = struct {
    key: []const u8,
    value: []const u8,
};

pub const Client = struct {
    fd: c.fd_t,
    server: posix.sockaddr.in,
    next_id: u32 = 0,
    timeout_ms: u32,
    retries: u32,
    backoff_cap_ms: u32,
    prng: std.Random.DefaultPrng,

    pub fn init(ip: []const u8, port: u16, opts: Options) !Client {
        const fd = c.socket(posix.AF.INET, posix.SOCK.DGRAM, 0);
        if (fd < 0) return error.SocketFailed;
        errdefer _ = c.close(fd);
        // Roomy socket buffers so a large value fits in one datagram (on platforms that allow it).
        const bufsz: c_int = 1 << 20;
        _ = c.setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, @ptrCast(&bufsz), @sizeOf(c_int));
        _ = c.setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, @ptrCast(&bufsz), @sizeOf(c_int));

        const server: posix.sockaddr.in = .{
            .family = posix.AF.INET,
            .port = std.mem.nativeToBig(u16, port),
            .addr = std.mem.nativeToBig(u32, try parseIp4(ip)),
            .zero = [_]u8{0} ** 8,
        };
        // Connect the socket to the one server it talks to. Each call then uses send and recv with no
        // per-call address, saving the kernel a route lookup, and stray datagrams from other hosts are
        // dropped before they reach us.
        if (c.connect(fd, @ptrCast(&server), @sizeOf(posix.sockaddr.in)) != 0) return error.ConnectFailed;

        return .{
            .fd = fd,
            .server = server,
            .timeout_ms = @max(1, opts.timeout_ms),
            .retries = @max(1, opts.retries),
            .backoff_cap_ms = @max(opts.timeout_ms, opts.backoff_cap_ms),
            .prng = std.Random.DefaultPrng.init(nowMs() ^ @as(u64, @intCast(fd))),
        };
    }

    pub fn deinit(self: *Client) void {
        _ = c.close(self.fd);
    }

    /// PING the server. Errors if no reply arrives within the retry budget.
    pub fn ping(self: *Client) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.ping, "", "", 0, &buf);
        if (r.status != .ok) return error.ServerError;
    }

    /// Store `value` under `key` with no expiry.
    pub fn set(self: *Client, key: []const u8, value: []const u8) !void {
        return self.setEx(key, value, 0);
    }

    /// Store `value` under `key`, expiring it `ttl_ms` milliseconds from now (0 means no expiry).
    pub fn setEx(self: *Client, key: []const u8, value: []const u8, ttl_ms: u32) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.set, key, value, ttl_ms, &buf);
        switch (r.status) {
            .ok => {},
            .too_large => return error.TooLarge,
            else => return error.ServerError,
        }
    }

    /// Read `key` into `out`, returning the value slice, or null if the key is absent or expired.
    pub fn get(self: *Client, key: []const u8, out: []u8) !?[]u8 {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.get, key, "", 0, &buf);
        switch (r.status) {
            .ok => {
                const n = @min(r.value.len, out.len);
                @memcpy(out[0..n], r.value[0..n]);
                return out[0..n];
            },
            .not_found => return null,
            else => return error.ServerError,
        }
    }

    /// Delete `key`.
    pub fn del(self: *Client, key: []const u8) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.del, key, "", 0, &buf);
        if (r.status != .ok) return error.ServerError;
    }

    /// Ask the server to push future changes of `key` to this client.
    pub fn subscribe(self: *Client, key: []const u8) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.subscribe, key, "", 0, &buf);
        if (r.status != .ok) return error.ServerError;
    }

    /// Stop receiving pushed changes of `key`.
    pub fn unsubscribe(self: *Client, key: []const u8) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.unsubscribe, key, "", 0, &buf);
        if (r.status != .ok) return error.ServerError;
    }

    /// Fetch the server's counters.
    pub fn stats(self: *Client) !proto.Stats {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.stats, "", "", 0, &buf);
        if (r.status != .ok) return error.ServerError;
        return proto.decodeStats(r.value) orelse error.ServerError;
    }

    /// Add `delta` to the integer value of `key` (absent is treated as 0) and return the new value.
    /// Errors `error.NotANumber` if the current value is not a valid integer.
    pub fn incrBy(self: *Client, key: []const u8, delta: i64) !i64 {
        var d: [8]u8 = undefined;
        std.mem.writeInt(i64, &d, delta, .little);
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.incrby, key, &d, 0, &buf);
        if (r.status != .ok) return error.NotANumber;
        return std.fmt.parseInt(i64, r.value, 10) catch error.ServerError;
    }

    /// Add 1 to `key` and return the new value.
    pub fn incr(self: *Client, key: []const u8) !i64 {
        return self.incrBy(key, 1);
    }

    /// Subtract 1 from `key` and return the new value.
    pub fn decr(self: *Client, key: []const u8) !i64 {
        return self.incrBy(key, -1);
    }

    /// Subtract `delta` from `key` and return the new value.
    pub fn decrBy(self: *Client, key: []const u8, delta: i64) !i64 {
        return self.incrBy(key, -delta);
    }

    /// Append `suffix` to `key` (absent is treated as empty) and return the new length.
    pub fn append(self: *Client, key: []const u8, suffix: []const u8) !u64 {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.append, key, suffix, 0, &buf);
        switch (r.status) {
            .ok => return std.fmt.parseInt(u64, r.value, 10) catch error.ServerError,
            .too_large => return error.TooLarge,
            else => return error.ServerError,
        }
    }

    /// Set `key` only if it is absent. Returns true if it was set, false if it already existed.
    pub fn setNx(self: *Client, key: []const u8, value: []const u8, ttl_ms: u32) !bool {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.setnx, key, value, ttl_ms, &buf);
        switch (r.status) {
            .ok => return r.value.len == 1 and r.value[0] == '1',
            .too_large => return error.TooLarge,
            else => return error.ServerError,
        }
    }

    /// Set `key` to `new_value` only if its current value equals `expected`. Returns whether it was
    /// swapped. Useful for optimistic locking and, with a TTL, for leases and leader election.
    pub fn cas(self: *Client, key: []const u8, expected: []const u8, new_value: []const u8, ttl_ms: u32) !bool {
        var vbuf: [proto.max_value]u8 = undefined;
        const n = proto.encodeCasValue(&vbuf, expected, new_value) catch return error.TooLarge;
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.cas, key, vbuf[0..n], ttl_ms, &buf);
        switch (r.status) {
            .ok => return r.value.len == 1 and r.value[0] == '1',
            .too_large => return error.TooLarge,
            else => return error.ServerError,
        }
    }

    /// Set `key` to `new_value` and return its previous value into `out`, or null if it was absent.
    pub fn getSet(self: *Client, key: []const u8, new_value: []const u8, out: []u8) !?[]u8 {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.getset, key, new_value, 0, &buf);
        switch (r.status) {
            .ok => {
                const n = @min(r.value.len, out.len);
                @memcpy(out[0..n], r.value[0..n]);
                return out[0..n];
            },
            .not_found => return null,
            .too_large => return error.TooLarge,
            else => return error.ServerError,
        }
    }

    /// Remove `key` and return its previous value into `out`, or null if it was absent.
    pub fn getDel(self: *Client, key: []const u8, out: []u8) !?[]u8 {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.getdel, key, "", 0, &buf);
        switch (r.status) {
            .ok => {
                const n = @min(r.value.len, out.len);
                @memcpy(out[0..n], r.value[0..n]);
                return out[0..n];
            },
            .not_found => return null,
            else => return error.ServerError,
        }
    }

    /// Begin a batch: queue several operations, then `send` them in one datagram and one round trip.
    pub fn batch(self: *Client) Batch {
        return .{ .client = self };
    }

    /// Wait up to one timeout for a pushed change, copying the key and value into the given buffers.
    /// Returns null if none arrived. A pushed change with an empty value means the key was deleted.
    /// Pushed changes are best effort: a dropped datagram is not retransmitted.
    pub fn pollUpdate(self: *Client, key_out: []u8, val_out: []u8) !?Update {
        var buf: [proto.max_datagram]u8 = undefined;
        const deadline = nowMs() + self.timeout_ms;
        while (true) {
            const remaining = deadline -| nowMs();
            if (remaining == 0 or !self.readable(@intCast(remaining))) return null;
            const got = c.recv(self.fd, &buf, buf.len, 0);
            if (got <= 0) return null;
            const msg = proto.decodeRequest(buf[0..@intCast(got)]) catch continue;
            if (msg.op != .update) continue; // a stray reply, not a change push
            const kn = @min(msg.key.len, key_out.len);
            const vn = @min(msg.value.len, val_out.len);
            @memcpy(key_out[0..kn], msg.key[0..kn]);
            @memcpy(val_out[0..vn], msg.value[0..vn]);
            return .{ .key = key_out[0..kn], .value = val_out[0..vn] };
        }
    }

    fn newId(self: *Client) u32 {
        self.next_id +%= 1;
        return self.next_id;
    }

    // Block up to `ms` for the socket to become readable. Returns false on timeout or error.
    fn readable(self: *Client, ms: c_int) bool {
        var pfd = [_]posix.pollfd{.{ .fd = self.fd, .events = posix.POLL.IN, .revents = 0 }};
        return c.poll(&pfd, 1, ms) > 0;
    }

    // The wait before the next retransmit: the timeout doubled per attempt, capped, plus jitter.
    fn backoffMs(self: *Client, attempt: u32) u32 {
        var w: u64 = self.timeout_ms;
        var i: u32 = 0;
        while (i < attempt and w < self.backoff_cap_ms) : (i += 1) w *= 2;
        if (w > self.backoff_cap_ms) w = self.backoff_cap_ms;
        const jitter = self.prng.random().uintLessThan(u64, w / 2 + 1);
        return @intCast(w + jitter);
    }

    fn roundtrip(self: *Client, op: proto.Op, key: []const u8, value: []const u8, ttl_ms: u32, resp_buf: []u8) !proto.Response {
        const id = self.newId();
        var req: [proto.max_datagram]u8 = undefined;
        const reqlen = try proto.encodeRequest(&req, .{ .id = id, .op = op, .key = key, .value = value, .ttl_ms = ttl_ms });

        var attempt: u32 = 0;
        while (attempt < self.retries) : (attempt += 1) {
            _ = c.send(self.fd, &req, reqlen, 0);
            const deadline = nowMs() + self.backoffMs(attempt);
            while (true) {
                const remaining = deadline -| nowMs();
                if (remaining == 0 or !self.readable(@intCast(remaining))) break; // retransmit
                const got = c.recv(self.fd, resp_buf.ptr, resp_buf.len, 0);
                if (got <= 0) break;
                const r = proto.decodeResponse(resp_buf[0..@intCast(got)]) catch continue;
                if (r.id == id) return r; // a different id is a stale reply or a push: keep waiting
            }
        }
        return error.Timeout;
    }
};

/// A queue of operations sent together in one datagram. The total queued size must fit one value
/// (16 KiB), as must the combined results, so batches are for many small operations.
pub const Batch = struct {
    client: *Client,
    body: [proto.max_value]u8 = undefined,
    len: usize = 0,

    pub fn set(self: *Batch, key: []const u8, value: []const u8) !void {
        try self.add(.set, key, value, 0);
    }
    pub fn setEx(self: *Batch, key: []const u8, value: []const u8, ttl_ms: u32) !void {
        try self.add(.set, key, value, ttl_ms);
    }
    pub fn get(self: *Batch, key: []const u8) !void {
        try self.add(.get, key, "", 0);
    }
    pub fn del(self: *Batch, key: []const u8) !void {
        try self.add(.del, key, "", 0);
    }

    fn add(self: *Batch, op: proto.Op, key: []const u8, value: []const u8, ttl_ms: u32) !void {
        self.len = try proto.appendSubRequest(&self.body, self.len, .{ .op = op, .key = key, .value = value, .ttl_ms = ttl_ms });
    }

    /// Send the queued operations and return an iterator over their results, in queue order. The
    /// results reference `resp_buf`, which must outlive the iteration.
    pub fn send(self: *Batch, resp_buf: []u8) !Results {
        const r = try self.client.roundtrip(.batch, "", self.body[0..self.len], 0, resp_buf);
        if (r.status != .ok) return error.ServerError;
        return .{ .reader = .{ .buf = r.value } };
    }
};

/// An iterator over a batch's results. Each `next` is one `SubResult`, in the order operations were
/// queued. The `value` of a get result is the stored value, or empty for a miss.
pub const Results = struct {
    reader: proto.SubResultReader,

    pub fn next(self: *Results) ?proto.SubResult {
        return self.reader.next();
    }
};

fn nowMs() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1000 + @as(u64, @intCast(ts.nsec)) / 1_000_000;
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
