//! The client. Each call sends one request datagram and waits for the reply that carries the same
//! id, retransmitting if the socket receive times out. GET, SET, and DEL are idempotent, so a
//! retransmit is harmless. `subscribe` asks the server to push later changes to this socket, which
//! `pollUpdate` reads.

const std = @import("std");
const c = std.c;
const posix = std.posix;
const proto = @import("proto.zig");

pub const Options = struct {
    timeout_ms: u32 = 200,
    retries: u32 = 3,
};

pub const Update = struct {
    key: []const u8,
    value: []const u8,
};

pub const Client = struct {
    fd: c.fd_t,
    server: posix.sockaddr.in,
    next_id: u32 = 0,
    retries: u32,

    pub fn init(ip: []const u8, port: u16, opts: Options) !Client {
        const fd = c.socket(posix.AF.INET, posix.SOCK.DGRAM, 0);
        if (fd < 0) return error.SocketFailed;
        errdefer _ = c.close(fd);

        var tv = posix.timeval{
            .sec = @intCast(opts.timeout_ms / 1000),
            .usec = @intCast((opts.timeout_ms % 1000) * 1000),
        };
        _ = c.setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVTIMEO, @ptrCast(&tv), @sizeOf(posix.timeval));

        return .{
            .fd = fd,
            .server = .{
                .family = posix.AF.INET,
                .port = std.mem.nativeToBig(u16, port),
                .addr = std.mem.nativeToBig(u32, try parseIp4(ip)),
                .zero = [_]u8{0} ** 8,
            },
            .retries = @max(1, opts.retries),
        };
    }

    pub fn deinit(self: *Client) void {
        _ = c.close(self.fd);
    }

    /// PING the server. Errors if no reply arrives within the retry budget.
    pub fn ping(self: *Client) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.ping, "", "", &buf);
        if (r.status != .ok) return error.ServerError;
    }

    /// Store `value` under `key`.
    pub fn set(self: *Client, key: []const u8, value: []const u8) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.set, key, value, &buf);
        switch (r.status) {
            .ok => {},
            .too_large => return error.TooLarge,
            else => return error.ServerError,
        }
    }

    /// Read `key` into `out`, returning the value slice, or null if the key is absent.
    pub fn get(self: *Client, key: []const u8, out: []u8) !?[]u8 {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.get, key, "", &buf);
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
        const r = try self.roundtrip(.del, key, "", &buf);
        if (r.status != .ok) return error.ServerError;
    }

    /// Ask the server to push future changes of `key` to this client.
    pub fn subscribe(self: *Client, key: []const u8) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.subscribe, key, "", &buf);
        if (r.status != .ok) return error.ServerError;
    }

    /// Stop receiving pushed changes of `key`.
    pub fn unsubscribe(self: *Client, key: []const u8) !void {
        var buf: [proto.max_datagram]u8 = undefined;
        const r = try self.roundtrip(.unsubscribe, key, "", &buf);
        if (r.status != .ok) return error.ServerError;
    }

    /// Wait up to one receive timeout for a pushed change, copying the key and value into the given
    /// buffers. Returns null if none arrived. Pushed changes are best effort: a dropped datagram is
    /// not retransmitted.
    pub fn pollUpdate(self: *Client, key_out: []u8, val_out: []u8) !?Update {
        var buf: [proto.max_datagram]u8 = undefined;
        while (true) {
            const got = c.recvfrom(self.fd, &buf, buf.len, 0, null, null);
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

    fn roundtrip(self: *Client, op: proto.Op, key: []const u8, value: []const u8, resp_buf: []u8) !proto.Response {
        const id = self.newId();
        var req: [proto.max_datagram]u8 = undefined;
        const reqlen = try proto.encodeRequest(&req, .{ .id = id, .op = op, .key = key, .value = value });

        var attempt: u32 = 0;
        while (attempt < self.retries) : (attempt += 1) {
            _ = c.sendto(self.fd, &req, reqlen, 0, @ptrCast(&self.server), @sizeOf(posix.sockaddr.in));
            while (true) {
                const got = c.recvfrom(self.fd, resp_buf.ptr, resp_buf.len, 0, null, null);
                if (got <= 0) break; // receive timed out: retransmit
                const r = proto.decodeResponse(resp_buf[0..@intCast(got)]) catch continue;
                if (r.id == id) return r; // a different id is a stale reply or a push: keep waiting
            }
        }
        return error.Timeout;
    }
};

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
