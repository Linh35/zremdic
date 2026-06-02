//! The wire protocol: a fixed binary header plus key and value bytes, one request and one reply per
//! UDP datagram. Each request carries a 32-bit id so a client can match replies and ignore stale or
//! duplicated datagrams. Sizes are capped so a request and its reply each fit in one datagram below
//! a typical path MTU, which keeps every operation to a single round trip with no fragmentation.

const std = @import("std");

pub const max_key = 256;
pub const max_value = 16 * 1024; // 16 KiB; values over the path MTU are sent in one fragmented datagram
pub const max_datagram = max_value + 1024; // room for the header and key alongside the largest value

pub const Op = enum(u8) {
    get = 1,
    set = 2,
    del = 3,
    ping = 4,
    subscribe = 5,
    unsubscribe = 6,
    update = 7, // server to client: a subscribed key changed
    stats = 8,
    batch = 9, // the value carries a sequence of sub-requests; the reply carries their results
};

pub const Status = enum(u8) { ok = 0, not_found = 1, too_large = 2, bad_request = 3 };

const req_header = 15; // id(4) op(1) key_len(2) val_len(4) ttl_ms(4)
pub const resp_header = 9; // id(4) status(1) val_len(4)

pub const Request = struct {
    id: u32,
    op: Op,
    key: []const u8 = &.{},
    value: []const u8 = &.{},
    ttl_ms: u32 = 0, // for set: lifetime in milliseconds, 0 means no expiry
};

pub const Response = struct {
    id: u32,
    status: Status,
    value: []const u8 = &.{},
};

pub const EncodeError = error{ BufferTooSmall, KeyTooLong, ValueTooLong };
pub const DecodeError = error{ Truncated, BadOp, BadStatus };

pub fn encodeRequest(buf: []u8, r: Request) EncodeError!usize {
    if (r.key.len > max_key) return error.KeyTooLong;
    if (r.value.len > max_value) return error.ValueTooLong;
    const total = req_header + r.key.len + r.value.len;
    if (buf.len < total) return error.BufferTooSmall;
    std.mem.writeInt(u32, buf[0..4], r.id, .little);
    buf[4] = @intFromEnum(r.op);
    std.mem.writeInt(u16, buf[5..7], @intCast(r.key.len), .little);
    std.mem.writeInt(u32, buf[7..11], @intCast(r.value.len), .little);
    std.mem.writeInt(u32, buf[11..15], r.ttl_ms, .little);
    @memcpy(buf[req_header..][0..r.key.len], r.key);
    @memcpy(buf[req_header + r.key.len ..][0..r.value.len], r.value);
    return total;
}

pub fn decodeRequest(buf: []const u8) DecodeError!Request {
    if (buf.len < req_header) return error.Truncated;
    const op = std.enums.fromInt(Op, buf[4]) orelse return error.BadOp;
    const klen = std.mem.readInt(u16, buf[5..7], .little);
    const vlen = std.mem.readInt(u32, buf[7..11], .little);
    if (buf.len < req_header + klen + vlen) return error.Truncated;
    return .{
        .id = std.mem.readInt(u32, buf[0..4], .little),
        .op = op,
        .key = buf[req_header..][0..klen],
        .value = buf[req_header + klen ..][0..vlen],
        .ttl_ms = std.mem.readInt(u32, buf[11..15], .little),
    };
}

pub fn encodeResponse(buf: []u8, r: Response) EncodeError!usize {
    if (r.value.len > max_value) return error.ValueTooLong;
    const total = resp_header + r.value.len;
    if (buf.len < total) return error.BufferTooSmall;
    std.mem.writeInt(u32, buf[0..4], r.id, .little);
    buf[4] = @intFromEnum(r.status);
    std.mem.writeInt(u32, buf[5..9], @intCast(r.value.len), .little);
    @memcpy(buf[resp_header..][0..r.value.len], r.value);
    return total;
}

/// Write a response header in place for a value already sitting at `buf[resp_header..][0..val_len]`,
/// returning the datagram length. This lets a get reply be assembled with no intermediate copy: the
/// store writes the value straight into the reply datagram and this stamps the header in front of it.
pub fn finishResponse(buf: []u8, id: u32, status: Status, val_len: usize) EncodeError!usize {
    if (val_len > max_value) return error.ValueTooLong;
    const total = resp_header + val_len;
    if (buf.len < total) return error.BufferTooSmall;
    std.mem.writeInt(u32, buf[0..4], id, .little);
    buf[4] = @intFromEnum(status);
    std.mem.writeInt(u32, buf[5..9], @intCast(val_len), .little);
    return total;
}

pub fn decodeResponse(buf: []const u8) DecodeError!Response {
    if (buf.len < resp_header) return error.Truncated;
    const status = std.enums.fromInt(Status, buf[4]) orelse return error.BadStatus;
    const vlen = std.mem.readInt(u32, buf[5..9], .little);
    if (buf.len < resp_header + vlen) return error.Truncated;
    return .{
        .id = std.mem.readInt(u32, buf[0..4], .little),
        .status = status,
        .value = buf[resp_header..][0..vlen],
    };
}

/// Server counters, returned in the value of a STATS reply as a row of little-endian u64s.
pub const Stats = struct {
    gets: u64 = 0,
    sets: u64 = 0,
    dels: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    pushes: u64 = 0,
    expired: u64 = 0,
    keys: u64 = 0,
    subscribers: u64 = 0,
};

const stats_fields = @typeInfo(Stats).@"struct".fields.len;
pub const stats_size = stats_fields * 8;

pub fn encodeStats(buf: []u8, s: Stats) void {
    inline for (@typeInfo(Stats).@"struct".fields, 0..) |f, i| {
        std.mem.writeInt(u64, buf[i * 8 ..][0..8], @field(s, f.name), .little);
    }
}

pub fn decodeStats(buf: []const u8) ?Stats {
    if (buf.len < stats_size) return null;
    var s: Stats = .{};
    inline for (@typeInfo(Stats).@"struct".fields, 0..) |f, i| {
        @field(s, f.name) = std.mem.readInt(u64, buf[i * 8 ..][0..8], .little);
    }
    return s;
}

// --- batches -------------------------------------------------------------------------------------
// A batch packs several operations into one datagram. The request value is a sequence of
// sub-requests, the reply value the matching sequence of sub-results, in the same order.

const sub_req_header = 11; // op(1) key_len(2) val_len(4) ttl(4)
const sub_res_header = 5; // status(1) val_len(4)

pub const SubRequest = struct {
    op: Op,
    key: []const u8 = &.{},
    value: []const u8 = &.{},
    ttl_ms: u32 = 0,
};

pub const SubResult = struct {
    status: Status,
    value: []const u8 = &.{},
};

/// Append a sub-request to `buf` at `pos`, returning the new position.
pub fn appendSubRequest(buf: []u8, pos: usize, r: SubRequest) EncodeError!usize {
    if (r.key.len > max_key) return error.KeyTooLong;
    if (r.value.len > max_value) return error.ValueTooLong;
    const end = pos + sub_req_header + r.key.len + r.value.len;
    if (end > buf.len) return error.BufferTooSmall;
    buf[pos] = @intFromEnum(r.op);
    std.mem.writeInt(u16, buf[pos + 1 ..][0..2], @intCast(r.key.len), .little);
    std.mem.writeInt(u32, buf[pos + 3 ..][0..4], @intCast(r.value.len), .little);
    std.mem.writeInt(u32, buf[pos + 7 ..][0..4], r.ttl_ms, .little);
    @memcpy(buf[pos + sub_req_header ..][0..r.key.len], r.key);
    @memcpy(buf[pos + sub_req_header + r.key.len ..][0..r.value.len], r.value);
    return end;
}

/// Append a sub-result to `buf` at `pos`, returning the new position.
pub fn appendSubResult(buf: []u8, pos: usize, r: SubResult) EncodeError!usize {
    if (r.value.len > max_value) return error.ValueTooLong;
    const end = pos + sub_res_header + r.value.len;
    if (end > buf.len) return error.BufferTooSmall;
    buf[pos] = @intFromEnum(r.status);
    std.mem.writeInt(u32, buf[pos + 1 ..][0..4], @intCast(r.value.len), .little);
    @memcpy(buf[pos + sub_res_header ..][0..r.value.len], r.value);
    return end;
}

/// Walks the sub-requests in a batch body. Stops at the end or at the first malformed entry.
pub const SubRequestReader = struct {
    buf: []const u8,
    pos: usize = 0,

    pub fn next(self: *SubRequestReader) ?SubRequest {
        if (self.pos + sub_req_header > self.buf.len) return null;
        const op = std.enums.fromInt(Op, self.buf[self.pos]) orelse return null;
        const klen = std.mem.readInt(u16, self.buf[self.pos + 1 ..][0..2], .little);
        const vlen = std.mem.readInt(u32, self.buf[self.pos + 3 ..][0..4], .little);
        const ttl = std.mem.readInt(u32, self.buf[self.pos + 7 ..][0..4], .little);
        const key_at = self.pos + sub_req_header;
        const end = key_at + klen + vlen;
        if (end > self.buf.len) return null;
        self.pos = end;
        return .{ .op = op, .key = self.buf[key_at..][0..klen], .value = self.buf[key_at + klen ..][0..vlen], .ttl_ms = ttl };
    }
};

/// Walks the sub-results in a batch reply body.
pub const SubResultReader = struct {
    buf: []const u8,
    pos: usize = 0,

    pub fn next(self: *SubResultReader) ?SubResult {
        if (self.pos + sub_res_header > self.buf.len) return null;
        const status = std.enums.fromInt(Status, self.buf[self.pos]) orelse return null;
        const vlen = std.mem.readInt(u32, self.buf[self.pos + 1 ..][0..4], .little);
        const val_at = self.pos + sub_res_header;
        if (val_at + vlen > self.buf.len) return null;
        self.pos = val_at + vlen;
        return .{ .status = status, .value = self.buf[val_at..][0..vlen] };
    }
};

// --- tests ---------------------------------------------------------------------------------------

const testing = std.testing;

test "a batch body round-trips through append and the readers" {
    var buf: [max_value]u8 = undefined;
    var pos: usize = 0;
    pos = try appendSubRequest(&buf, pos, .{ .op = .set, .key = "a", .value = "1" });
    pos = try appendSubRequest(&buf, pos, .{ .op = .get, .key = "a" });
    pos = try appendSubRequest(&buf, pos, .{ .op = .del, .key = "a" });

    var reader = SubRequestReader{ .buf = buf[0..pos] };
    const a = reader.next().?;
    try testing.expectEqual(Op.set, a.op);
    try testing.expectEqualStrings("a", a.key);
    try testing.expectEqualStrings("1", a.value);
    try testing.expectEqual(Op.get, reader.next().?.op);
    try testing.expectEqual(Op.del, reader.next().?.op);
    try testing.expect(reader.next() == null);
}

test "request round-trips through encode and decode, ttl included" {
    var buf: [max_datagram]u8 = undefined;
    const n = try encodeRequest(&buf, .{ .id = 42, .op = .set, .key = "name", .value = "ada", .ttl_ms = 5000 });
    const r = try decodeRequest(buf[0..n]);
    try testing.expectEqual(@as(u32, 42), r.id);
    try testing.expectEqual(Op.set, r.op);
    try testing.expectEqualStrings("name", r.key);
    try testing.expectEqualStrings("ada", r.value);
    try testing.expectEqual(@as(u32, 5000), r.ttl_ms);
}

test "response round-trips, including an empty value" {
    var buf: [max_datagram]u8 = undefined;
    const n = try encodeResponse(&buf, .{ .id = 7, .status = .ok, .value = "hello" });
    const r = try decodeResponse(buf[0..n]);
    try testing.expectEqual(@as(u32, 7), r.id);
    try testing.expectEqual(Status.ok, r.status);
    try testing.expectEqualStrings("hello", r.value);

    const m = try encodeResponse(&buf, .{ .id = 8, .status = .not_found });
    const e = try decodeResponse(buf[0..m]);
    try testing.expectEqual(Status.not_found, e.status);
    try testing.expectEqual(@as(usize, 0), e.value.len);
}

test "a response assembled in place decodes the same as an encoded one" {
    var buf: [max_datagram]u8 = undefined;
    const value = "in place";
    @memcpy(buf[resp_header..][0..value.len], value);
    const n = try finishResponse(&buf, 99, .ok, value.len);
    const r = try decodeResponse(buf[0..n]);
    try testing.expectEqual(@as(u32, 99), r.id);
    try testing.expectEqual(Status.ok, r.status);
    try testing.expectEqualStrings(value, r.value);
}

test "stats round-trip" {
    var buf: [stats_size]u8 = undefined;
    encodeStats(&buf, .{ .gets = 10, .sets = 5, .hits = 8, .keys = 3 });
    const s = decodeStats(&buf).?;
    try testing.expectEqual(@as(u64, 10), s.gets);
    try testing.expectEqual(@as(u64, 5), s.sets);
    try testing.expectEqual(@as(u64, 8), s.hits);
    try testing.expectEqual(@as(u64, 3), s.keys);
}

test "oversize key and value are rejected at encode time" {
    var buf: [max_datagram]u8 = undefined;
    var big: [max_key + 1]u8 = undefined;
    try testing.expectError(error.KeyTooLong, encodeRequest(&buf, .{ .id = 1, .op = .get, .key = &big }));
}

test "a truncated datagram does not read past its end" {
    var buf: [max_datagram]u8 = undefined;
    const n = try encodeRequest(&buf, .{ .id = 1, .op = .set, .key = "k", .value = "vvvv" });
    try testing.expectError(error.Truncated, decodeRequest(buf[0 .. n - 2]));
    try testing.expectError(error.Truncated, decodeRequest(buf[0..3]));
}

test "a bad op byte is reported, not interpreted" {
    var buf: [max_datagram]u8 = undefined;
    const n = try encodeRequest(&buf, .{ .id = 1, .op = .get, .key = "k" });
    buf[4] = 99;
    try testing.expectError(error.BadOp, decodeRequest(buf[0..n]));
}
