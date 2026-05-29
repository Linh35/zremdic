//! The wire protocol: a fixed binary header plus key and value bytes, one request and one reply per
//! UDP datagram. Each request carries a 32-bit id so a client can match replies and ignore stale or
//! duplicated datagrams. Sizes are capped so a request and its reply each fit in one datagram below
//! a typical path MTU, which keeps every operation to a single round trip with no fragmentation.

const std = @import("std");

pub const max_key = 256;
pub const max_value = 1024;
pub const max_datagram = 1500;

pub const Op = enum(u8) {
    get = 1,
    set = 2,
    del = 3,
    ping = 4,
    subscribe = 5,
    unsubscribe = 6,
    update = 7, // server to client: a subscribed key changed
};
pub const Status = enum(u8) { ok = 0, not_found = 1, too_large = 2, bad_request = 3 };

const req_header = 11; // id(4) op(1) key_len(2) val_len(4)
const resp_header = 9; // id(4) status(1) val_len(4)

pub const Request = struct {
    id: u32,
    op: Op,
    key: []const u8 = &.{},
    value: []const u8 = &.{},
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

// --- tests ---------------------------------------------------------------------------------------

const testing = std.testing;

test "request round-trips through encode and decode" {
    var buf: [max_datagram]u8 = undefined;
    const n = try encodeRequest(&buf, .{ .id = 42, .op = .set, .key = "name", .value = "ada" });
    const r = try decodeRequest(buf[0..n]);
    try testing.expectEqual(@as(u32, 42), r.id);
    try testing.expectEqual(Op.set, r.op);
    try testing.expectEqualStrings("name", r.key);
    try testing.expectEqualStrings("ada", r.value);
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

test "oversize key and value are rejected at encode time" {
    var buf: [max_datagram]u8 = undefined;
    var big: [max_key + 1]u8 = undefined;
    try testing.expectError(error.KeyTooLong, encodeRequest(&buf, .{ .id = 1, .op = .get, .key = &big }));
}

test "a truncated datagram does not read past its end" {
    var buf: [max_datagram]u8 = undefined;
    const n = try encodeRequest(&buf, .{ .id = 1, .op = .set, .key = "k", .value = "vvvv" });
    try testing.expectError(error.Truncated, decodeRequest(buf[0 .. n - 2])); // cut off the value
    try testing.expectError(error.Truncated, decodeRequest(buf[0..3])); // shorter than the header
}

test "a bad op byte is reported, not interpreted" {
    var buf: [max_datagram]u8 = undefined;
    const n = try encodeRequest(&buf, .{ .id = 1, .op = .get, .key = "k" });
    buf[4] = 99; // not a valid Op
    try testing.expectError(error.BadOp, decodeRequest(buf[0..n]));
}
