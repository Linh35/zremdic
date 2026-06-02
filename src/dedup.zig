//! A small cache of recent replies, keyed by the client address and the request id, so a retransmit
//! of a non-idempotent operation replays its first reply instead of applying the change a second
//! time. Only mutating ops touch it, so reads and plain writes pay nothing. It is striped by a hash
//! of the key, each stripe behind its own spinlock, and entries fall out after a short time to live.
//!
//! The lock for a stripe is held across the operation between `lookup` returning a miss and the
//! matching `commit`. Because retransmits of one request hash to the same stripe, two of them racing
//! on different workers serialize there: the first applies and caches, the second sees the cached
//! reply. That makes the operation exactly-once regardless of how the kernel routes the datagrams.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("proto.zig");
const store = @import("store.zig");
const SpinLock = store.SpinLock;
const nowMs = store.nowMs;
const Addr = store.Addr;

// Replies up to this size are kept inline, so counters, "0"/"1" flags, and append lengths never
// allocate. Larger replies (an old value from getset or getdel) are copied to the heap.
const inline_cap = 32;

// A stripe holds at most this many live entries; past it, expired entries are swept and, if needed,
// one entry is dropped, so a flood of unique mutating ops cannot grow it without bound.
const stripe_cap = 8192;

const DedupKey = packed struct {
    addr: u32, // client address, network order
    port: u16, // client port, network order
    id: u32, // the request id the client chose
};

const Val = struct {
    created_at: u64,
    status: proto.Status,
    len: u32,
    inl: [inline_cap]u8 = undefined,
    heap: ?[]u8 = null,

    fn bytes(self: *const Val) []const u8 {
        return if (self.heap) |h| h else self.inl[0..self.len];
    }
};

const Stripe = struct {
    gpa: Allocator,
    lock: SpinLock = .{},
    map: std.AutoHashMapUnmanaged(DedupKey, Val) = .empty,
};

pub const Reply = struct {
    status: proto.Status,
    value: []const u8, // borrows the caller's output buffer, valid after the stripe lock is released
};

pub const Ticket = struct {
    stripe: *Stripe,
    key: DedupKey,
}; // returned with the stripe lock held; the caller must call commit to release it

pub const Lookup = union(enum) {
    hit: Reply, // a cached reply was copied into the caller's buffer; the lock is already released
    miss: Ticket, // no cached reply; the stripe lock is held until commit
};

pub const Dedup = struct {
    gpa: Allocator,
    stripes: []Stripe,
    mask: usize,
    ttl_ms: u64,
    hits: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn init(gpa: Allocator, stripe_count: usize, ttl_ms: u64) !Dedup {
        const n = std.math.ceilPowerOfTwo(usize, @max(1, stripe_count)) catch unreachable;
        const stripes = try gpa.alloc(Stripe, n);
        for (stripes) |*s| s.* = .{ .gpa = gpa };
        return .{ .gpa = gpa, .stripes = stripes, .mask = n - 1, .ttl_ms = ttl_ms };
    }

    pub fn deinit(self: *Dedup) void {
        for (self.stripes) |*s| {
            var it = s.map.valueIterator();
            while (it.next()) |v| if (v.heap) |h| self.gpa.free(h);
            s.map.deinit(self.gpa);
        }
        self.gpa.free(self.stripes);
    }

    pub fn hitCount(self: *Dedup) u64 {
        return self.hits.load(.monotonic);
    }

    fn stripeFor(self: *Dedup, key: DedupKey) *Stripe {
        var h: u64 = key.addr;
        h = (h *% 0x9E3779B97F4A7C15) ^ (@as(u64, key.id) *% 0xD1B54A32D192ED03) ^ key.port;
        return &self.stripes[@as(usize, @intCast(h)) & self.mask];
    }

    /// Look up the reply for `(addr, id)`. On a hit the cached reply is copied into `out` and the
    /// stripe lock is released. On a miss the stripe lock is held and a ticket is returned; the
    /// caller applies the operation and then calls `commit` with the reply, which releases the lock.
    pub fn lookup(self: *Dedup, addr: Addr, id: u32, out: []u8) Lookup {
        const key: DedupKey = .{ .addr = addr.addr, .port = addr.port, .id = id };
        const stripe = self.stripeFor(key);
        stripe.lock.lock();
        if (stripe.map.getPtr(key)) |v| {
            if (nowMs() < v.created_at + self.ttl_ms) {
                const src = v.bytes();
                const n = @min(src.len, out.len);
                @memcpy(out[0..n], src[0..n]);
                const status = v.status;
                stripe.lock.unlock();
                _ = self.hits.fetchAdd(1, .monotonic);
                return .{ .hit = .{ .status = status, .value = out[0..n] } };
            }
            if (v.heap) |h| self.gpa.free(h); // stale: drop it and treat as a miss
            _ = stripe.map.remove(key);
        }
        return .{ .miss = .{ .stripe = stripe, .key = key } };
    }

    /// Store the reply for a ticket from a missed `lookup` and release the stripe lock.
    pub fn commit(self: *Dedup, ticket: Ticket, status: proto.Status, value: []const u8) void {
        const stripe = ticket.stripe;
        defer stripe.lock.unlock();

        const gop = stripe.map.getOrPut(self.gpa, ticket.key) catch return; // out of memory: skip caching
        if (gop.found_existing) {
            if (gop.value_ptr.heap) |h| self.gpa.free(h);
        }
        var v: Val = .{ .created_at = nowMs(), .status = status, .len = @intCast(value.len) };
        if (value.len <= inline_cap) {
            @memcpy(v.inl[0..value.len], value);
        } else {
            v.heap = self.gpa.dupe(u8, value) catch {
                _ = stripe.map.remove(ticket.key); // cannot keep the reply, drop the slot
                return;
            };
        }
        gop.value_ptr.* = v;
        self.prune(stripe);
    }

    // Keep a stripe bounded: once it is over the cap, free expired entries, and if none expired drop
    // one entry so inserts keep making progress. Called under the stripe lock.
    fn prune(self: *Dedup, stripe: *Stripe) void {
        if (stripe.map.count() <= stripe_cap) return;
        const now = nowMs();
        var victims: [64]DedupKey = undefined;
        var n: usize = 0;
        var it = stripe.map.iterator();
        while (it.next()) |e| {
            if (now >= e.value_ptr.created_at + self.ttl_ms) {
                if (e.value_ptr.heap) |h| self.gpa.free(h);
                victims[n] = e.key_ptr.*;
                n += 1;
                if (n == victims.len) break;
            }
        }
        if (n == 0) {
            var it2 = stripe.map.iterator();
            if (it2.next()) |e| {
                if (e.value_ptr.heap) |h| self.gpa.free(h);
                victims[0] = e.key_ptr.*;
                n = 1;
            }
        }
        var i: usize = 0;
        while (i < n) : (i += 1) _ = stripe.map.remove(victims[i]);
    }
};

// --- tests ---------------------------------------------------------------------------------------

const testing = std.testing;

fn addrOf(a: u32, p: u16) Addr {
    return .{ .family = std.posix.AF.INET, .port = p, .addr = a, .zero = [_]u8{0} ** 8 };
}

test "a miss caches and a retransmit replays the same reply" {
    var d = try Dedup.init(testing.allocator, 8, 5000);
    defer d.deinit();

    const client = addrOf(0x0100007f, 4000);
    var out: [64]u8 = undefined;

    // First time: a miss, the caller applies and commits a reply.
    switch (d.lookup(client, 42, &out)) {
        .hit => return error.UnexpectedHit,
        .miss => |t| d.commit(t, .ok, "result-7"),
    }

    // Retransmit of the same id: a hit, replaying the stored reply, no second apply.
    switch (d.lookup(client, 42, &out)) {
        .hit => |r| {
            try testing.expectEqual(proto.Status.ok, r.status);
            try testing.expectEqualStrings("result-7", r.value);
        },
        .miss => return error.ExpectedHit,
    }
    try testing.expectEqual(@as(u64, 1), d.hitCount());

    // A different id from the same client is independent.
    switch (d.lookup(client, 43, &out)) {
        .hit => return error.UnexpectedHit,
        .miss => |t| d.commit(t, .not_found, ""),
    }
}

test "a reply larger than the inline buffer round-trips through the heap" {
    var d = try Dedup.init(testing.allocator, 4, 5000);
    defer d.deinit();

    const client = addrOf(0x0100007f, 5000);
    var big: [100]u8 = undefined;
    for (&big, 0..) |*b, i| b.* = @intCast('a' + (i % 26));

    switch (d.lookup(client, 1, big[0..0])) {
        .miss => |t| d.commit(t, .ok, &big),
        .hit => return error.UnexpectedHit,
    }
    var out: [128]u8 = undefined;
    switch (d.lookup(client, 1, &out)) {
        .hit => |r| try testing.expectEqualSlices(u8, &big, r.value),
        .miss => return error.ExpectedHit,
    }
}

test "an entry past its time to live is not replayed" {
    var d = try Dedup.init(testing.allocator, 4, 20); // 20 ms ttl
    defer d.deinit();

    const client = addrOf(0x0100007f, 6000);
    var out: [32]u8 = undefined;
    switch (d.lookup(client, 9, &out)) {
        .miss => |t| d.commit(t, .ok, "x"),
        .hit => return error.UnexpectedHit,
    }
    var none: [0]std.posix.pollfd = .{};
    _ = std.c.poll(&none, 0, 40); // wait past the ttl

    switch (d.lookup(client, 9, &out)) {
        .hit => return error.ShouldHaveExpired,
        .miss => |t| d.commit(t, .ok, "x"), // released the lock
    }
}
