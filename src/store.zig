//! The in-memory dictionary. The keyspace is split into shards by key hash, each shard a hash map
//! behind its own mutex, so worker threads contend only when they touch the same shard. Values are
//! copied out under the lock, so a reader never sees memory a concurrent writer is freeing.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// A subscriber's address: the UDP endpoint the server pushes change notifications to.
pub const Addr = std.posix.sockaddr.in;

/// Most subscribers tracked per key. Extra subscriptions to a full key are dropped.
pub const max_subscribers = 32;

fn addrEql(a: Addr, b: Addr) bool {
    return a.port == b.port and a.addr == b.addr;
}

// A stored value and when it expires. `expires_at` is a monotonic millisecond deadline, or 0 for a
// value that never expires.
const Entry = struct {
    bytes: []u8,
    expires_at: u64,
};

fn nowMs() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1000 + @as(u64, @intCast(ts.nsec)) / 1_000_000;
}

// Sleep for `ms` by polling no descriptors. Used only by the tests.
fn sleepMs(ms: c_int) void {
    var none: [0]std.posix.pollfd = .{};
    _ = std.c.poll(&none, 0, ms);
}

// A test-and-test-and-set spinlock. Shard critical sections are a map lookup plus a small memcpy,
// so spinning beats a futex's syscall, and contention is already low because the keyspace is sharded.
const SpinLock = struct {
    held: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn lock(self: *SpinLock) void {
        while (self.held.swap(true, .acquire)) {
            while (self.held.load(.monotonic)) std.atomic.spinLoopHint();
        }
    }
    fn unlock(self: *SpinLock) void {
        self.held.store(false, .release);
    }
};

pub const Store = struct {
    gpa: Allocator,
    shards: []Shard,

    pub fn init(gpa: Allocator, shard_count: usize) !Store {
        const shards = try gpa.alloc(Shard, @max(1, shard_count));
        for (shards) |*s| s.* = .{ .gpa = gpa };
        return .{ .gpa = gpa, .shards = shards };
    }

    pub fn deinit(self: *Store) void {
        for (self.shards) |*s| s.deinit();
        self.gpa.free(self.shards);
    }

    fn shardFor(self: *Store, key: []const u8) *Shard {
        return &self.shards[std.hash.Wyhash.hash(0, key) % self.shards.len];
    }

    /// Copy the value for `key` into `out` and return its length, or null if the key is absent.
    pub fn get(self: *Store, key: []const u8, out: []u8) ?usize {
        return self.shardFor(key).get(key, out);
    }

    /// Insert or overwrite `key` with a private copy of `value`. A non-zero `ttl_ms` makes the key
    /// expire that many milliseconds from now.
    pub fn set(self: *Store, key: []const u8, value: []const u8, ttl_ms: u32) !void {
        return self.shardFor(key).set(key, value, ttl_ms);
    }

    /// Remove `key`. Returns whether it was present.
    pub fn del(self: *Store, key: []const u8) bool {
        return self.shardFor(key).del(key);
    }

    /// Total number of keys across all shards.
    pub fn count(self: *Store) usize {
        var total: usize = 0;
        for (self.shards) |*s| {
            s.mutex.lock();
            total += s.map.count();
            s.mutex.unlock();
        }
        return total;
    }

    /// Pre-size every shard's table for roughly `total` keys spread evenly, so the store does not
    /// rehash as it fills toward that size.
    pub fn reserve(self: *Store, total: usize) !void {
        const per: u32 = @intCast(total / self.shards.len + 1);
        for (self.shards) |*s| {
            s.mutex.lock();
            defer s.mutex.unlock();
            try s.map.ensureTotalCapacity(s.gpa, per);
        }
    }

    /// Register `addr` to be notified when `key` changes. Idempotent per address; drops the
    /// registration if the key already has `max_subscribers`.
    pub fn subscribe(self: *Store, key: []const u8, addr: Addr) !void {
        return self.shardFor(key).subscribe(key, addr);
    }

    /// Stop notifying `addr` about `key`.
    pub fn unsubscribe(self: *Store, key: []const u8, addr: Addr) void {
        self.shardFor(key).unsubscribe(key, addr);
    }

    /// Copy the addresses subscribed to `key` into `out` and return how many there are.
    pub fn subscribersOf(self: *Store, key: []const u8, out: []Addr) usize {
        return self.shardFor(key).subscribersOf(key, out);
    }

    /// Total number of keys lazily dropped because they had expired.
    pub fn expiredCount(self: *Store) usize {
        var total: usize = 0;
        for (self.shards) |*s| {
            s.mutex.lock();
            total += s.expired;
            s.mutex.unlock();
        }
        return total;
    }

    /// Total number of subscriptions across all keys and shards.
    pub fn subscriberCount(self: *Store) usize {
        var total: usize = 0;
        for (self.shards) |*s| {
            s.mutex.lock();
            var it = s.subs.valueIterator();
            while (it.next()) |list| total += list.n;
            s.mutex.unlock();
        }
        return total;
    }
};

const SubList = struct {
    addrs: [max_subscribers]Addr = undefined,
    n: usize = 0,
};

const Shard = struct {
    gpa: Allocator,
    mutex: SpinLock = .{},
    map: std.StringHashMapUnmanaged(Entry) = .empty,
    subs: std.StringHashMapUnmanaged(SubList) = .empty,
    expired: usize = 0,

    fn deinit(self: *Shard) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            self.gpa.free(e.value_ptr.bytes);
        }
        self.map.deinit(self.gpa);
        var sit = self.subs.keyIterator();
        while (sit.next()) |k| self.gpa.free(k.*);
        self.subs.deinit(self.gpa);
    }

    fn subscribe(self: *Shard, key: []const u8, addr: Addr) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const gop = try self.subs.getOrPut(self.gpa, key);
        if (!gop.found_existing) {
            gop.key_ptr.* = self.gpa.dupe(u8, key) catch |err| {
                self.subs.removeByPtr(gop.key_ptr);
                return err;
            };
            gop.value_ptr.* = .{};
        }
        const list = gop.value_ptr;
        for (list.addrs[0..list.n]) |a| if (addrEql(a, addr)) return; // already subscribed
        if (list.n < max_subscribers) {
            list.addrs[list.n] = addr;
            list.n += 1;
        }
    }

    fn unsubscribe(self: *Shard, key: []const u8, addr: Addr) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const list = self.subs.getPtr(key) orelse return;
        var i: usize = 0;
        while (i < list.n) : (i += 1) {
            if (addrEql(list.addrs[i], addr)) {
                list.addrs[i] = list.addrs[list.n - 1];
                list.n -= 1;
                return;
            }
        }
    }

    fn subscribersOf(self: *Shard, key: []const u8, out: []Addr) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        const list = self.subs.get(key) orelse return 0;
        const n = @min(list.n, out.len);
        @memcpy(out[0..n], list.addrs[0..n]);
        return n;
    }

    fn get(self: *Shard, key: []const u8, out: []u8) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        const e = self.map.getEntry(key) orelse return null;
        const v = e.value_ptr;
        if (v.expires_at != 0 and nowMs() >= v.expires_at) {
            // expired: drop it now so it does not linger
            self.gpa.free(e.key_ptr.*);
            self.gpa.free(v.bytes);
            self.map.removeByPtr(e.key_ptr);
            self.expired += 1;
            return null;
        }
        const n = @min(v.bytes.len, out.len);
        @memcpy(out[0..n], v.bytes[0..n]);
        return n;
    }

    fn set(self: *Shard, key: []const u8, value: []const u8, ttl_ms: u32) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const expires_at: u64 = if (ttl_ms != 0) nowMs() + ttl_ms else 0;
        const gop = try self.map.getOrPut(self.gpa, key);
        if (gop.found_existing) {
            const nv = try self.gpa.realloc(gop.value_ptr.bytes, value.len);
            @memcpy(nv, value);
            gop.value_ptr.* = .{ .bytes = nv, .expires_at = expires_at };
        } else {
            // getOrPut stored the borrowed `key`; replace it with a copy the shard owns.
            const owned_key = self.gpa.dupe(u8, key) catch |err| {
                self.map.removeByPtr(gop.key_ptr);
                return err;
            };
            gop.key_ptr.* = owned_key;
            const bytes = self.gpa.dupe(u8, value) catch |err| {
                _ = self.map.remove(owned_key);
                self.gpa.free(owned_key);
                return err;
            };
            gop.value_ptr.* = .{ .bytes = bytes, .expires_at = expires_at };
        }
    }

    fn del(self: *Shard, key: []const u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.map.fetchRemove(key)) |kv| {
            self.gpa.free(kv.key);
            self.gpa.free(kv.value.bytes);
            return true;
        }
        return false;
    }
};

// --- tests ---------------------------------------------------------------------------------------

const testing = std.testing;

test "set, get, overwrite, delete" {
    var s = try Store.init(testing.allocator, 8);
    defer s.deinit();

    var out: [64]u8 = undefined;
    try testing.expect(s.get("k", &out) == null);

    try s.set("k", "first", 0);
    try testing.expectEqualStrings("first", out[0..s.get("k", &out).?]);

    try s.set("k", "second-longer", 0);
    try testing.expectEqualStrings("second-longer", out[0..s.get("k", &out).?]);
    try testing.expectEqual(@as(usize, 1), s.count());

    try testing.expect(s.del("k"));
    try testing.expect(!s.del("k"));
    try testing.expect(s.get("k", &out) == null);
}

test "many keys land in their shards and all read back" {
    var s = try Store.init(testing.allocator, 16);
    defer s.deinit();

    var buf: [32]u8 = undefined;
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        try s.set(key, key, 0);
    }
    try testing.expectEqual(@as(usize, 1000), s.count());

    var out: [32]u8 = undefined;
    i = 0;
    while (i < 1000) : (i += 1) {
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        try testing.expectEqualStrings(key, out[0..s.get(key, &out).?]);
    }
}

test "a key with a ttl expires and is gone on the next read" {
    var s = try Store.init(testing.allocator, 4);
    defer s.deinit();

    var out: [32]u8 = undefined;
    try s.set("temp", "here", 30); // expires in 30 ms
    try testing.expectEqualStrings("here", out[0..s.get("temp", &out).?]);

    sleepMs(60);
    try testing.expect(s.get("temp", &out) == null); // expired
    try testing.expectEqual(@as(usize, 0), s.count()); // and dropped from the shard

    try s.set("perm", "stays", 0); // no ttl
    sleepMs(20);
    try testing.expectEqualStrings("stays", out[0..s.get("perm", &out).?]);
}

test "subscribers register once, list back, and unsubscribe" {
    var s = try Store.init(testing.allocator, 4);
    defer s.deinit();

    const a: Addr = .{ .family = std.posix.AF.INET, .port = 1, .addr = 100, .zero = [_]u8{0} ** 8 };
    const b: Addr = .{ .family = std.posix.AF.INET, .port = 2, .addr = 200, .zero = [_]u8{0} ** 8 };

    try s.subscribe("k", a);
    try s.subscribe("k", a); // idempotent
    try s.subscribe("k", b);

    var out: [8]Addr = undefined;
    try testing.expectEqual(@as(usize, 2), s.subscribersOf("k", &out));
    try testing.expectEqual(@as(usize, 0), s.subscribersOf("other", &out));

    s.unsubscribe("k", a);
    try testing.expectEqual(@as(usize, 1), s.subscribersOf("k", &out));
    try testing.expect(addrEql(out[0], b));
}

test "concurrent writers and readers across threads stay consistent" {
    var s = try Store.init(testing.allocator, 32);
    defer s.deinit();

    const Worker = struct {
        fn run(store: *Store, base: usize) void {
            var buf: [32]u8 = undefined;
            var out: [32]u8 = undefined;
            var i: usize = 0;
            while (i < 2000) : (i += 1) {
                const key = std.fmt.bufPrint(&buf, "k-{d}-{d}", .{ base, i }) catch unreachable;
                store.set(key, key, 0) catch unreachable;
                _ = store.get(key, &out);
            }
        }
    };

    var threads: [4]std.Thread = undefined;
    for (&threads, 0..) |*t, b| t.* = try std.Thread.spawn(.{}, Worker.run, .{ &s, b });
    for (&threads) |t| t.join();

    try testing.expectEqual(@as(usize, 4 * 2000), s.count());
}
