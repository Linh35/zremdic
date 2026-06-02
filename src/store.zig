//! The in-memory dictionary. The keyspace is split into shards by key hash, each shard a hash map
//! behind its own mutex, so worker threads contend only when they touch the same shard. Values are
//! copied out under the lock, so a reader never sees memory a concurrent writer is freeing. Each
//! shard holds a byte budget and evicts its least recently used keys when it would overflow.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("proto.zig");

/// A subscriber's address: the UDP endpoint the server pushes change notifications to.
pub const Addr = std.posix.sockaddr.in;

/// Most subscribers tracked per key. Extra subscriptions to a full key are dropped.
pub const max_subscribers = 32;

/// How many keys an eviction samples before dropping the least recently used of them. A small sample
/// approximates true LRU closely while staying cheap, the way Redis does it.
const evict_sample = 5;

fn addrEql(a: Addr, b: Addr) bool {
    return a.port == b.port and a.addr == b.addr;
}

// A stored value with its expiry, last-access tick, and index in the shard's key list. `expires_at`
// is a monotonic millisecond deadline, or 0 for a value that never expires. `last_access` is the
// shard's logical clock at the most recent touch, used to pick eviction victims.
const Entry = struct {
    bytes: []u8,
    expires_at: u64,
    last_access: u64,
    slot: usize,
};

/// The status and length of a value an operation wrote into a caller buffer.
pub const OpResult = struct {
    status: proto.Status,
    len: usize = 0,
};

pub fn nowMs() u64 {
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
pub const SpinLock = struct {
    held: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn lock(self: *SpinLock) void {
        while (self.held.swap(true, .acquire)) {
            while (self.held.load(.monotonic)) std.atomic.spinLoopHint();
        }
    }
    pub fn unlock(self: *SpinLock) void {
        self.held.store(false, .release);
    }
};

pub const Store = struct {
    gpa: Allocator,
    shards: []Shard,
    mask: usize, // shards.len is a power of two, so a key maps with one bit-and rather than a modulo

    /// Build a store with `shard_count` shards (rounded up to a power of two). `max_bytes` is the
    /// total key and value budget across all shards; 0 leaves the store unbounded.
    pub fn init(gpa: Allocator, shard_count: usize, max_bytes: usize) !Store {
        const n = std.math.ceilPowerOfTwo(usize, @max(1, shard_count)) catch unreachable;
        const per: usize = if (max_bytes == 0) 0 else @max(1, max_bytes / n);
        const shards = try gpa.alloc(Shard, n);
        for (shards, 0..) |*s, i| s.* = .{
            .gpa = gpa,
            .max_bytes = per,
            .prng = std.Random.DefaultPrng.init(nowMs() ^ i),
        };
        return .{ .gpa = gpa, .shards = shards, .mask = n - 1 };
    }

    pub fn deinit(self: *Store) void {
        for (self.shards) |*s| s.deinit();
        self.gpa.free(self.shards);
    }

    fn shardFor(self: *Store, key: []const u8) *Shard {
        return &self.shards[std.hash.Wyhash.hash(0, key) & self.mask];
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

    /// Add `delta` to the integer value of `key` (absent means 0), store the result, and write it as
    /// decimal into `out`. The key's expiry is preserved. A non-integer value yields `bad_request`.
    pub fn incrBy(self: *Store, key: []const u8, delta: i64, out: []u8) OpResult {
        return self.shardFor(key).incrBy(key, delta, out);
    }

    /// Append `suffix` to `key` (absent means empty) and write the new length as decimal into `out`.
    pub fn append(self: *Store, key: []const u8, suffix: []const u8, out: []u8) OpResult {
        return self.shardFor(key).append(key, suffix, out);
    }

    /// Set `key` only if it is absent. Writes "1" into `out` if set, "0" if it already existed.
    pub fn setNx(self: *Store, key: []const u8, value: []const u8, ttl_ms: u32, out: []u8) OpResult {
        return self.shardFor(key).setNx(key, value, ttl_ms, out);
    }

    /// Set `key` to `new_value` only if its current value equals `expected`. Writes "1" if swapped,
    /// "0" otherwise (including when the key is absent).
    pub fn cas(self: *Store, key: []const u8, expected: []const u8, new_value: []const u8, ttl_ms: u32, out: []u8) OpResult {
        return self.shardFor(key).cas(key, expected, new_value, ttl_ms, out);
    }

    /// Set `key` to `new_value` and write the previous value into `out`. The status is `not_found`
    /// with a zero length when the key was absent.
    pub fn getSet(self: *Store, key: []const u8, new_value: []const u8, ttl_ms: u32, out: []u8) OpResult {
        return self.shardFor(key).getSet(key, new_value, ttl_ms, out);
    }

    /// Remove `key` and write its previous value into `out`, or `not_found` if it was absent.
    pub fn getDel(self: *Store, key: []const u8, out: []u8) OpResult {
        return self.shardFor(key).getDel(key, out);
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

    /// Total resident key and value bytes across all shards.
    pub fn byteCount(self: *Store) usize {
        var total: usize = 0;
        for (self.shards) |*s| {
            s.mutex.lock();
            total += s.bytes;
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
            try s.keys.ensureTotalCapacity(s.gpa, per);
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

    /// Total number of keys dropped to stay under the byte budget.
    pub fn evictedCount(self: *Store) usize {
        var total: usize = 0;
        for (self.shards) |*s| {
            s.mutex.lock();
            total += s.evicted;
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
    keys: std.ArrayListUnmanaged([]const u8) = .empty, // the live keys, for sampling eviction victims
    subs: std.StringHashMapUnmanaged(SubList) = .empty,
    bytes: usize = 0, // resident key and value bytes in this shard
    max_bytes: usize = 0, // this shard's slice of the cache budget; 0 means unbounded
    clock: u64 = 0, // a logical clock; each access stamps an entry so eviction can find the oldest
    expired: usize = 0,
    evicted: usize = 0,
    prng: std.Random.DefaultPrng = std.Random.DefaultPrng.init(0),

    fn deinit(self: *Shard) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            self.gpa.free(e.value_ptr.bytes);
        }
        self.map.deinit(self.gpa);
        self.keys.deinit(self.gpa);
        var sit = self.subs.keyIterator();
        while (sit.next()) |k| self.gpa.free(k.*);
        self.subs.deinit(self.gpa);
    }

    fn tick(self: *Shard) u64 {
        self.clock += 1;
        return self.clock;
    }

    fn expiryFrom(ttl_ms: u32) u64 {
        return if (ttl_ms != 0) nowMs() + ttl_ms else 0;
    }

    // Return the entry for `key` if it is present and unexpired. An expired entry is dropped here, so
    // a stale key never lingers or feeds a read. The caller must hold the lock.
    fn live(self: *Shard, key: []const u8) ?*Entry {
        const e = self.map.getEntry(key) orelse return null;
        const v = e.value_ptr;
        if (v.expires_at != 0 and nowMs() >= v.expires_at) {
            self.removeAt(v.slot);
            self.expired += 1;
            return null;
        }
        return v;
    }

    // Insert or overwrite `key` with a copy of `value`. When the key exists, `keep_ttl` preserves its
    // expiry rather than setting one from `ttl_ms`. Maintains the key list, byte count, and clock,
    // then evicts down to the budget. The caller must hold the lock.
    fn put(self: *Shard, key: []const u8, value: []const u8, ttl_ms: u32, keep_ttl: bool) !void {
        const gop = try self.map.getOrPut(self.gpa, key);
        if (gop.found_existing) {
            const old_len = gop.value_ptr.bytes.len;
            const nv = try self.gpa.realloc(gop.value_ptr.bytes, value.len);
            @memcpy(nv, value);
            gop.value_ptr.bytes = nv;
            gop.value_ptr.expires_at = if (keep_ttl) gop.value_ptr.expires_at else expiryFrom(ttl_ms);
            gop.value_ptr.last_access = self.tick();
            self.bytes = self.bytes - old_len + value.len;
        } else {
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
            self.keys.append(self.gpa, owned_key) catch |err| {
                _ = self.map.remove(owned_key);
                self.gpa.free(owned_key);
                self.gpa.free(bytes);
                return err;
            };
            gop.value_ptr.* = .{
                .bytes = bytes,
                .expires_at = expiryFrom(ttl_ms),
                .last_access = self.tick(),
                .slot = self.keys.items.len - 1,
            };
            self.bytes += owned_key.len + value.len;
        }
        self.evictIfNeeded(key);
    }

    // Drop the entry at key-list index `idx`, freeing its key and value and fixing up the key list.
    fn removeAt(self: *Shard, idx: usize) void {
        const key = self.keys.items[idx];
        const kv = self.map.fetchRemove(key).?;
        self.bytes -= kv.key.len + kv.value.bytes.len;
        self.gpa.free(kv.value.bytes);
        _ = self.keys.swapRemove(idx);
        self.gpa.free(kv.key); // the same allocation as `key`
        if (idx < self.keys.items.len) {
            // swapRemove moved the last key into idx; point its entry at the new slot
            if (self.map.getPtr(self.keys.items[idx])) |moved| moved.slot = idx;
        }
    }

    // While over the byte budget, sample a few keys and drop the least recently used, never the key
    // just written (`protect`). The caller must hold the lock.
    fn evictIfNeeded(self: *Shard, protect: []const u8) void {
        if (self.max_bytes == 0) return;
        while (self.bytes > self.max_bytes and self.keys.items.len > 1) {
            const sample = @min(@as(usize, evict_sample), self.keys.items.len);
            var victim: ?usize = null;
            var oldest: u64 = std.math.maxInt(u64);
            var s: usize = 0;
            while (s < sample) : (s += 1) {
                const idx = self.prng.random().uintLessThan(usize, self.keys.items.len);
                const k = self.keys.items[idx];
                if (std.mem.eql(u8, k, protect)) continue;
                const e = self.map.getPtr(k) orelse continue;
                if (e.last_access < oldest) {
                    oldest = e.last_access;
                    victim = idx;
                }
            }
            if (victim) |idx| {
                self.removeAt(idx);
                self.evicted += 1;
            } else break; // every sampled key was the protected one; try again next write
        }
    }

    fn writeOut(out: []u8, status: proto.Status, value: []const u8) OpResult {
        const n = @min(value.len, out.len);
        @memcpy(out[0..n], value[0..n]);
        return .{ .status = status, .len = n };
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
        const v = self.live(key) orelse return null;
        v.last_access = self.tick();
        const n = @min(v.bytes.len, out.len);
        @memcpy(out[0..n], v.bytes[0..n]);
        return n;
    }

    fn set(self: *Shard, key: []const u8, value: []const u8, ttl_ms: u32) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.put(key, value, ttl_ms, false);
    }

    fn del(self: *Shard, key: []const u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        const e = self.map.getPtr(key) orelse return false;
        const slot = e.slot;
        const was_live = !(e.expires_at != 0 and nowMs() >= e.expires_at);
        self.removeAt(slot);
        if (!was_live) self.expired += 1;
        return was_live;
    }

    fn incrBy(self: *Shard, key: []const u8, delta: i64, out: []u8) OpResult {
        self.mutex.lock();
        defer self.mutex.unlock();
        var cur: i64 = 0;
        if (self.live(key)) |v| {
            cur = std.fmt.parseInt(i64, v.bytes, 10) catch return .{ .status = .bad_request };
        }
        const new_value = std.math.add(i64, cur, delta) catch return .{ .status = .bad_request };
        var tmp: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&tmp, "{d}", .{new_value}) catch unreachable;
        self.put(key, s, 0, true) catch return .{ .status = .bad_request };
        return writeOut(out, .ok, s);
    }

    fn append(self: *Shard, key: []const u8, suffix: []const u8, out: []u8) OpResult {
        self.mutex.lock();
        defer self.mutex.unlock();
        var new_len: usize = suffix.len;
        if (self.live(key)) |v| {
            const total = v.bytes.len + suffix.len;
            if (total > proto.max_value) return .{ .status = .too_large };
            const nb = self.gpa.realloc(v.bytes, total) catch return .{ .status = .bad_request };
            @memcpy(nb[v.bytes.len..], suffix);
            self.bytes += suffix.len;
            v.bytes = nb;
            v.last_access = self.tick();
            new_len = total;
        } else {
            if (suffix.len > proto.max_value) return .{ .status = .too_large };
            self.put(key, suffix, 0, false) catch return .{ .status = .bad_request };
        }
        var tmp: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&tmp, "{d}", .{new_len}) catch unreachable;
        return writeOut(out, .ok, s);
    }

    fn setNx(self: *Shard, key: []const u8, value: []const u8, ttl_ms: u32, out: []u8) OpResult {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.live(key)) |_| return writeOut(out, .ok, "0");
        self.put(key, value, ttl_ms, false) catch return .{ .status = .bad_request };
        return writeOut(out, .ok, "1");
    }

    fn cas(self: *Shard, key: []const u8, expected: []const u8, new_value: []const u8, ttl_ms: u32, out: []u8) OpResult {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.live(key)) |v| {
            if (std.mem.eql(u8, v.bytes, expected)) {
                self.put(key, new_value, ttl_ms, false) catch return .{ .status = .bad_request };
                return writeOut(out, .ok, "1");
            }
        }
        return writeOut(out, .ok, "0");
    }

    fn getSet(self: *Shard, key: []const u8, new_value: []const u8, ttl_ms: u32, out: []u8) OpResult {
        self.mutex.lock();
        defer self.mutex.unlock();
        var status: proto.Status = .not_found;
        var len: usize = 0;
        if (self.live(key)) |v| {
            len = @min(v.bytes.len, out.len);
            @memcpy(out[0..len], v.bytes[0..len]);
            status = .ok;
        }
        self.put(key, new_value, ttl_ms, false) catch return .{ .status = .bad_request };
        return .{ .status = status, .len = len };
    }

    fn getDel(self: *Shard, key: []const u8, out: []u8) OpResult {
        self.mutex.lock();
        defer self.mutex.unlock();
        const v = self.live(key) orelse return .{ .status = .not_found };
        const len = @min(v.bytes.len, out.len);
        @memcpy(out[0..len], v.bytes[0..len]);
        self.removeAt(v.slot);
        return .{ .status = .ok, .len = len };
    }
};

// --- tests ---------------------------------------------------------------------------------------

const testing = std.testing;

test "set, get, overwrite, delete" {
    var s = try Store.init(testing.allocator, 8, 0);
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

test "a shard count that is not a power of two is rounded up and still works" {
    var s = try Store.init(testing.allocator, 10, 0); // rounds up to 16 shards
    defer s.deinit();
    try testing.expectEqual(@as(usize, 16), s.shards.len);

    var out: [16]u8 = undefined;
    try s.set("k", "v", 0);
    try testing.expectEqualStrings("v", out[0..s.get("k", &out).?]);
}

test "many keys land in their shards and all read back" {
    var s = try Store.init(testing.allocator, 16, 0);
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
    var s = try Store.init(testing.allocator, 4, 0);
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

test "the store evicts least recently used keys to stay under its byte budget" {
    // A tiny budget over many shards: one shard, so the budget bites predictably.
    var s = try Store.init(testing.allocator, 1, 2000);
    defer s.deinit();

    var kbuf: [32]u8 = undefined;
    const value = "0123456789" ** 5; // 50 bytes each

    // Keep one key hot so it survives eviction.
    try s.set("hot", value, 0);
    var out: [64]u8 = undefined;

    var i: usize = 0;
    while (i < 200) : (i += 1) {
        const key = try std.fmt.bufPrint(&kbuf, "cold-{d}", .{i});
        try s.set(key, value, 0);
        _ = s.get("hot", &out); // touch the hot key so it stays most recently used
    }

    try testing.expect(s.byteCount() <= 2000); // budget held
    try testing.expect(s.evictedCount() > 0); // and it actually evicted
    try testing.expect(s.get("hot", &out) != null); // the hot key was protected
}

test "incr, append, setnx, cas, getset, and getdel" {
    var s = try Store.init(testing.allocator, 4, 0);
    defer s.deinit();
    var out: [64]u8 = undefined;

    // incr from absent treats the key as zero
    try testing.expectEqualStrings("1", out[0..s.incrBy("n", 1, &out).len]);
    try testing.expectEqualStrings("11", out[0..s.incrBy("n", 10, &out).len]);
    try testing.expectEqualStrings("9", out[0..s.incrBy("n", -2, &out).len]);

    // incr on a non-integer value is rejected
    try s.set("word", "hello", 0);
    try testing.expectEqual(proto.Status.bad_request, s.incrBy("word", 1, &out).status);

    // append returns the new length
    try testing.expectEqualStrings("5", out[0..s.append("a", "hello", &out).len]); // new key
    try testing.expectEqualStrings("8", out[0..s.append("a", "!!!", &out).len]);
    try testing.expectEqualStrings("hello!!!", out[0..s.get("a", &out).?]);

    // setnx sets once
    try testing.expectEqualStrings("1", out[0..s.setNx("lock", "me", 0, &out).len]);
    try testing.expectEqualStrings("0", out[0..s.setNx("lock", "you", 0, &out).len]);
    try testing.expectEqualStrings("me", out[0..s.get("lock", &out).?]);

    // cas swaps only on a match
    try testing.expectEqualStrings("1", out[0..s.cas("lock", "me", "owned", 0, &out).len]);
    try testing.expectEqualStrings("0", out[0..s.cas("lock", "me", "again", 0, &out).len]);
    try testing.expectEqualStrings("owned", out[0..s.get("lock", &out).?]);

    // getset returns the old value, getdel removes and returns it
    const gs = s.getSet("lock", "fresh", 0, &out);
    try testing.expectEqual(proto.Status.ok, gs.status);
    try testing.expectEqualStrings("owned", out[0..gs.len]);
    const gd = s.getDel("lock", &out);
    try testing.expectEqual(proto.Status.ok, gd.status);
    try testing.expectEqualStrings("fresh", out[0..gd.len]);
    try testing.expect(s.get("lock", &out) == null);

    // getset and getdel on an absent key report not_found
    try testing.expectEqual(proto.Status.not_found, s.getDel("ghost", &out).status);
    try testing.expectEqual(proto.Status.not_found, s.getSet("ghost", "v", 0, &out).status);
}

test "incr preserves an existing ttl" {
    var s = try Store.init(testing.allocator, 4, 0);
    defer s.deinit();
    var out: [64]u8 = undefined;

    try s.set("n", "5", 40);
    _ = s.incrBy("n", 1, &out);
    try testing.expectEqualStrings("6", out[0..s.get("n", &out).?]);
    sleepMs(70);
    try testing.expect(s.get("n", &out) == null); // the ttl still fired after the incr
}

test "subscribers register once, list back, and unsubscribe" {
    var s = try Store.init(testing.allocator, 4, 0);
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
    var s = try Store.init(testing.allocator, 32, 0);
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
