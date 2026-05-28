const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("zremdic", .{
        .root_source_file = b.path("src/zremdic.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    // `zig build` / `zig build run` builds and runs the server binary.
    const server = b.addExecutable(.{
        .name = "zremdic-server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{.{ .name = "zremdic", .module = mod }},
        }),
    });
    b.installArtifact(server);
    const run = b.addRunArtifact(server);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);
    b.step("run", "Run the server (args: [port] [threads] [shards])").dependOn(&run.step);

    // `zig build bench` runs the loopback throughput benchmark.
    const bench = b.addExecutable(.{
        .name = "zremdic-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{.{ .name = "zremdic", .module = mod }},
        }),
    });
    const bench_run = b.addRunArtifact(bench);
    bench_run.step.dependOn(b.getInstallStep());
    b.step("bench", "Run the loopback throughput benchmark").dependOn(&bench_run.step);

    // `zig build example` runs the client API tour against an in-process server.
    const example = b.addExecutable(.{
        .name = "zremdic-example",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/usage.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{.{ .name = "zremdic", .module = mod }},
        }),
    });
    const example_run = b.addRunArtifact(example);
    example_run.step.dependOn(b.getInstallStep());
    b.step("example", "Run the Zig usage example").dependOn(&example_run.step);

    // `zig build test` runs the protocol, store, and end-to-end tests.
    const tests = b.addTest(.{ .root_module = mod });
    b.step("test", "Run all tests").dependOn(&b.addRunArtifact(tests).step);
}
