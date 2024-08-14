const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addStaticLibrary(.{
        .name = "igua-n5",
        .target = target,
        .optimize = optimize,
    });
    lib.addIncludePath(b.path("src/vendor"));
    lib.addCSourceFile(.{ .file = b.path("src/vendor/lz4.c") });
    lib.linkLibC();
    b.installArtifact(lib);

    var main_tests = b.addTest(.{
        .root_source_file = b.path("iguan5.zig"),
        .target = target,
        .optimize = optimize,
    });
    main_tests.addIncludePath(b.path("src/vendor"));
    main_tests.addCSourceFile(.{ .file = b.path("src/vendor/lz4.c") });
    main_tests.linkLibC();
    const uts = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&uts.step);
}
