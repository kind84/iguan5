const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const mode = b.standardReleaseOptions();

    const lib = b.addStaticLibrary("igua-n5", "iguan5.zig");
    lib.setBuildMode(mode);
    lib.addIncludeDir("./src/vendor/");
    lib.addCSourceFile("./src/vendor/lz4.c", &.{});
    lib.linkLibC();
    lib.install();

    var main_tests = b.addTest("iguan5.zig");
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);
}
