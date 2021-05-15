const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    //const lib = b.addStaticLibrary("igua-n5", "src/main.zig");
    const lib = b.addExecutable("igua-n5", "src/main.zig");
    lib.setBuildMode(mode);
    lib.addIncludeDir("src/vendor");
    lib.addCSourceFile("src/vendor/lz4.c", &.{});
    lib.linkLibC();
    lib.install();

    var main_tests = b.addTest("src/main.zig");
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);
}
