const std = @import("std");
const IguaN5Builder = @import("src/builder.zig");

pub fn build(b: *std.build.Builder) !void {
    const mode = b.standardReleaseOptions();

    var builder = try IguaN5Builder.init(b, ".");
    defer builder.deinit();

    const lib = b.addStaticLibrary("igua-n5", "iguan5.zig");
    lib.setBuildMode(mode);
    builder.link(lib);
    lib.install();

    var main_tests = b.addTest("iguan5.zig");
    builder.link(main_tests);
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);
}
