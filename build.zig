const std = @import("std");
const LZ4Builder = @import("src/builder.zig");

pub fn build(b: *std.build.Builder) !void {
    const mode = b.standardReleaseOptions();

    var lz4 = try LZ4Builder.init(b, ".");
    defer lz4.deinit();

    const lib = b.addStaticLibrary("igua-n5", "iguan5.zig");
    lib.setBuildMode(mode);
    lz4.link(lib);
    lib.install();

    var main_tests = b.addTest("iguan5.zig");
    lz4.link(main_tests);
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);
}
