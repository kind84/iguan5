const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const mode = b.standardReleaseOptions();

    const lib = b.addStaticLibrary("igua-n5", "iguan5.zig");
    lib.setBuildMode(mode);
    lib.addIncludeDir("src/vendor");
    lib.addCSourceFile("src/vendor/lz4.c", &.{});
    lib.linkLibC();
    lib.install();
}
