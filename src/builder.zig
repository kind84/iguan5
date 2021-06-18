const std = @import("std");
const path = std.fs.path;

const Self = @This();

allocator: *std.mem.Allocator,
src_path: []const u8,
include_dir: []const u8,

pub fn init(b: *std.build.Builder, basePath: []const u8) !Self {
    const src_path = try std.fs.path.join(b.allocator, &[_][]const u8{ basePath, "src/vendor/lz4.c" });
    errdefer b.allocator.free(src_path);

    const include_dir = try std.fs.path.join(b.allocator, &[_][]const u8{ basePath, "src/vendor" });
    errdefer b.allocator.free(include_dir);

    return Self{
        .allocator = b.allocator,
        .src_path = src_path,
        .include_dir = include_dir,
    };
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.src_path);
    self.allocator.free(self.include_dir);
}

pub fn link(self: Self, obj: *std.build.LibExeObjStep) void {
    obj.addIncludeDir(self.include_dir);
    obj.addCSourceFile(self.src_path, &.{});
    obj.linkLibC();
}
