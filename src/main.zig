const std = @import("std");
const heap = std.heap;
const Fs = @import("Fs.zig");

pub fn main() !void {
    var gpa = heap.GeneralPurposeAllocator(.{}){};

    var path_buffer: [std.os.PATH_MAX]u8 = undefined;
    var full_path = try std.fs.realpath("testdata/lynx_gzip/data.n5", &path_buffer);
    var fs = try Fs.init(&gpa.allocator, full_path);

    var grid_position = [_]i64{ 0, 0, 0, 0, 0 };

    var attr = try fs.datasetAttributes("0/0");
    var d_block = try fs.getBlock("0/0", attr, &grid_position);
    defer d_block.deinit();
    var out = std.io.getStdOut();
    try out.writeFileAll(d_block.source, .{});
}
