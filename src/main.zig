const std = @import("std");
const heap = std.heap;
const Fs = @import("Fs.zig");

pub fn main() !void {
    var gpa = heap.GeneralPurposeAllocator(.{}){};

    var path_buffer: [std.os.PATH_MAX]u8 = undefined;
    var full_path = try std.fs.realpath("testdata/lynx_lz4/data.n5", &path_buffer);
    var allocator = &gpa.allocator;
    var fs = try Fs.init(allocator, full_path);

    var grid_position = [_]i64{ 0, 0, 0, 0, 0 };

    var attr = try fs.datasetAttributes("0/0");
    defer attr.deinit();
    std.debug.print("{}\n", .{attr});
    var d_block = try fs.getBlock("0/0", attr, &grid_position);
    defer d_block.deinit();
    var out = std.io.getStdOut();
    var buf = try allocator.alloc(u8, d_block.len);
    var n = try d_block.reader().read(buf);
    defer allocator.free(buf);
    try out.writeAll(buf);
}

test "iguan5" {
    _ = @import("dataset_attributes.zig");
}
