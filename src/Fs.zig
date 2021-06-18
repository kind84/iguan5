const std = @import("std");
const fmt = std.fmt;
const path = std.fs.path;
const Allocator = std.mem.Allocator;
const Datablock = @import("datablock.zig").Datablock;
const DatasetAttributes = @import("dataset_attributes.zig").DatasetAttributes;
const DataType = @import("dataset_attributes.zig").DataType;
const Compression = @import("dataset_attributes.zig").Compression;
const CompressionType = @import("dataset_attributes.zig").CompressionType;

const json_file = "attributes.json";

/// interacts with N5 on a local filesystem.
pub const Fs = @This();

allocator: *Allocator,
basePath: []const u8,

pub fn init(allocator: *Allocator, basePath: []const u8) !Fs {
    var data_path = try path.join(allocator, &.{ basePath, "data.n5" });
    var dir = try std.fs.openDirAbsolute(data_path, .{});
    defer dir.close();

    return Fs{
        .allocator = allocator,
        .basePath = data_path,
    };
}

/// returns the datablock at the provided coordinates.
pub fn getBlock(self: *Fs, datasetPath: []const u8, attributes: DatasetAttributes, gridPosition: []i64) !Datablock(std.fs.File) {
    var dataset_path = try self.datablockPath(datasetPath, gridPosition);
    defer self.allocator.free(dataset_path);
    var fd = try std.fs.openFileAbsolute(dataset_path, .{});

    return Datablock(std.fs.File).init(self.allocator, fd, attributes, gridPosition);
}

/// returns the attributes for the provided dataset path.
pub fn datasetAttributes(self: *Fs, datasetPath: []const u8) !DatasetAttributes {
    var full_path = try path.join(self.allocator, &.{ self.basePath, datasetPath, json_file });
    defer self.allocator.free(full_path);

    return DatasetAttributes.init(self.allocator, full_path);
}

fn datablockPath(self: *Fs, datasetPath: []const u8, gridPosition: []i64) ![]u8 {
    var full_path = try path.join(self.allocator, &.{ self.basePath, datasetPath });
    for (gridPosition) |gp| {
        var buf = try self.allocator.alloc(u8, 4096);
        errdefer self.allocator.free(buf);
        const gp_str = try fmt.bufPrint(buf, "{d}", .{gp});
        full_path = try path.join(self.allocator, &.{ full_path, gp_str });
        self.allocator.free(buf);
    }

    return full_path;
}

test "lz4" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &gpa.allocator;

    var path_buffer: [std.os.PATH_MAX]u8 = undefined;
    var full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
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
