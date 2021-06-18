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
    errdefer allocator.free(data_path);
    var dir = try std.fs.openDirAbsolute(data_path, .{});
    defer dir.close();

    return Fs{
        .allocator = allocator,
        .basePath = data_path,
    };
}

pub fn deinit(self: *Fs) void {
    self.allocator.free(self.basePath);
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
    defer self.allocator.free(full_path);
    for (gridPosition) |gp| {
        var buf = try self.allocator.alloc(u8, 4096);
        errdefer self.allocator.free(buf);
        const gp_str = try fmt.bufPrint(buf, "{d}", .{gp});
        var temp_path = try path.join(self.allocator, &.{ full_path, gp_str });
        defer self.allocator.free(temp_path);
        full_path = try self.allocator.resize(full_path, temp_path.len);
        std.mem.copy(u8, full_path, temp_path);
        self.allocator.free(buf);
    }

    var final_path = try self.allocator.alloc(u8, full_path.len);
    std.mem.copy(u8, final_path, full_path);
    return final_path;
}

test "init" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &gpa.allocator;

    var path_buffer: [std.os.PATH_MAX]u8 = undefined;
    var full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
    var fs = try Fs.init(allocator, full_path);
    fs.deinit();

    try std.testing.expect(!gpa.deinit());
}

test "lz4" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &gpa.allocator;

    var path_buffer: [std.os.PATH_MAX]u8 = undefined;
    var full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
    var fs = try Fs.init(allocator, full_path);
    errdefer fs.deinit();

    var grid_position = [_]i64{ 0, 0, 0, 0, 0 };

    var attr = try fs.datasetAttributes("0/0");
    errdefer attr.deinit();
    var d_block = try fs.getBlock("0/0", attr, &grid_position);
    errdefer d_block.deinit();
    var out = std.io.getStdOut();
    var buf = try allocator.alloc(u8, d_block.len);
    errdefer allocator.free(buf);
    var n = try d_block.reader().read(buf);
    try out.writeAll(buf);

    allocator.free(buf);
    d_block.deinit();
    attr.deinit();
    fs.deinit();
    try std.testing.expect(!gpa.deinit());
}
