const std = @import("std");
const fmt = std.fmt;
const io = std.io;
const path = std.fs.path;
const Allocator = std.mem.Allocator;
const Datablock = @import("datablock.zig").Datablock;
const DatasetAttributes = @import("dataset_attributes.zig").DatasetAttributes;
const DataType = @import("dataset_attributes.zig").DataType;
const Compression = @import("dataset_attributes.zig").Compression;
const CompressionType = @import("dataset_attributes.zig").CompressionType;
const util = @import("util.zig");

/// interacts with N5 on a local filesystem.
pub const Fs = @This();

allocator: *Allocator,
basePath: []const u8,

pub fn init(allocator: *Allocator, basePath: []const u8) !Fs {
    var data_path = try path.join(allocator, &.{ basePath, "data.n5" });
    errdefer allocator.free(data_path);
    // Catch the error here if dir does not exist and create it.
    // It means that the Fs in used for writing.
    var dir: std.fs.Dir = undefined;
    dir = std.fs.openDirAbsolute(data_path, .{}) catch |err| blk: {
        if (err == std.fs.Dir.OpenError.FileNotFound) {
            try dir.makePath(data_path);
            dir = try std.fs.openDirAbsolute(data_path, .{});
            break :blk dir;
        } else return err;
    };
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
pub fn getBlock(
    self: *Fs,
    datasetPath: []const u8,
    gridPosition: []i64,
    attributes: DatasetAttributes(std.fs.File),
) !Datablock(std.fs.File) {
    var dataset_full_path = try path.join(self.allocator, &.{ self.basePath, datasetPath });
    defer self.allocator.free(dataset_full_path);
    var datablock_path = try self.datablockPath(dataset_full_path, gridPosition);
    defer self.allocator.free(datablock_path);
    // TODO: catch the error and create the file for the writer.
    var fd = try std.fs.openFileAbsolute(datablock_path, .{});

    return Datablock(std.fs.File).init(self.allocator, fd, dataset_full_path, gridPosition, attributes);
}

fn datablockPath(self: *Fs, datasetPath: []u8, gridPosition: []i64) ![]u8 {
    var gps = try self.allocator.alloc([]u8, gridPosition.len + 1);
    defer {
        // gps[0] is already freed by the caller
        var i: u8 = 1;
        while (i < gps.len) : (i += 1) {
            self.allocator.free(gps[i]);
        }
        self.allocator.free(gps);
    }
    gps[0] = datasetPath;
    for (gridPosition) |gp, i| {
        const gp_str = try fmt.allocPrint(self.allocator, "{d}", .{gp});
        defer self.allocator.free(gp_str);
        gps[i + 1] = try self.allocator.alloc(u8, gp_str.len);
        std.mem.copy(u8, gps[i + 1], gp_str);
    }

    var full_path = try path.join(self.allocator, gps);
    defer self.allocator.free(full_path);
    var final_path = try self.allocator.alloc(u8, full_path.len);
    std.mem.copy(u8, final_path, full_path);
    std.debug.print("{s}\n", .{final_path});
    return final_path;
}

pub fn datasetAttributes(self: *Fs, datasetPath: []const u8) !DatasetAttributes(std.fs.File) {
    var attr_full_path = try path.join(self.allocator, &.{ self.basePath, datasetPath });
    defer self.allocator.free(attr_full_path);

    return DatasetAttributes(std.fs.File).init(self.allocator, attr_full_path);
}

test "init" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &gpa.allocator;

    comptime var buff_size = util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    var full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
    var n5_path = try path.join(allocator, &.{ full_path, "data.n5" });
    var fs = try Fs.init(allocator, full_path);
    _ = try std.fs.openDirAbsolute(n5_path, .{});
    fs.deinit();
    allocator.free(n5_path);

    try std.testing.expect(!gpa.deinit());
}

test "init new folder" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &gpa.allocator;

    comptime var buff_size = util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    var full_path = try std.fs.realpath("testdata", &path_buffer);
    var data_path = try path.join(allocator, &.{ full_path, "banana" });
    var dir: std.fs.Dir = undefined;
    try dir.deleteTree(data_path);
    var n5_path = try path.join(allocator, &.{ data_path, "data.n5" });
    var fs = try Fs.init(allocator, data_path);
    dir = try std.fs.openDirAbsolute(n5_path, .{});
    fs.deinit();
    dir.close();
    try dir.deleteTree(data_path);
    allocator.free(n5_path);
    allocator.free(data_path);

    try std.testing.expect(!gpa.deinit());
}

test "lz4" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = &gpa.allocator;

    comptime var buff_size = util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    var full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
    var fs = try Fs.init(allocator, full_path);
    errdefer fs.deinit();

    var attr = try fs.datasetAttributes("0/0");
    errdefer attr.deinit();

    var grid_position = [_]i64{ 0, 0, 0, 0, 0 };

    var d_block = try fs.getBlock("0/0", &grid_position, attr);
    errdefer d_block.deinit();
    var out = std.io.getStdOut();
    var buf = try allocator.alloc(u8, d_block.len);
    errdefer allocator.free(buf);
    _ = try d_block.reader().read(buf);
    try out.writeAll(buf);
    std.debug.print("\n", .{});

    allocator.free(buf);
    d_block.deinit();
    attr.deinit();
    fs.deinit();
    try std.testing.expect(!gpa.deinit());
}

test "write" {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = &gpa.allocator;

    // var path_buffer: [std.os.PATH_MAX]u8 = undefined;
    // var full_path = try std.fs.realpath("testdata/write_lz4", &path_buffer);
    // var fs = try Fs.init(allocator, full_path);
    // errdefer fs.deinit();

    // var attr = try fs.datasetAttributes("0/0");
    // errdefer attr.deinit();

    // var grid_position = [_]i64{ 0, 0, 0, 0, 0 };

    // var d_block = try fs.getBlock("0/0", &grid_position, attr);
    // errdefer d_block.deinit();

}
