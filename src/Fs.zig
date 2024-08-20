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

allocator: Allocator,
basePath: []const u8,

pub fn init(allocator: Allocator, basePath: []const u8) !Fs {
    const data_path = try path.join(allocator, &.{ basePath, "data.n5" });
    errdefer allocator.free(data_path);
    // Catch the error here if dir does not exist and create it.
    // It means that the Fs is used for writing.
    var dir: std.fs.Dir = undefined;
    dir = std.fs.openDirAbsolute(data_path, .{}) catch |err| blk: {
        if (err == std.fs.Dir.OpenError.FileNotFound) {
            try dir.makePath(data_path);
            dir = try std.fs.openDirAbsolute(data_path, .{});
            break :blk dir;
        } else return err;
    };
    defer dir.close();

    return .{
        .allocator = allocator,
        .basePath = data_path,
    };
}

pub fn deinit(self: Fs) void {
    self.allocator.free(self.basePath);
}

/// returns the datablock at the provided coordinates.
pub fn getBlock(
    self: Fs,
    datasetPath: []const u8,
    gridPosition: []i64,
    attributes: DatasetAttributes(std.fs.File),
) !Datablock(std.fs.File) {
    const dataset_full_path = try path.join(self.allocator, &.{ self.basePath, datasetPath });
    defer self.allocator.free(dataset_full_path);
    const datablock_path = try self.datablockPath(dataset_full_path, gridPosition);
    defer self.allocator.free(datablock_path);
    // catch the error and create the file for the writer.
    var fd: std.fs.File = undefined;
    fd = std.fs.openFileAbsolute(datablock_path, .{}) catch |err| blk: {
        if (err == std.fs.Dir.OpenError.FileNotFound) {
            fd = try std.fs.createFileAbsolute(datablock_path, .{});
            break :blk fd;
        } else return err;
    };

    return Datablock(std.fs.File).init(self.allocator, fd, dataset_full_path, gridPosition, attributes);
}

fn datablockPath(self: Fs, datasetPath: []u8, gridPosition: []i64) ![]u8 {
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
    for (gridPosition, 0..) |gp, i| {
        const gp_str = try fmt.allocPrint(self.allocator, "{d}", .{gp});
        defer self.allocator.free(gp_str);
        gps[i + 1] = try self.allocator.alloc(u8, gp_str.len);
        @memcpy(gps[i + 1], gp_str);
    }

    const full_path = try path.join(self.allocator, gps);
    defer self.allocator.free(full_path);
    const final_path = try self.allocator.alloc(u8, full_path.len);
    @memcpy(final_path, full_path);
    return final_path;
}

pub fn datasetAttributes(self: Fs, datasetPath: []const u8) !DatasetAttributes(std.fs.File) {
    const attr_full_path = try path.join(self.allocator, &.{ self.basePath, datasetPath });
    defer self.allocator.free(attr_full_path);

    return DatasetAttributes(std.fs.File).init(self.allocator, attr_full_path);
}

test "init" {
    const allocator = std.testing.allocator;

    const buff_size = comptime util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    const full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
    const n5_path = try path.join(allocator, &.{ full_path, "data.n5" });
    var fs = try Fs.init(allocator, full_path);
    _ = try std.fs.openDirAbsolute(n5_path, .{});
    fs.deinit();
    allocator.free(n5_path);
}

test "init new folder" {
    const allocator = std.testing.allocator;

    const buff_size = comptime util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    const full_path = try std.fs.realpath("testdata", &path_buffer);
    const data_path = try path.join(allocator, &.{ full_path, "banana" });
    var dir: std.fs.Dir = undefined;
    try dir.deleteTree(data_path);
    const n5_path = try path.join(allocator, &.{ data_path, "data.n5" });
    var fs = try Fs.init(allocator, data_path);
    dir = try std.fs.openDirAbsolute(n5_path, .{});
    fs.deinit();
    dir.close();
    try dir.deleteTree(data_path);
    allocator.free(n5_path);
    allocator.free(data_path);
}

test "read lz4" {
    const allocator = std.testing.allocator;

    const buff_size = comptime util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    const full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
    var fs = try Fs.init(allocator, full_path);
    errdefer fs.deinit();

    var attr = try fs.datasetAttributes("0/0");
    errdefer attr.deinit();

    var grid_position = [_]i64{ 0, 0, 0, 0, 0 };

    var d_block = try fs.getBlock("0/0", &grid_position, attr);
    errdefer d_block.deinit();
    const buf = try allocator.alloc(u8, d_block.len);
    errdefer allocator.free(buf);
    _ = try d_block.reader().read(buf);

    allocator.free(buf);
    d_block.deinit();
    attr.deinit();
    fs.deinit();
}

test "write lz4" {
    const allocator = std.testing.allocator;

    const buff_size = comptime util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    const full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
    var fs = try Fs.init(allocator, full_path);
    errdefer fs.deinit();

    var attr = try fs.datasetAttributes("0/0");
    errdefer attr.deinit();

    var grid_position = [_]i64{ 0, 0, 0, 0, 0 };

    var d_block = try fs.getBlock("0/0", &grid_position, attr);
    errdefer d_block.deinit();
    const buf_r = try allocator.alloc(u8, d_block.len);
    errdefer allocator.free(buf_r);
    _ = try d_block.reader().read(buf_r);

    grid_position[4] = 1;

    var d_block_w = try fs.getBlock("0/0", &grid_position, attr);
    // errdefer d_block_w.deinit();
    //var file_path_buffer: [buff_size]u8 = undefined;
    //var file_full_path = try std.fs.realpath("testdata/lynx_lz4/data.n5/0/0/0/0/0/0/1", &file_path_buffer);
    // defer std.fs.deleteFileAbsolute(file_full_path) catch unreachable;
    _ = try d_block_w.writer(0).write(buf_r);
    d_block_w.deinit();

    // var d_block_r2 = try fs.getBlock("0/0", &grid_position, attr);
    // errdefer d_block_r2.deinit();
    // var buf_w = try allocator.alloc(u8, d_block_r2.len);
    // errdefer allocator.free(buf_w);
    // _ = try d_block_r2.reader().read(buf_w);
    // try std.testing.expect(buf_w.len == buf_r.len);

    allocator.free(buf_r);
    //allocator.free(buf_w);
    //d_block_r2.deinit();
    d_block.deinit();
    attr.deinit();
    fs.deinit();
}
