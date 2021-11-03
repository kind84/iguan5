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

/// interacts with N5 on a local filesystem.
pub const Fs = @This();

allocator: *Allocator,
basePath: []const u8,

pub fn init(allocator: *Allocator, basePath: []const u8) !Fs {
    var data_path = try path.join(allocator, &.{ basePath, "data.n5" });
    errdefer allocator.free(data_path);
    // TODO: catch error here and if dir does not exist, create it.
    // It can be the case if the Fs in used for writing.
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

// implement the writer interface
pub const Writer = io.Writer(*Fs, anyerror, write);

pub fn write(self: *Fs, bytes: []const u8) !usize {
    _ = self;
    _ = bytes;
    return 0;
}

pub fn writer(self: *Fs) Writer {
    return .{ .context = self };
}

/// returns the datablock at the provided coordinates.
pub fn getBlock(
    self: *Fs,
    datasetPath: []const u8,
    gridPosition: []i64,
) !Datablock(std.fs.File) {
    var dataset_full_path = try path.join(self.allocator, &.{ self.basePath, datasetPath });
    defer self.allocator.free(dataset_full_path);
    var datablock_path = try self.datablockPath(dataset_full_path, gridPosition);
    defer self.allocator.free(datablock_path);
    // TODO: catch the error and create the file for the writer.
    var fd = try std.fs.openFileAbsolute(datablock_path, .{});

    return Datablock(std.fs.File).init(self.allocator, fd, dataset_full_path, gridPosition);
}

fn datablockPath(self: *Fs, datasetPath: []const u8, gridPosition: []i64) ![]u8 {
    var full_path = try path.resolve(self.allocator, &.{datasetPath});
    defer self.allocator.free(full_path);
    for (gridPosition) |gp| {
        const gp_str = try fmt.allocPrint(self.allocator, "{d}", .{gp});
        defer self.allocator.free(gp_str);
        var temp_path = try path.join(self.allocator, &.{ full_path, gp_str });
        defer self.allocator.free(temp_path);
        full_path = try self.allocator.resize(full_path, temp_path.len);
        std.mem.copy(u8, full_path, temp_path);
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

    var d_block = try fs.getBlock("0/0", &grid_position);
    errdefer d_block.deinit();
    var out = std.io.getStdOut();
    var buf = try allocator.alloc(u8, d_block.len);
    errdefer allocator.free(buf);
    _ = try d_block.reader().read(buf);
    try out.writeAll(buf);
    std.debug.print("\n", .{});

    allocator.free(buf);
    d_block.deinit();
    fs.deinit();
    try std.testing.expect(!gpa.deinit());
}
