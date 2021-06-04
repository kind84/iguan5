const std = @import("std");
const fmt = std.fmt;
const fs = std.fs;
const path = fs.path;
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
    var dir = try fs.openDirAbsolute(basePath, .{});
    defer dir.close();

    return Fs{
        .allocator = allocator,
        .basePath = basePath,
    };
}

/// returns the datablock at the provided coordinates.
pub fn getBlock(self: *Fs, datasetPath: []const u8, attributes: DatasetAttributes, gridPosition: []i64) !Datablock(fs.File) {
    var dataset_path = try self.datablockPath(datasetPath, gridPosition);
    defer self.allocator.free(dataset_path);
    var fd = try fs.openFileAbsolute(dataset_path, .{});

    return Datablock(fs.File).init(self.allocator, fd, attributes, gridPosition);
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
