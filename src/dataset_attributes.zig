const std = @import("std");
const fmt = std.fmt;
const fs = std.fs;
const heap = std.heap;
const json = std.json;
const mem = std.mem;
const meta = std.meta;
const os = std.os;
const path = std.fs.path;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const Allocator = mem.Allocator;

const json_file = "attributes.json";

pub const DatasetAttributes = struct {
    allocator: *Allocator,
    dimensions: []u64,
    blockSize: []u64,
    dataType: DataType,
    compression: Compression,

    pub fn init(allocator: *Allocator, attr_path: []const u8) !DatasetAttributes {
        var full_path = try path.join(allocator, &.{ attr_path, json_file });
        defer allocator.free(full_path);

        var attr_fd = try fs.openFileAbsolute(full_path, .{});
        defer attr_fd.close();
        var max_size = try attr_fd.getEndPos();
        const str = try attr_fd.readToEndAlloc(allocator, max_size);
        defer allocator.free(str);
        var stream = json.TokenStream.init(str);

        var next_data_type = false;
        var next_compression = false;
        var next_compression_type = false;
        var next_dimensions = false;
        var next_block_size = false;
        var next_level = false;
        var next_zlib = false;
        var data_type: DataType = undefined;
        var compression_type: CompressionType = undefined;
        var dimensions = std.ArrayList(u64).init(allocator);
        var block_size = std.ArrayList(u64).init(allocator);
        var comp_block_size: u32 = 0;
        var level: i32 = 0;
        var zlib = false;

        while (try stream.next()) |token| {
            switch (token) {
                .String => |string| {
                    var st = string.slice(stream.slice, stream.i - 1);
                    if (mem.eql(u8, st, "dataType")) {
                        next_data_type = true;
                        continue;
                    }
                    if (mem.eql(u8, st, "compression")) {
                        next_compression = true;
                        continue;
                    }
                    if (mem.eql(u8, st, "type")) {
                        next_compression_type = true;
                        continue;
                    }
                    if (mem.eql(u8, st, "dimensions")) {
                        next_dimensions = true;
                        continue;
                    }
                    if (mem.eql(u8, st, "blockSize")) {
                        next_block_size = true;
                        continue;
                    }
                    if (mem.eql(u8, st, "level")) {
                        next_level = true;
                        continue;
                    }
                    if (mem.eql(u8, st, "useZlib")) {
                        next_zlib = true;
                        continue;
                    }
                    if (next_data_type) {
                        data_type = meta.stringToEnum(DataType, st).?;
                        next_data_type = false;
                        continue;
                    }
                    if (next_compression_type) {
                        compression_type = meta.stringToEnum(CompressionType, st).?;
                        next_compression_type = false;
                        continue;
                    }
                },
                .ObjectBegin => {},
                .ObjectEnd => {
                    if (next_compression) {
                        next_compression = false;
                        continue;
                    }
                },
                .ArrayBegin => {},
                .ArrayEnd => {
                    if (next_dimensions) {
                        next_dimensions = false;
                        continue;
                    }
                    if (next_block_size) {
                        next_block_size = false;
                        continue;
                    }
                },
                .Number => |num| {
                    if (next_dimensions) {
                        var val = try fmt.parseInt(u64, num.slice(stream.slice, stream.i - 1), 10);
                        try dimensions.append(val);
                        continue;
                    }
                    if (next_block_size) {
                        if (next_compression) {
                            var val = try fmt.parseInt(u32, num.slice(stream.slice, stream.i - 1), 10);
                            comp_block_size = val;
                            next_block_size = false;
                            continue;
                        } else {
                            var val = try fmt.parseInt(u64, num.slice(stream.slice, stream.i - 1), 10);
                            try block_size.append(val);
                            continue;
                        }
                    }
                    if (next_level) {
                        var val = try fmt.parseInt(i32, num.slice(stream.slice, stream.i - 1), 10);
                        level = val;
                        next_level = false;
                        continue;
                    }
                },
                .True => {
                    if (next_zlib) {
                        zlib = true;
                        next_zlib = false;
                        continue;
                    }
                },
                .False => {
                    if (next_zlib) {
                        next_zlib = false;
                        continue;
                    }
                },
                .Null => {},
            }
        }

        // json.parse does not allow to have optional fields for now
        //
        // var d_attr = try json.parse(DatasetAttributes, &stream, .{ .allocator = allocator, .ignore_unknown_fields = true });
        // defer json.parseFree(DatasetAttributes, d_attr, .{ .allocator = allocator });

        return DatasetAttributes{
            .allocator = allocator,
            .dataType = data_type,
            .compression = Compression{
                .type = compression_type,
                .useZlib = zlib,
                .level = level,
                .blockSize = comp_block_size,
            },
            .dimensions = dimensions.toOwnedSlice(),
            .blockSize = block_size.toOwnedSlice(),
        };
    }

    pub fn deinit(self: *DatasetAttributes) void {
        self.allocator.free(self.dimensions);
        self.allocator.free(self.blockSize);
    }
};

pub const Compression = struct {
    type: CompressionType,
    useZlib: bool,
    level: i32,
    blockSize: u32,
};

pub const CompressionType = enum {
    bzip2,
    blosc,
    gzip,
    lz4,
    raw,
    xz,
};

pub const DataType = enum {
    uint8,
    uint16,
    uint32,
    uint64,
    int8,
    int16,
    int32,
    int64,
    float32,
    float64,
    object,
};

test "init" {
    var gpa = heap.GeneralPurposeAllocator(.{}){};
    var allocator = &gpa.allocator;
    var path_buffer: [os.PATH_MAX]u8 = undefined;
    var full_path = try fs.realpath("testdata/lynx_raw/data.n5/0/0", &path_buffer);

    var da = try DatasetAttributes.init(allocator, full_path);

    try expect(da.dataType == DataType.uint8);
    var expected_dim = [_]u64{ 1920, 1080, 3, 1, 1 };
    for (expected_dim) |dim, i| {
        try expect(da.dimensions[i] == dim);
    }
    var expected_block_size = [_]u64{ 512, 512, 1, 1, 1 };
    for (expected_block_size) |block, i| {
        try expect(da.blockSize[i] == block);
    }
    try expect(da.compression.type == CompressionType.raw);
    try expect(da.compression.useZlib == false);
    try expect(da.compression.blockSize == 0);
    try expect(da.compression.level == 0);
    da.deinit();
    try expect(!gpa.deinit());
}
