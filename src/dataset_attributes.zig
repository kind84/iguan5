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
const util = @import("util.zig");

const json_file = "attributes.json";

pub fn DatasetAttributes(comptime AttributesType: type) type {
    return struct {
        allocator: Allocator,
        dimensions: []u64,
        blockSize: []u64,
        dataType: DataType,
        compression: Compression,

        const Self = @This();

        pub fn init(allocator: Allocator, source: []const u8) !Self {
            var str: []u8 = undefined;
            defer allocator.free(str);
            var attr: AttributesType = undefined;

            switch (AttributesType) {
                std.fs.File => {
                    defer attr.close();
                    const full_path = try path.join(allocator, &.{ source, json_file });
                    defer allocator.free(full_path);

                    attr = try fs.openFileAbsolute(full_path, .{});
                    const max_size = try attr.getEndPos();
                    str = try attr.readToEndAlloc(allocator, max_size);
                },
                []u8, []const u8 => {
                    str = try allocator.alloc(u8, source.len);
                    @memcpy(str, source[0..]);
                },
                else => unreachable,
            }

            var stream = json.Scanner.initCompleteInput(allocator, str);

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

            while (stream.next()) |token| {
                switch (token) {
                    // object_begin,
                    // object_end,
                    // array_begin,
                    // array_end,

                    // true,
                    // false,
                    // null,

                    // number: []const u8,
                    // partial_number: []const u8,
                    // allocated_number: []u8,

                    // string: []const u8,
                    // partial_string: []const u8,
                    // partial_string_escaped_1: [1]u8,
                    // partial_string_escaped_2: [2]u8,
                    // partial_string_escaped_3: [3]u8,
                    // partial_string_escaped_4: [4]u8,
                    // allocated_string: []u8,

                    // end_of_document,
                    .string => |st| {
                        // const st = string.slice(stream.slice, stream.i - 1);
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
                    .object_begin => {},
                    .object_end => {
                        if (next_compression) {
                            next_compression = false;
                            continue;
                        }
                    },
                    .array_begin => {},
                    .array_end => {
                        if (next_dimensions) {
                            next_dimensions = false;
                            continue;
                        }
                        if (next_block_size) {
                            next_block_size = false;
                            continue;
                        }
                    },
                    .number => |num| {
                        if (next_dimensions) {
                            const val = try fmt.parseInt(u64, num, 10);
                            try dimensions.append(val);
                            continue;
                        }
                        if (next_block_size) {
                            if (next_compression) {
                                const val = try fmt.parseInt(u32, num, 10);
                                comp_block_size = val;
                                next_block_size = false;
                                continue;
                            } else {
                                const val = try fmt.parseInt(u64, num, 10);
                                try block_size.append(val);
                                continue;
                            }
                        }
                        if (next_level) {
                            const val = try fmt.parseInt(i32, num, 10);
                            level = val;
                            next_level = false;
                            continue;
                        }
                    },
                    .true => {
                        if (next_zlib) {
                            zlib = true;
                            next_zlib = false;
                            continue;
                        }
                    },
                    .false => {
                        if (next_zlib) {
                            next_zlib = false;
                            continue;
                        }
                    },
                    else => {},
                }
            } else |err| {
                return err;
            }

            // json.parse does not allow to have optional fields for now
            //
            // var d_attr = try json.parse(DatasetAttributes, &stream, .{ .allocator = allocator, .ignore_unknown_fields = true });
            // defer json.parseFree(DatasetAttributes, d_attr, .{ .allocator = allocator });

            // This does not work for the moment:
            // /usr/local/zig/lib/std/json/static.zig:107:39: error: cannot dereference non-pointer type 'json.scanner.Reader(4096,[]const u8)'
            //
            // const r = json.reader(allocator, attr);
            // const d_attr = try json.parseFromTokenSource(Self, allocator, r, .{});
            //
            // return d_attr;

            return Self{
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

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.dimensions);
            self.allocator.free(self.blockSize);
        }
    };
}

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

test "init file" {
    var gpa = heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    const buff_size = comptime util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    const full_path = try fs.realpath("testdata/lynx_raw/data.n5/0/0", &path_buffer);

    var da = try DatasetAttributes(std.fs.File).init(allocator, full_path);

    try expect(da.dataType == DataType.uint8);
    const expected_dim = [_]u64{ 1920, 1080, 3, 1, 1 };
    for (expected_dim, 0..) |dim, i| {
        try expect(da.dimensions[i] == dim);
    }
    const expected_block_size = [_]u64{ 512, 512, 1, 1, 1 };
    for (expected_block_size, 0..) |block, i| {
        try expect(da.blockSize[i] == block);
    }
    try expect(da.compression.type == CompressionType.raw);
    try expect(da.compression.useZlib == false);
    try expect(da.compression.blockSize == 0);
    try expect(da.compression.level == 0);
    da.deinit();
    const check = gpa.deinit();
    try expect(check != .leak);
}

test "init buffer" {
    const attr = "{\"dataType\":\"uint8\",\"compression\":{\"type\":\"lz4\",\"blockSize\":65536},\"blockSize\":[512,512,1,1,1],\"dimensions\":[1920,1080,3,1,1]}";

    var gpa = heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var da = try DatasetAttributes([]const u8).init(allocator, attr);

    try expect(da.dataType == DataType.uint8);
    const expected_dim = [_]u64{ 1920, 1080, 3, 1, 1 };
    for (expected_dim, 0..) |dim, i| {
        try expect(da.dimensions[i] == dim);
    }
    const expected_block_size = [_]u64{ 512, 512, 1, 1, 1 };
    for (expected_block_size, 0..) |block, i| {
        try expect(da.blockSize[i] == block);
    }
    try expect(da.compression.type == CompressionType.lz4);
    try expect(da.compression.useZlib == false);
    try expect(da.compression.blockSize == 65536);
    try expect(da.compression.level == 0);
    da.deinit();
    const check = gpa.deinit();
    try expect(check != .leak);
}
