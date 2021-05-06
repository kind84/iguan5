const std = @import("std");
const fs = std.fs;
const io = std.io;
const os = std.os;
const Allocator = std.mem.Allocator;
const gzip = std.compress.gzip;
const DatasetAttributes = @import("dataset_attributes.zig").DatasetAttributes;
const CompressionType = @import("dataset_attributes.zig").CompressionType;

const chunkInfo = struct {
    blockSize: []u32,
    elementsNo: u32,
};

pub fn datablock(reader: anytype) !Datablock(@TypeOf(reader)) {
    return Datablock(@TypeOf(reader));
}

pub fn Datablock(comptime ReaderType: type) type {
    return struct {
        allocator: *Allocator,
        source: ReaderType,
        attributes: DatasetAttributes,
        size: []u32,
        elementsNo: u32,
        gridPosition: []i64,

        const Self = @This();

        pub fn init(a: *Allocator, source: ReaderType, attributes: DatasetAttributes, gridPosition: []i64) !Self {
            var d_block = Self{
                .allocator = a,
                .source = source,
                .attributes = attributes,
                .gridPosition = gridPosition,
                .size = undefined,
                .elementsNo = undefined,
            };
            var info = try d_block.initChunk();
            d_block.elementsNo = info.elementsNo;
            d_block.size = info.blockSize;
            return d_block;
        }

        pub fn deinit(self: *Self) void {
            self.source.close();
            self.allocator.free(self.size);
        }

        pub const Reader = io.Reader(*Self, anyerror, read);

        pub fn read(self: *Self, buffer: []u8) !usize {
            switch (self.attributes.compression.type) {
                CompressionType.raw => {
                    return self.source.read(buffer);
                },
                CompressionType.gzip => {
                    var gzip_reader = try gzip.gzipStream(self.allocator, self.source.reader());
                    return gzip_reader.read(buffer);
                },
                CompressionType.bzip2 => unreachable,
                CompressionType.blosc => unreachable,
                CompressionType.lz4 => unreachable,
                CompressionType.xz => unreachable,
            }
        }

        pub fn reader(self: *Self) Reader {
            return .{ .context = self };
        }

        fn initChunk(self: *Self) !chunkInfo {
            var r = self.source.reader();
            var mode = try r.readIntBig(u16);
            var block_size: []u32 = undefined;
            var elements_no: u32 = undefined;

            if (mode != 2) {
                var dim_no = try r.readIntBig(u16);

                block_size = try self.allocator.alloc(u32, dim_no);
                var i: u16 = 0;
                while (i < dim_no) : (i += 1) {
                    var dim_size = try r.readIntBig(u32);
                    block_size[i] = dim_size;
                }

                if (mode == 0) {
                    elements_no = totalElements(block_size);
                } else {
                    // mode == 1
                    elements_no = try r.readIntBig(u32);
                }
            } else {
                // mode == 2
                elements_no = try r.readIntBig(u32);
            }

            return chunkInfo{
                .blockSize = block_size,
                .elementsNo = elements_no,
            };
        }
    };
}

fn totalElements(dimensions: []u32) u32 {
    if (dimensions.len == 0) return 0;
    var n: u32 = 1;
    for (dimensions) |d| {
        n *= d;
    }
    return n;
}

test "totalElements" {
    var dim0 = [_]u32{};
    var dim1 = [_]u32{ 1, 2, 3 };
    var tests = [_]struct {
        dimensions: []u32,
        expected: u32,
    }{
        .{
            .dimensions = &dim1,
            .expected = 6,
        },
        .{
            .dimensions = &dim0,
            .expected = 0,
        },
    };

    for (tests) |t| {
        var n = totalElements(t.dimensions);

        std.testing.expect(n == t.expected);
    }
}
