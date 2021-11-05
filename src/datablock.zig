const std = @import("std");
const fmt = std.fmt;
const fs = std.fs;
const io = std.io;
const os = std.os;
const path = std.fs.path;
const Allocator = std.mem.Allocator;
const gzip = std.compress.gzip;
const DatasetAttributes = @import("dataset_attributes.zig").DatasetAttributes;
const CompressionType = @import("dataset_attributes.zig").CompressionType;
const c = @cImport({
    @cDefine("LZ4LIB_API", {});
    @cInclude("lz4.h");
});

pub fn Datablock(comptime ReaderType: type) type {
    return struct {
        allocator: *Allocator,
        source: ReaderType,
        attributes: ?DatasetAttributes(ReaderType),
        datasetPath: []const u8,
        size: []u32,
        elementsNo: u32,
        gridPosition: []i64,
        len: usize,

        const Self = @This();

        pub fn init(
            a: *Allocator,
            source: ReaderType,
            datasetPath: []const u8,
            gridPosition: []i64,
            attributes: ?DatasetAttributes(ReaderType),
        ) !Self {
            var d_block = Self{
                .allocator = a,
                .source = source,
                .attributes = attributes,
                .datasetPath = datasetPath,
                .gridPosition = gridPosition,
                .size = undefined,
                .elementsNo = undefined,
                .len = 0,
            };
            try d_block.initChunk();
            return d_block;
        }

        pub fn deinit(self: *Self) void {
            self.source.close();
            self.allocator.free(self.size);
            if (self.attributes) |*attr| {
                attr.*.deinit();
            }
        }

        pub const Writer = io.Writer(*Self, anyerror, write);

        pub fn write(self: *Self, bytes: []const u8) !usize {
            _ = self;
            _ = bytes;
            return 0;
        }

        pub fn writer(self: *Self) Writer {
            return .{ .context = self };
        }

        pub const Reader = io.Reader(*Self, anyerror, read);

        pub fn read(self: *Self, buffer: []u8) !usize {
            if (self.attributes) |attr| {
                switch (attr.compression.type) {
                    CompressionType.raw => {
                        return self.source.read(buffer);
                    },
                    CompressionType.gzip => {
                        var gzip_reader = try gzip.gzipStream(self.allocator, self.source.reader());
                        return gzip_reader.read(buffer);
                    },
                    CompressionType.bzip2 => unreachable,
                    CompressionType.blosc => unreachable,
                    CompressionType.lz4 => {
                        var current_byte: usize = 0;
                        var decompressed: c_int = 0;

                        while (true) {
                            // each lz4 block is preceeded by
                            // 'Lz4Block' (8 bytes) + 1 byte token
                            var s = self.seeker();
                            try s.seekBy(9);

                            var r = self.source.reader();

                            // compressedLength(4 bytes)
                            // decompressedLength(4 bytes)
                            // checksum 4 bytes
                            var comp_size = try r.readIntLittle(i32);
                            var decomp_size = try r.readIntLittle(i32);
                            // var checksum = try r.readIntLittle(i32);
                            try s.seekBy(4);
                            if (decomp_size == 0) {
                                break;
                            }

                            var comp_buf = try self.allocator.alloc(u8, @intCast(usize, comp_size));
                            defer self.allocator.free(comp_buf);
                            _ = try r.read(comp_buf);

                            var res = c.LZ4_decompress_safe(
                                comp_buf.ptr,
                                buffer.ptr + current_byte,
                                @intCast(c_int, comp_size),
                                @intCast(c_int, decomp_size),
                            );

                            if (res < 0) {
                                return error.LZ4DecompressionError;
                            }
                            decompressed += res;
                            current_byte += @intCast(usize, decomp_size);
                        }
                        return @intCast(usize, decompressed);
                    },
                    CompressionType.xz => unreachable,
                }
            } else return @as(usize, 0);
        }

        pub fn reader(self: *Self) Reader {
            return .{ .context = self };
        }

        pub const SeekableStream = io.SeekableStream(
            *Self,
            anyerror,
            anyerror,
            seekTo,
            seekBy,
            getPos,
            getEndPos,
        );

        pub fn seekTo(self: *Self, offset: u64) !void {
            return self.source.seekTo(offset);
        }

        pub fn seekBy(self: *Self, offset: i64) !void {
            return self.source.seekBy(offset);
        }

        pub fn getPos(self: *Self) !u64 {
            return self.source.getPos();
        }

        pub fn getEndPos(self: *Self) !u64 {
            return self.source.getEndPos();
        }

        pub fn seeker(self: *Self) SeekableStream {
            return .{ .context = self };
        }

        fn initChunk(self: *Self) !void {
            self.attributes = try DatasetAttributes(ReaderType).init(self.allocator, self.datasetPath);
            var r = self.source.reader();
            // fail silently in case the source is to be written.
            var mode = r.readIntBig(u16) catch return;
            var block_size: []u32 = undefined;
            var elements_no: u32 = undefined;

            // The mode defines the content of the header of the datablock:
            //
            // - 0: This is the default mode, in which the encoded mode is followed by
            //      the number of dimensions (uint16 big endian) and the size of each
            //      dimension (uint32 big endian for each dimension).
            // - 1: The encoded mode is followed by the number of dimensions (uint16 big endian),
            //      the size of each dimension (uint32 big endian for each dimension), and
            //      finally the total number of elements in the datablock (uint32 big endian).
            // - 2: The encoded mode is only followed by the total number of elements in the
            //      datablock (uint32 big endian). This mode is not documented on the original
            //      repository, but from the java implementation we can see that this mode is
            //      used for data of type OBJECT.
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

            var len: u32 = 1;
            for (block_size) |dim_size| {
                if (dim_size > 0) {
                    len *= dim_size;
                }
            }

            self.len = @intCast(usize, len);
            self.elementsNo = elements_no;
            self.size = block_size;
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

        try std.testing.expect(n == t.expected);
    }
}
