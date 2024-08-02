const std = @import("std");
const Allocator = std.mem.Allocator;
const gzip = std.compress.gzip;
const Fs = @import("Fs.zig");
const DatasetAttributes = @import("dataset_attributes.zig").DatasetAttributes;
const CompressionType = @import("dataset_attributes.zig").CompressionType;
const util = @import("util.zig");
const c = @cImport({
    @cDefine("LZ4LIB_API", {});
    @cInclude("lz4.h");
});

const lz4Magic = [_]u8{ 0x4C, 0x5A, 0x34, 0x42, 0x6C, 0x6F, 0x63, 0x6B, 0x26 }; // Lz4Block&

pub fn Datablock(comptime SourceType: type) type {
    return struct {
        allocator: Allocator,
        source: ?SourceType,
        stream: std.io.StreamSource,
        attributes: ?DatasetAttributes(SourceType),
        datasetPath: []const u8,
        size: []u32,
        elementsNo: u32,
        gridPosition: []i64,
        len: usize,
        mode: u2,

        const Self = @This();

        pub fn init(
            a: Allocator,
            source: ?SourceType,
            datasetPath: []const u8,
            gridPosition: []i64,
            attributes: ?DatasetAttributes(SourceType),
        ) !Self {
            var d_block = Self{
                .allocator = a,
                .source = source,
                .stream = if (source) |s| blk: {
                    switch (SourceType) {
                        std.fs.File => break :blk std.io.StreamSource{ .file = s },
                        []u8 => break :blk std.io.StreamSource{ .buffer = std.io.fixedBufferStream(s) },
                        []const u8 => break :blk std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(s) },
                        else => unreachable,
                    }
                } else undefined,
                .attributes = attributes,
                .datasetPath = datasetPath,
                .gridPosition = gridPosition,
                .size = &[_]u32{},
                .elementsNo = undefined,
                .len = 0,
                .mode = undefined,
            };
            if (source) |_| {
                try d_block.initChunk();
            }
            return d_block;
        }

        pub fn deinit(self: *Self) void {
            switch (SourceType) {
                std.fs.File => self.source.?.close(),
                []u8 => {
                    if (self.source) |s| self.allocator.free(s);
                },
                []const u8 => {},
                else => unreachable,
            }
            if (self.size.len > 0) {
                self.allocator.free(self.size);
            }
        }

        pub const Writer = std.io.Writer(*Self, anyerror, write);

        pub fn write(self: *Self, bytes: []const u8) !usize {
            // if source is slice, set up an arraylist to have a writer.
            var array_list: std.ArrayList(u8) = undefined;
            var w = switch (SourceType) {
                std.fs.File, []const u8 => self.stream.writer(),
                []u8 => blk: {
                    array_list = std.ArrayList(u8).init(self.allocator);
                    break :blk array_list.writer();
                },
                else => unreachable,
            };
            defer {
                switch (SourceType) {
                    std.fs.File, []const u8 => {},
                    []u8 => {
                        self.source = array_list.toOwnedSlice();
                        self.stream = std.io.StreamSource{ .buffer = std.io.fixedBufferStream(self.source.?) };
                    },
                    else => unreachable,
                }
            }

            // write the header
            const sizes = self.attributes.?.blockSize;
            const dims = self.attributes.?.dimensions;
            try w.writeIntBig(u16, @as(u16, self.mode));
            if (self.mode < 2) {
                try w.writeIntBig(u16, @as(u16, sizes.len));
                for (sizes) |s| {
                    try w.writeIntBig(u32, @as(u32, s));
                }
                if (self.mode == 1) {
                    try w.writeIntBig(u32, totalElements(u64, dims));
                }
            } else {
                try w.writeIntBig(u32, totalElements(u64, dims));
            }

            // now the data
            if (self.attributes) |attr| {
                switch (attr.compression.type) {
                    CompressionType.raw => {
                        const n = try w.write(bytes);
                        self.len = n;
                        return n;
                    },
                    CompressionType.gzip => unreachable,
                    CompressionType.bzip2 => unreachable,
                    CompressionType.blosc => unreachable,
                    CompressionType.lz4 => {
                        // TODO: use lz4 compress bound func to comput dest size
                        const dest_size_c = c.LZ4_compressBound(@as(c_int, bytes.len));
                        const dest_size = @as(usize, dest_size_c);
                        var dest_buf = try self.allocator.alloc(u8, dest_size);
                        defer self.allocator.free(dest_buf);

                        const comp_c = c.LZ4_compress_default(
                            bytes.ptr,
                            dest_buf.ptr,
                            @as(c_int, bytes.len),
                            dest_size_c,
                        );

                        // if compression failed use the original length
                        const comp: i32 = if (comp_c == 0) @as(i32, bytes.len) else @as(i32, comp_c);

                        // 21 bytes lz4 header:
                        // 9 bytes: magic + token (Lz4Block&)
                        // 4 bytes: compressed length
                        // 4 bytes: decompressed length
                        // 4 bytes: checksum
                        //
                        // // TODO: group in a buffer and write once
                        _ = try w.write(&lz4Magic);
                        _ = try w.writeIntLittle(i32, comp);
                        _ = try w.writeIntLittle(i32, @as(i32, bytes.len));
                        _ = try w.writeIntLittle(i32, 0x0000); // TODO compute checksum

                        const comp_buf = dest_buf[0..@as(usize, comp)];

                        _ = try w.write(comp_buf);

                        // signal end of lz4 block
                        _ = try w.write(&lz4Magic);
                        _ = try w.writeIntLittle(i32, comp);
                        _ = try w.writeIntLittle(i32, 0x0000);
                        _ = try w.writeIntLittle(i32, 0x0000); // TODO compute checksum

                        const compr_len = @as(usize, comp);
                        if (compr_len <= bytes.len) {
                            self.len = compr_len;
                        } else {
                            self.len = bytes.len;
                        }
                        return compr_len;
                    },
                    CompressionType.xz => unreachable,
                }
            } else return @as(usize, 0);
        }

        pub fn writer(self: *Self, mode: u2) Writer {
            self.mode = mode;
            return .{ .context = self };
        }

        pub const Reader = std.io.Reader(*Self, anyerror, read);

        pub fn read(self: *Self, buffer: []u8) !usize {
            if (self.attributes) |attr| {
                switch (attr.compression.type) {
                    CompressionType.raw => {
                        return self.stream.read(buffer);
                    },
                    CompressionType.gzip => {
                        var gzip_reader = try gzip.gzipStream(self.allocator, self.stream.reader());
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

                            var r = self.stream.reader();

                            // compressedLength(4 bytes)
                            // decompressedLength(4 bytes)
                            // checksum 4 bytes
                            const comp_size = try r.readIntLittle(i32);
                            const decomp_size = try r.readIntLittle(i32);
                            // var checksum = try r.readIntLittle(i32);
                            try s.seekBy(4);
                            if (decomp_size == 0) {
                                break;
                            }

                            const comp_buf = try self.allocator.alloc(u8, @as(usize, comp_size));
                            defer self.allocator.free(comp_buf);
                            _ = try r.read(comp_buf);

                            const res = c.LZ4_decompress_safe(
                                comp_buf.ptr,
                                buffer.ptr + current_byte,
                                @as(c_int, comp_size),
                                @as(c_int, decomp_size),
                            );

                            if (res < 0) {
                                return error.LZ4DecompressionError;
                            }
                            decompressed += res;
                            current_byte += @as(usize, decomp_size);
                        }
                        return @as(usize, decompressed);
                    },
                    CompressionType.xz => unreachable,
                }
            } else return @as(usize, 0);
        }

        pub fn reader(self: *Self) Reader {
            return .{ .context = self };
        }

        pub const SeekableStream = std.io.SeekableStream(
            *Self,
            anyerror,
            anyerror,
            seekTo,
            seekBy,
            getPos,
            getEndPos,
        );

        pub fn seekTo(self: *Self, offset: u64) !void {
            return self.stream.seekTo(offset);
        }

        pub fn seekBy(self: *Self, offset: i64) !void {
            return self.stream.seekBy(offset);
        }

        pub fn getPos(self: *Self) !u64 {
            return self.stream.getPos();
        }

        pub fn getEndPos(self: *Self) !u64 {
            return self.stream.getEndPos();
        }

        pub fn seeker(self: *Self) SeekableStream {
            return .{ .context = self };
        }

        fn initChunk(self: *Self) !void {
            // early return in case the file is empty
            switch (SourceType) {
                []const u8, std.fs.File => {
                    const size = try self.seeker().getEndPos();
                    if (size == 0) return;
                },
                []u8 => {
                    if (self.source.?.len == 0) return;
                },
                else => unreachable,
            }

            var r = self.stream.reader();
            const mode = try r.readIntBig(u16);
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
            if (mode < 2) {
                const dim_no = try r.readIntBig(u16);

                block_size = try self.allocator.alloc(u32, dim_no);
                var i: u16 = 0;
                while (i < dim_no) : (i += 1) {
                    const dim_size = try r.readIntBig(u32);
                    block_size[i] = dim_size;
                }

                if (mode == 0) {
                    elements_no = totalElements(u32, block_size);
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

            self.len = @as(usize, len);
            self.elementsNo = elements_no;
            self.size = block_size;
        }
    };
}

test "raw file" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const buff_size = util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    const full_path = try std.fs.realpath("testdata/lynx_raw", &path_buffer);
    var fs = try Fs.init(allocator, full_path);

    const fs_path = try std.fs.path.join(allocator, &[_][]const u8{ full_path, "data.n5/0/0/0/0/0/0/1" });
    std.fs.deleteFileAbsolute(fs_path) catch {};

    var d_attr = try fs.datasetAttributes("0/0");

    var grid_position = [_]i64{ 0, 0, 0, 0, 1 };
    var d_block = try fs.getBlock("0/0", &grid_position, d_attr);

    const deadbeef = &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    _ = try d_block.writer(0).write(deadbeef);

    d_block.deinit();

    d_block = try fs.getBlock("0/0", &grid_position, d_attr);
    const out_buf = try allocator.alloc(u8, deadbeef.len);
    _ = try d_block.reader().read(out_buf);
    try std.testing.expect(std.mem.eql(u8, out_buf, deadbeef));

    allocator.free(out_buf);
    d_block.deinit();
    d_attr.deinit();
    fs.deinit();
    try std.fs.deleteFileAbsolute(fs_path);
    allocator.free(fs_path);
    try std.testing.expect(!gpa.deinit());
}

test "LZ4 file" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const buff_size = util.pathBufferSize();
    var path_buffer: [buff_size]u8 = undefined;
    const full_path = try std.fs.realpath("testdata/lynx_lz4", &path_buffer);
    var fs = try Fs.init(allocator, full_path);

    const fs_path = try std.fs.path.join(allocator, &[_][]const u8{ full_path, "data.n5/0/0/0/0/0/0/1" });
    std.fs.deleteFileAbsolute(fs_path) catch {};

    var d_attr = try fs.datasetAttributes("0/0");

    var grid_position = [_]i64{ 0, 0, 0, 0, 1 };
    var d_block = try fs.getBlock("0/0", &grid_position, d_attr);

    const deadbeef = &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    _ = try d_block.writer(0).write(deadbeef);

    d_block.deinit();

    d_block = try fs.getBlock("0/0", &grid_position, d_attr);
    const out_buf = try allocator.alloc(u8, deadbeef.len);
    _ = try d_block.reader().read(out_buf);
    try std.testing.expect(std.mem.eql(u8, out_buf, deadbeef));

    allocator.free(out_buf);
    d_block.deinit();
    d_attr.deinit();
    fs.deinit();
    try std.fs.deleteFileAbsolute(fs_path);
    allocator.free(fs_path);
    try std.testing.expect(!gpa.deinit());
}

test "raw bytes" {
    const attr = "{\"dataType\":\"uint8\",\"compression\":{\"type\":\"raw\"},\"blockSize\":[512,512,1,1,1],\"dimensions\":[1920,1080,3,1,1]}";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var allocator = gpa.allocator();

    var d_attr = try DatasetAttributes([]u8).init(allocator, attr);
    var d_block = try Datablock([]u8).init(allocator, null, &.{}, &.{}, d_attr);

    const deadbeef = &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    _ = try d_block.writer(0).write(deadbeef);

    const out_buf = try allocator.alloc(u8, d_block.len);
    try d_block.initChunk();
    _ = try d_block.reader().read(out_buf);
    try std.testing.expect(std.mem.eql(u8, out_buf, deadbeef));

    allocator.free(out_buf);
    d_block.deinit();
    d_attr.deinit();
    try std.testing.expect(!gpa.deinit());
}

test "LZ4 bytes" {
    const attr = "{\"dataType\":\"uint8\",\"compression\":{\"type\":\"lz4\",\"blockSize\":65536},\"blockSize\":[512,512,1,1,1],\"dimensions\":[1920,1080,3,1,1]}";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var allocator = gpa.allocator();

    var d_attr = try DatasetAttributes([]u8).init(allocator, attr);
    var d_block = try Datablock([]u8).init(allocator, null, &.{}, &.{}, d_attr);

    const deadbeef = &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    _ = try d_block.writer(0).write(deadbeef);

    const out_buf = try allocator.alloc(u8, d_block.len);
    try d_block.initChunk();
    _ = try d_block.reader().read(out_buf);
    std.debug.print("{s}\n", .{std.fmt.fmtSliceHexLower(out_buf)});
    try std.testing.expect(std.mem.eql(u8, out_buf, deadbeef));

    allocator.free(out_buf);
    d_block.deinit();
    d_attr.deinit();
    try std.testing.expect(!gpa.deinit());
}

fn totalElements(comptime T: type, dimensions: []T) u32 {
    if (dimensions.len == 0) return 0;
    var n: u32 = 1;
    for (dimensions) |d| {
        n *= @as(u32, d);
    }
    return n;
}

test "totalElements" {
    var dim0 = [_]u32{};
    var dim1 = [_]u32{ 1, 2, 3 };
    const tests = [_]struct {
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
        const n = totalElements(u32, t.dimensions);

        try std.testing.expect(n == t.expected);
    }
}
