pub const DatasetAttributes = struct {
    dimensions: []u64,
    blockSize: []u64,
    dataType: DataType,
    compression: Compression,
};

pub const Compression = struct {
    type: CompressionType,
    useZlib: bool,
    level: i32,
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
