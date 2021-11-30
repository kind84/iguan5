const std = @import("std");
const builtin = @import("builtin");

pub fn pathBufferSize() u32 {
    if (builtin.os.tag == .windows) {
        return 98302;
    }
    return std.fs.MAX_PATH_BYTES;
}
