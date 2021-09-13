const std = @import("std");
pub const pkgs = struct {
    pub fn addAllTo(artifact: *std.build.LibExeObjStep) void {
        @setEvalBranchQuota(1_000_000);
        inline for (std.meta.declarations(pkgs)) |decl| {
            if (decl.is_pub and decl.data == .Var) {
                artifact.addPackage(@field(pkgs, decl.name));
            }
        }
    }
};

pub const exports = struct {
    pub const IguaN5Builder = std.build.Pkg{
        .name = "IguaN5Builder",
        .path = .{ .path = "src/builder.zig" },
        .dependencies = &.{
        },
    };
    pub const iguaN5 = std.build.Pkg{
        .name = "iguaN5",
        .path = .{ .path = "iguan5.zig" },
        .dependencies = &.{
        },
    };
};
pub const base_dirs = struct {
};
