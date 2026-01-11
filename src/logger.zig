//! Simple logging wrapper for Zentis.
//!
//! This module provides a clean, ergonomic API over std.log
//! with context helpers for common operations like address formatting.
//! No unnecessary abstractions - just small wrappers for readability.

const std = @import("std");

pub const Level = enum {
    debug,
    info,
    warn,
    err,
};

/// format a network address into a fixed buffer as "ip:port".
/// returns the formatted string slice.
pub fn formatAddress(address: std.net.Address, buf: []u8) []const u8 {
    if (address.any.family == std.posix.AF.INET) {
        const ip_bytes = @as(*const [4]u8, @ptrCast(&address.in.sa.addr));
        const port = std.mem.bigToNative(u16, address.in.sa.port);
        return std.fmt.bufPrint(buf, "{}.{}.{}.{}:{}", .{
            ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3], port,
        }) catch "unknown";
    }
    return std.fmt.bufPrint(buf, "{any}", .{address}) catch "unknown";
}

pub fn debug(comptime fmt: []const u8, args: anytype) void {
    std.log.debug(fmt, args);
}

pub fn info(comptime fmt: []const u8, args: anytype) void {
    std.log.info(fmt, args);
}

pub fn warn(comptime fmt: []const u8, args: anytype) void {
    std.log.warn(fmt, args);
}

pub fn err(comptime fmt: []const u8, args: anytype) void {
    std.log.err(fmt, args);
}

/// context-aware logger for network operations.
/// adds automatic address formatting to reduce repetition.
pub const NetCtx = struct {
    address: std.net.Address,
    addr_buf: [32]u8 = undefined,

    pub fn init(address: std.net.Address) NetCtx {
        return .{ .address = address };
    }

    pub fn str(self: *NetCtx) []const u8 {
        return formatAddress(self.address, &self.addr_buf);
    }

    /// get the formatted address string (const version).
    pub fn strConst(self: NetCtx) []const u8 {
        var addr_buf: [32]u8 = undefined;
        return formatAddress(self.address, &addr_buf);
    }

    pub fn debug(self: *NetCtx, comptime fmt: []const u8, args: anytype) void {
        std.log.debug("[{s}] " ++ fmt, .{self.str()} ++ args);
    }

    pub fn info(self: *NetCtx, comptime fmt: []const u8, args: anytype) void {
        std.log.info("[{s}] " ++ fmt, .{self.str()} ++ args);
    }

    pub fn warn(self: *NetCtx, comptime fmt: []const u8, args: anytype) void {
        std.log.warn("[{s}] " ++ fmt, .{self.str()} ++ args);
    }

    pub fn err(self: *NetCtx, comptime fmt: []const u8, args: anytype) void {
        std.log.err("[{s}] " ++ fmt, .{self.str()} ++ args);
    }
};
