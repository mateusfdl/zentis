//! Simple logging wrapper for Zentis.
//!
//! This module provides a clean, ergonomic API over std.log
//! with context helpers for common operations like address formatting.
//! No unnecessary abstractions - just small wrappers for readability.

const std = @import("std");

/// Log levels supported by the logger.
pub const Level = enum {
    debug,
    info,
    warn,
    err,
};

/// Format a network address into a fixed buffer as "IP:PORT".
/// Returns the formatted string slice.
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

/// Log a debug message.
/// Debug messages are only shown in debug builds.
pub fn debug(comptime fmt: []const u8, args: anytype) void {
    std.log.debug(fmt, args);
}

/// Log an info message.
/// Info messages show the normal operation of the server.
pub fn info(comptime fmt: []const u8, args: anytype) void {
    std.log.info(fmt, args);
}

/// Log a warning message.
/// Warnings indicate something unexpected but non-fatal.
pub fn warn(comptime fmt: []const u8, args: anytype) void {
    std.log.warn(fmt, args);
}

/// Log an error message.
/// Errors indicate failures that need attention.
pub fn err(comptime fmt: []const u8, args: anytype) void {
    std.log.err(fmt, args);
}

/// Context-aware logger for network operations.
/// Adds automatic address formatting to reduce repetition.
pub const NetCtx = struct {
    address: std.net.Address,
    addr_buf: [32]u8 = undefined,

    /// Initialize a network context from an address.
    pub fn init(address: std.net.Address) NetCtx {
        return .{ .address = address };
    }

    /// Get the formatted address string.
    pub fn str(self: *NetCtx) []const u8 {
        return formatAddress(self.address, &self.addr_buf);
    }

    /// Log a debug message with the address prepended.
    pub fn debug(self: *NetCtx, comptime fmt: []const u8, args: anytype) void {
        std.log.debug("[{s}] " ++ fmt, .{self.str()} ++ args);
    }

    /// Log an info message with the address prepended.
    pub fn info(self: *NetCtx, comptime fmt: []const u8, args: anytype) void {
        std.log.info("[{s}] " ++ fmt, .{self.str()} ++ args);
    }

    /// Log a warning message with the address prepended.
    pub fn warn(self: *NetCtx, comptime fmt: []const u8, args: anytype) void {
        std.log.warn("[{s}] " ++ fmt, .{self.str()} ++ args);
    }

    /// Log an error message with the address prepended.
    pub fn err(self: *NetCtx, comptime fmt: []const u8, args: anytype) void {
        std.log.err("[{s}] " ++ fmt, .{self.str()} ++ args);
    }
};
