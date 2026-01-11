//! Zentis - A real-time pub/sub server (Pusher-like) implemented in Zig.
//!
//! This is the entry point for the application.
//! It owns process startup, initialization, and graceful shutdown.

const std = @import("std");
const logger = @import("logger.zig");
const server = @import("server.zig");

const DEFAULT_ADDRESS = "127.0.0.1:9000";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{
        .safety = true,
        .verbose_log = false,
    }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            logger.err("Memory leak detected!", .{});
        }
    }

    const allocator = gpa.allocator();

    logger.info("Starting Zentis server", .{});

    server.run(allocator, DEFAULT_ADDRESS) catch |err| {
        logger.err("Server error: {}", .{err});
        std.posix.exit(1);
    };

    logger.info("Server stopped cleanly", .{});
}
