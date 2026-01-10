//! Zentis - A real-time pub/sub server (Pusher-like) implemented in Zig.
//!
//! This is the entry point for the application.
//! It owns process startup, initialization, and graceful shutdown.

const std = @import("std");
const logger = @import("logger.zig");
const server = @import("server.zig");

/// Default address to listen on.
/// Using loopback for security during development.
const DEFAULT_ADDRESS = "127.0.0.1:9000";

/// Main entry point.
/// Initializes the allocator and starts the server.
pub fn main() !void {
    // Initialize the general-purpose allocator.
    // This allocator is used for all heap allocations in the application.
    // In a future version, we might use an arena allocator for better
    // control over memory lifetimes.
    var gpa = std.heap.GeneralPurposeAllocator(.{
        // Enable detailed memory reporting in debug mode
        .safety = true,
        .verbose_log = std.debug.runtime_safety,
    }){};
    defer {
        // Check for memory leaks on shutdown.
        // This will report any allocations that weren't freed.
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
