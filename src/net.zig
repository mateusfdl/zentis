//! Networking helpers and OS abstraction layer.
//!
//! This module isolates OS-specific networking logic and provides
//! small, focused helpers for TCP operations.

const std = @import("std");
const logger = @import("logger.zig");
const FastQueue = @import("fast_queue.zig").FastQueue;

/// Size of the read buffer for each connection.
/// Kept small for simplicity; can be increased later.
pub const READ_BUFFER_SIZE = 4096;

/// Size of the write buffer for each connection.
pub const WRITE_BUFFER_SIZE = 8192;

/// Parse an address string like "127.0.0.1:9000" into a std.net.Address.
/// Returns an error if the string is malformed or the address is invalid.
pub fn parseAddress(allocator: std.mem.Allocator, str: []const u8) !std.net.Address {
    _ = allocator; // Currently unused, kept for future expansion

    // parseIpAndPort handles IPv4 and IPv6 addresses with port (e.g., "127.0.0.1:9000")
    return std.net.Address.parseIpAndPort(str) catch |err| {
        logger.err("Failed to parse address '{s}': {}", .{ str, err });
        return error.InvalidAddress;
    };
}

/// Set a file descriptor to non-blocking mode.
pub fn setNonBlocking(fd: std.posix.fd_t) !void {
    const flags = try std.posix.fcntl(fd, std.posix.F.GETFL, 0);
    // O_NONBLOCK is 0x0004 on both Linux and macOS
    const O_NONBLOCK: u32 = 0x0004;
    _ = try std.posix.fcntl(fd, std.posix.F.SETFL, flags | O_NONBLOCK);
}

/// Create a listening TCP server bound to the given address.
/// The server socket is set to non-blocking mode.
pub fn createServer(address_str: []const u8) !std.net.Server {
    const address = try parseAddress(std.heap.page_allocator, address_str);

    const server = try address.listen(.{
        .reuse_address = true,
        .kernel_backlog = 128, // Pending connections queue size
    });

    try setNonBlocking(server.stream.handle);

    return server;
}

/// Connection represents a client connection with buffering for non-blocking I/O.
/// It tracks the file descriptor, peer address, read/write buffers, and connection state.
pub const Connection = struct {
    /// File descriptor for this connection
    fd: std.posix.fd_t,

    /// Peer address for logging
    peer_address: std.net.Address,

    /// Fixed-size read buffer
    read_buf: [READ_BUFFER_SIZE]u8,

    /// Ring buffer for write data (handles partial writes)
    write_buf: FastQueue,

    closed: bool,

    /// Initialize a new connection from an accepted socket.
    pub fn init(allocator: std.mem.Allocator, fd: std.posix.fd_t, peer_address: std.net.Address) !Connection {
        // Set the client socket to non-blocking mode
        try setNonBlocking(fd);

        return Connection{
            .fd = fd,
            .peer_address = peer_address,
            .read_buf = undefined,
            .write_buf = try FastQueue.init(allocator, WRITE_BUFFER_SIZE),
            .closed = false,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.write_buf.deinit();
        if (!self.closed) {
            self.close();
        }
    }

    pub fn close(self: *Connection) void {
        if (!self.closed) {
            std.posix.close(self.fd);
            self.closed = true;
        }
    }

    /// Returns true if there is data waiting to be written.
    pub fn hasDataToWrite(self: *const Connection) bool {
        return self.write_buf.available() > 0;
    }
};
