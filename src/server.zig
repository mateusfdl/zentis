//! TCP server implementation with non-blocking I/O and event loop.
//!
//! This module implements an event-driven server using poll() for scalability.
//! It handles multiple concurrent connections without threads or async.

const std = @import("std");
const logger = @import("logger.zig");
const net = @import("net.zig");

const MAX_CONNECTIONS = 1024;

/// TCP server with an event loop.
/// It owns the listening socket and manages all active connections.
pub const Server = struct {
    allocator: std.mem.Allocator,
    listener: std.net.Server,
    address: std.net.Address,

    /// Map from file descriptor to connection
    connections: std.AutoHashMap(std.posix.fd_t, net.Connection),

    shutdown: bool,

    pub fn init(allocator: std.mem.Allocator, address_str: []const u8) !Server {
        const listener = try net.createServer(address_str);

        logger.info("Server initialized on {s}", .{address_str});

        return Server{
            .allocator = allocator,
            .listener = listener,
            .address = listener.listen_address,
            .connections = std.AutoHashMap(std.posix.fd_t, net.Connection).init(allocator),
            .shutdown = false,
        };
    }

    /// Start the server event loop.
    /// This function blocks until the server is stopped.
    pub fn run(self: *Server) !void {
        var addr_ctx = logger.NetCtx.init(self.address);

        logger.info("Server listening on {s}", .{addr_ctx.str()});

        while (!self.shutdown or self.connections.count() > 0) {
            try self.eventLoop();
        }

        logger.info("Event loop exited", .{});
    }

    /// Single iteration of the event loop using poll().
    fn eventLoop(self: *Server) !void {
        // Allocate space for listener + all connections
        var pollfds = try self.allocator.alloc(std.posix.pollfd, MAX_CONNECTIONS + 1);
        defer self.allocator.free(pollfds);

        var nfds: usize = 0;

        // Add listener socket if not shutting down
        if (!self.shutdown) {
            pollfds[nfds] = .{
                .fd = self.listener.stream.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            };
            nfds += 1;
        }

        // Add all connection sockets
        var conn_iter = self.connections.iterator();
        while (conn_iter.next()) |entry| {
            const conn = entry.value_ptr;
            if (conn.closed) continue;

            var events: i16 = std.posix.POLL.IN;

            // Only register for POLLOUT if we have data to write
            if (conn.hasDataToWrite()) {
                events |= std.posix.POLL.OUT;
            }

            pollfds[nfds] = .{
                .fd = conn.fd,
                .events = events,
                .revents = 0,
            };
            nfds += 1;
        }

        // Wait for events (timeout: 1 second to check shutdown flag)
        const ready = std.posix.poll(pollfds[0..nfds], 1000) catch |err| {
            // EINTR means poll() was interrupted by a signal (e.g., SIGINT/SIGTERM)
            // This is expected during shutdown, so just continue the loop
            if (err == error.Interrupted) {
                return;
            }
            logger.err("poll() failed: {}", .{err});
            return err;
        };

        if (ready == 0) {
            // Timeout - no events
            return;
        }

        for (pollfds[0..nfds]) |pollfd| {
            if (pollfd.revents == 0) continue;

            // Check if this is the listener socket
            if (!self.shutdown and pollfd.fd == self.listener.stream.handle) {
                if (pollfd.revents & std.posix.POLL.IN != 0) {
                    self.acceptConnection() catch |err| {
                        logger.err("Failed to accept connection: {}", .{err});
                    };
                }
            } else {
                // This is a client connection
                if (pollfd.revents & std.posix.POLL.IN != 0) {
                    self.handleRead(pollfd.fd) catch |err| {
                        logger.debug("Error reading from fd {}: {}", .{pollfd.fd, err});
                        self.closeConnection(pollfd.fd);
                    };
                }

                if (pollfd.revents & std.posix.POLL.OUT != 0) {
                    self.handleWrite(pollfd.fd) catch |err| {
                        logger.debug("Error writing to fd {}: {}", .{pollfd.fd, err});
                        self.closeConnection(pollfd.fd);
                    };
                }

                // Check for errors or hangup
                if (pollfd.revents & (std.posix.POLL.ERR | std.posix.POLL.HUP) != 0) {
                    logger.debug("Connection fd {} error/hangup", .{pollfd.fd});
                    self.closeConnection(pollfd.fd);
                }
            }
        }
    }

    fn acceptConnection(self: *Server) !void {
        const accepted = self.listener.accept() catch |err| {
            // WouldBlock means no connection is ready (shouldn't happen after poll)
            if (err == error.WouldBlock) {
                return;
            }
            return err;
        };

        // Create a Connection with buffers
        const conn = try net.Connection.init(self.allocator, accepted.stream.handle, accepted.address);

        try self.connections.put(conn.fd, conn);

        var peer_ctx = logger.NetCtx.init(conn.peer_address);
        logger.info("New connection from {s} (fd {})", .{peer_ctx.str(), conn.fd});
    }

    fn handleRead(self: *Server, fd: std.posix.fd_t) !void {
        var conn = self.connections.getPtr(fd) orelse return error.ConnectionNotFound;

        const bytes_read = std.posix.read(conn.fd, &conn.read_buf) catch |err| {
            // WouldBlock means no data available (shouldn't happen after poll)
            if (err == error.WouldBlock) {
                return;
            }
            return err;
        };

        if (bytes_read == 0) {
            logger.debug("Client closed connection (fd {})", .{fd});
            self.closeConnection(fd);
            return;
        }

        var peer_ctx = logger.NetCtx.init(conn.peer_address);
        logger.debug("Read {} bytes from {s}", .{bytes_read, peer_ctx.str()});

        const written = conn.write_buf.write(conn.read_buf[0..bytes_read]);
        if (written < bytes_read) {
            logger.warn("Write buffer full, dropped {} bytes", .{bytes_read - written});
        }
    }

    /// Handle a write event on a connection.
    fn handleWrite(self: *Server, fd: std.posix.fd_t) !void {
        var conn = self.connections.getPtr(fd) orelse return error.ConnectionNotFound;

        const data = conn.write_buf.peekReadable();
        if (data.len == 0) {
            return; // Nothing to write
        }

        const bytes_written = std.posix.write(conn.fd, data) catch |err| {
            // WouldBlock means socket not ready (shouldn't happen after poll)
            if (err == error.WouldBlock) {
                return;
            }
            return err;
        };

        conn.write_buf.consume(bytes_written);

        var peer_ctx = logger.NetCtx.init(conn.peer_address);
        logger.debug("Wrote {} bytes to {s}", .{bytes_written, peer_ctx.str()});
    }

    fn closeConnection(self: *Server, fd: std.posix.fd_t) void {
        if (self.connections.fetchRemove(fd)) |entry| {
            var conn = entry.value;
            var peer_ctx = logger.NetCtx.init(conn.peer_address);
            logger.info("Connection from {s} closed (fd {})", .{peer_ctx.str(), fd});
            conn.deinit();
        }
    }

    pub fn beginShutdown(self: *Server) void {
        if (!self.shutdown) {
            logger.info("Beginning graceful shutdown", .{});
            self.shutdown = true;
        }
    }

    pub fn deinit(self: *Server) void {
        logger.info("Shutting down server", .{});

        var conn_iter = self.connections.iterator();
        while (conn_iter.next()) |entry| {
            var conn = entry.value_ptr;
            conn.deinit();
        }
        self.connections.deinit();

        self.listener.deinit();
    }
};

/// Global server instance for signal handling
var global_server: ?*Server = null;

/// Signal handler for SIGINT (Ctrl+C) and SIGTERM (kill command)
fn handleSignal(sig: c_int) callconv(.c) void {
    _ = sig;
    if (global_server) |server| {
        server.beginShutdown();
    }
}

/// Helper function to run a server with proper cleanup and signal handling.
/// This is useful for main.zig to keep the code simple.
pub fn run(allocator: std.mem.Allocator, address: []const u8) !void {
    var server = try Server.init(allocator, address);
    defer server.deinit();

    // Set up signal handler for graceful shutdown
    global_server = &server;
    const sig_action = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.mem.zeroes(std.posix.sigset_t),
        .flags = 0,
    };
    // Handle both SIGINT (Ctrl+C) and SIGTERM (kill command)
    _ = std.posix.sigaction(std.posix.SIG.INT, &sig_action, null);
    _ = std.posix.sigaction(std.posix.SIG.TERM, &sig_action, null);

    try server.run();

    global_server = null;
}
