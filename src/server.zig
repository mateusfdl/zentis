//! TCP server implementation with non-blocking I/O and event loop.
//!
//! This module implements an event-driven server using poll() for scalability.
//! It handles multiple concurrent connections without threads or async.

var global_server: ?*Server = null;

const std = @import("std");
const logger = @import("logger.zig");
const net = @import("net.zig");
const protocol = @import("protocol.zig");
const pubsub_mod = @import("pubsub.zig");
const client_mod = @import("client.zig");

const MAX_CONNECTIONS = 1024;

const ErrorCodes = enum(u8) {
    UNKNOWN_MESSAGE = 1,
    INVALID_PAYLOAD = 2,
    NOT_AUTHENTICATED = 3,
    CHANNEL_NOT_FOUND = 4,
    ALREADY_SUBSCRIBED = 5,
    NOT_SUBSCRIBED = 6,
    INTERNAL_ERROR = 7,
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    listener: std.net.Server,
    address: std.net.Address,
    connections: std.AutoHashMap(std.posix.fd_t, net.Connection),
    parsers: std.AutoHashMap(std.posix.fd_t, protocol.MessageParser),
    clients: std.AutoHashMap(std.posix.fd_t, client_mod.ClientState),
    pubsub: pubsub_mod.PubSub,
    msg_builder: protocol.MessageBuilder,

    shutdown: bool,

    pub fn init(allocator: std.mem.Allocator, address_str: []const u8) !Server {
        const listener = try net.createServer(address_str);

        logger.info("Server initialized on {s}", .{address_str});

        return Server{
            .allocator = allocator,
            .listener = listener,
            .address = listener.listen_address,
            .connections = std.AutoHashMap(std.posix.fd_t, net.Connection).init(allocator),
            .parsers = std.AutoHashMap(std.posix.fd_t, protocol.MessageParser).init(allocator),
            .clients = std.AutoHashMap(std.posix.fd_t, client_mod.ClientState).init(allocator),
            .pubsub = pubsub_mod.PubSub.init(allocator),
            .msg_builder = protocol.MessageBuilder.init(allocator),
            .shutdown = false,
        };
    }

    /// start event loop.
    pub fn run(self: *Server) !void {
        var addr_ctx = logger.NetCtx.init(self.address);

        logger.info("Server listening on {s}", .{addr_ctx.str()});

        while (!self.shutdown or self.connections.count() > 0) {
            try self.eventLoop();
        }

        logger.info("Event loop exited", .{});
    }

    /// single iteration of the event loop using poll().
    fn eventLoop(self: *Server) !void {
        var pollfds = try self.allocator.alloc(std.posix.pollfd, MAX_CONNECTIONS + 1);
        defer self.allocator.free(pollfds);

        var nfds: usize = 0;

        // add listener socket if not shutting down
        if (!self.shutdown) {
            pollfds[nfds] = .{
                .fd = self.listener.stream.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            };
            nfds += 1;
        }

        // add all connection sockets
        var conn_iter = self.connections.iterator();
        while (conn_iter.next()) |entry| {
            const conn = entry.value_ptr;
            if (conn.closed) continue;

            var events: i16 = std.posix.POLL.IN;

            // only register for pollout if we have data to write
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

        const ready = std.posix.poll(pollfds[0..nfds], 1000) catch |err| {
            // EINTR means poll() was interrupted by a signal (e.g., SIGINT/SIGTERM)
            if (err == error.Interrupted) {
                return;
            }
            logger.err("poll() failed: {}", .{err});
            return err;
        };

        if (ready == 0) {
            return;
        }

        for (pollfds[0..nfds]) |pollfd| {
            if (pollfd.revents == 0) continue;

            // check if this is the listener socket
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
                        logger.debug("Error reading from fd {}: {}", .{ pollfd.fd, err });
                        self.closeConnection(pollfd.fd);
                    };
                }

                if (pollfd.revents & std.posix.POLL.OUT != 0) {
                    self.handleWrite(pollfd.fd) catch |err| {
                        logger.debug("Error writing to fd {}: {}", .{ pollfd.fd, err });
                        self.closeConnection(pollfd.fd);
                    };
                }

                // check for errors or hangup
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

        const conn = try net.Connection.init(self.allocator, accepted.stream.handle, accepted.address);
        const parser = protocol.MessageParser.init(self.allocator);
        const client = client_mod.ClientState.init(self.allocator, conn.fd);

        try self.connections.put(conn.fd, conn);
        try self.parsers.put(conn.fd, parser);
        try self.clients.put(conn.fd, client);

        var peer_ctx = logger.NetCtx.init(conn.peer_address);
        logger.info("New connection from {s} (fd {})", .{ peer_ctx.str(), conn.fd });
    }

    fn handleRead(self: *Server, fd: std.posix.fd_t) !void {
        var conn = self.connections.getPtr(fd) orelse return error.ConnectionNotFound;
        var parser = self.parsers.getPtr(fd) orelse return error.ParserNotFound;

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
        logger.debug("Read {} bytes from {s}", .{ bytes_read, peer_ctx.str() });

        try parser.feed(conn.read_buf[0..bytes_read]);

        while (try parser.next()) |msg| {
            self.handleMessage(fd, msg) catch |err| {
                logger.err("Error handling message from fd {}: {}", .{ fd, err });
                if (err == error.UnknownMessageType or err == error.MessageTooLarge) {
                    self.sendError(fd, @intFromEnum(ErrorCodes.UNKNOWN_MESSAGE), "Protocol error") catch {};
                    self.closeConnection(fd);
                }
            };
            parser.consume(msg.total_len);
        }
    }

    fn handleWrite(self: *Server, fd: std.posix.fd_t) !void {
        var conn = self.connections.getPtr(fd) orelse return error.ConnectionNotFound;

        const data = conn.write_buf.peekReadable();
        if (data.len == 0) {
            return;
        }

        const bytes_written = std.posix.write(conn.fd, data) catch |err| {
            if (err == error.WouldBlock) {
                return;
            }
            return err;
        };

        conn.write_buf.consume(bytes_written);

        var peer_ctx = logger.NetCtx.init(conn.peer_address);
        logger.debug("Wrote {} bytes to {s}", .{ bytes_written, peer_ctx.str() });
    }

    fn handleMessage(self: *Server, fd: std.posix.fd_t, msg: protocol.Message) !void {
        logger.debug("Handling message type: {} (fd {})", .{ msg.msg_type, fd });

        switch (msg.msg_type) {
            .CONNECT => try self.handleConnect(fd, msg.payload),
            .SUBSCRIBE => try self.handleSubscribe(fd, msg.payload),
            .UNSUBSCRIBE => try self.handleUnsubscribe(fd, msg.payload),
            .PUBLISH => try self.handlePublish(fd, msg.payload),
            .PING => try self.sendPong(fd),
            else => {
                logger.warn("Unhandled message type: {} (fd {})", .{ msg.msg_type, fd });
                try self.sendError(fd, @intFromEnum(ErrorCodes.UNKNOWN_MESSAGE), "Unhandled message type");
            },
        }
    }

    /// Handle CONNECT message.
    fn handleConnect(self: *Server, fd: std.posix.fd_t, payload: []const u8) !void {
        var client = self.clients.getPtr(fd) orelse return error.ClientNotFound;

        if (client.authenticated) {
            try self.sendError(fd, @intFromEnum(ErrorCodes.ALREADY_SUBSCRIBED), "Already authenticated");
            return;
        }

        const connect = protocol.Message{ .msg_type = .CONNECT, .payload = payload, .total_len = 0 };
        const auth = connect.parseConnect() catch |err| {
            logger.err("Invalid CONNECT payload: {}", .{err});
            try self.sendError(fd, @intFromEnum(ErrorCodes.INVALID_PAYLOAD), "Invalid CONNECT format");
            return;
        };

        // For now, accept any auth token (no real authentication)
        try client.authenticate(auth.auth_token);

        // Send CONNECTED response with connection ID
        var conn_id_buf: [32]u8 = undefined;
        const conn_id = std.fmt.bufPrint(&conn_id_buf, "conn-{}", .{fd}) catch "unknown";
        try self.sendMessageTo(.CONNECTED, conn_id, fd);

        logger.info("Client fd {} authenticated with token '{s}'", .{ fd, auth.auth_token });
    }

    fn handleSubscribe(self: *Server, fd: std.posix.fd_t, payload: []const u8) !void {
        var client = self.clients.getPtr(fd) orelse return error.ClientNotFound;

        const msg = protocol.Message{ .msg_type = .SUBSCRIBE, .payload = payload, .total_len = 0 };
        const channel_name = msg.parseSubscribe() catch |err| {
            logger.err("Invalid SUBSCRIBE payload: {}", .{err});
            try self.sendError(fd, @intFromEnum(ErrorCodes.INVALID_PAYLOAD), "Invalid SUBSCRIBE format");
            return;
        };

        const newly_subscribed = try self.pubsub.subscribe(fd, channel_name);
        if (newly_subscribed) {
            try client.addSubscription(channel_name);
            try self.sendMessageTo(.SUBSCRIBED, channel_name, fd);
            logger.info("Client fd {} subscribed to channel '{s}'", .{ fd, channel_name });
        } else {
            logger.debug("Client fd {} already subscribed to '{s}'", .{ fd, channel_name });
        }
    }

    fn handleUnsubscribe(self: *Server, fd: std.posix.fd_t, payload: []const u8) !void {
        var client = self.clients.getPtr(fd) orelse return error.ClientNotFound;

        const msg = protocol.Message{ .msg_type = .UNSUBSCRIBE, .payload = payload, .total_len = 0 };
        const channel_name = msg.parseUnsubscribe() catch |err| {
            logger.err("Invalid UNSUBSCRIBE payload: {}", .{err});
            try self.sendError(fd, @intFromEnum(ErrorCodes.INVALID_PAYLOAD), "Invalid UNSUBSCRIBE format");
            return;
        };

        const was_subscribed = self.pubsub.unsubscribe(fd, channel_name);

        if (was_subscribed) {
            _ = client.removeSubscription(channel_name);
            try self.sendMessageTo(.UNSUBSCRIBED, channel_name, fd);
            logger.info("Client fd {} unsubscribed from channel '{s}'", .{ fd, channel_name });
        } else {
            try self.sendError(fd, @intFromEnum(ErrorCodes.NOT_SUBSCRIBED), "Not subscribed to channel");
        }
    }

    fn handlePublish(self: *Server, fd: std.posix.fd_t, payload: []const u8) !void {
        const msg = protocol.Message{ .msg_type = .PUBLISH, .payload = payload, .total_len = 0 };
        const publish = msg.parsePublish() catch |err| {
            logger.err("Invalid PUBLISH payload: {}", .{err});
            try self.sendError(fd, @intFromEnum(ErrorCodes.INVALID_PAYLOAD), "Invalid PUBLISH format");
            return;
        };

        const recipient_count = self.pubsub.publish(publish.channel, publish.data, fd);

        const subscribers = try self.pubsub.getSubscribers(self.allocator, publish.channel, fd);
        defer self.allocator.free(subscribers);

        for (subscribers) |sub_fd| {
            self.sendMessageToClient(.MESSAGE, publish.channel, publish.data, sub_fd) catch |err| {
                logger.err("Failed to send message to fd {}: {}", .{ sub_fd, err });
            };
        }

        logger.info("Client fd {} published to channel '{s}' ({} recipients)", .{ fd, publish.channel, recipient_count });
    }

    fn sendMessageToClient(
        self: *Server,
        msg_type: protocol.MessageType,
        channel: []const u8,
        data: []const u8,
        fd: std.posix.fd_t,
    ) !void {
        _ = msg_type;
        const msg_bytes = try self.msg_builder.buildChannelMessage(channel, data);
        defer self.msg_builder.allocator.free(msg_bytes);

        var conn = self.connections.getPtr(fd) orelse return error.ConnectionNotFound;

        const written = conn.write_buf.write(msg_bytes);
        if (written < msg_bytes.len) {
            logger.warn("Write buffer full for fd {}, dropped {} bytes", .{ fd, msg_bytes.len - written });
        }
    }

    /// send a string message to a specific client.
    fn sendMessageTo(self: *Server, msg_type: protocol.MessageType, str: []const u8, fd: std.posix.fd_t) !void {
        const msg_bytes = switch (msg_type) {
            .CONNECTED => try self.msg_builder.buildConnected(str),
            .SUBSCRIBED => try self.msg_builder.buildSubscribed(str),
            .UNSUBSCRIBED => try self.msg_builder.buildUnsubscribed(str),
            else => return error.UnsupportedMessageType,
        };
        defer self.msg_builder.allocator.free(msg_bytes);

        var conn = self.connections.getPtr(fd) orelse return error.ConnectionNotFound;

        const written = conn.write_buf.write(msg_bytes);
        if (written < msg_bytes.len) {
            logger.warn("Write buffer full for fd {}, dropped {} bytes", .{ fd, msg_bytes.len - written });
        }
    }

    fn sendPong(self: *Server, fd: std.posix.fd_t) !void {
        const msg_bytes = try self.msg_builder.buildPong();
        defer self.msg_builder.allocator.free(msg_bytes);

        var conn = self.connections.getPtr(fd) orelse return error.ConnectionNotFound;

        const written = conn.write_buf.write(msg_bytes);
        if (written < msg_bytes.len) {
            logger.warn("Write buffer full for fd {}, dropped {} bytes", .{ fd, msg_bytes.len - written });
        }
    }

    fn sendError(self: *Server, fd: std.posix.fd_t, error_code: u8, error_msg: []const u8) !void {
        const msg_bytes = try self.msg_builder.buildError(error_code, error_msg);
        defer self.msg_builder.allocator.free(msg_bytes);

        var conn = self.connections.getPtr(fd) orelse return error.ConnectionNotFound;

        const written = conn.write_buf.write(msg_bytes);
        if (written < msg_bytes.len) {
            logger.warn("Write buffer full for fd {}, dropped {} bytes", .{ fd, msg_bytes.len - written });
        }
    }

    fn closeConnection(self: *Server, fd: std.posix.fd_t) void {
        var peer_ctx_val: ?logger.NetCtx = null;

        if (self.connections.fetchRemove(fd)) |entry| {
            var conn = entry.value;
            peer_ctx_val = logger.NetCtx.init(conn.peer_address);
            conn.deinit();
        }

        if (self.parsers.fetchRemove(fd)) |entry| {
            var parser = entry.value;
            parser.deinit();
        }

        if (self.clients.fetchRemove(fd)) |entry| {
            var client = entry.value;
            self.pubsub.removeConnection(fd);
            client.deinit();
        }

        if (peer_ctx_val) |ctx| {
            logger.info("Connection from {s} closed (fd {})", .{ ctx.strConst(), fd });
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

        var parser_iter = self.parsers.iterator();
        while (parser_iter.next()) |entry| {
            var parser = entry.value_ptr;
            parser.deinit();
        }
        self.parsers.deinit();

        var client_iter = self.clients.iterator();
        while (client_iter.next()) |entry| {
            var client = entry.value_ptr;
            client.deinit();
        }

        self.clients.deinit();
        self.pubsub.deinit();
        self.listener.deinit();
    }
};

fn handleSignal(sig: c_int) callconv(.c) void {
    _ = sig;
    if (global_server) |server| {
        server.beginShutdown();
    }
}

pub fn run(allocator: std.mem.Allocator, address: []const u8) !void {
    var server = try Server.init(allocator, address);
    defer server.deinit();

    global_server = &server;
    const sig_action = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.mem.zeroes(std.posix.sigset_t),
        .flags = 0,
    };

    _ = std.posix.sigaction(std.posix.SIG.INT, &sig_action, null);
    _ = std.posix.sigaction(std.posix.SIG.TERM, &sig_action, null);

    try server.run();

    global_server = null;
}
