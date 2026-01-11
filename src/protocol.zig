//! Message format (Big-Endian):
//! ┌─────────────┬─────────────┬─────────────────────────┐
//! │ Type ID (2) │ Length (2)  │ Payload (variable)      │
//! └─────────────┴─────────────┴─────────────────────────┘

const std = @import("std");
const logger = @import("logger.zig");

pub const MessageType = enum(u16) {
    CONNECT = 1,
    CONNECTED = 2,
    SUBSCRIBE = 3,
    SUBSCRIBED = 4,
    UNSUBSCRIBE = 5,
    UNSUBSCRIBED = 6,
    PUBLISH = 7,
    MESSAGE = 8,
    ERROR = 9,
    PING = 10,
    PONG = 11,

    pub fn fromInt(value: u16) ?MessageType {
        return std.meta.intToEnum(MessageType, value) catch null;
    }
};

pub const ProtocolError = error{
    IncompleteHeader,
    IncompletePayload,
    InvalidMessageLength,
    InvalidStringLength,
    MessageTooLarge,
};

/// Maximum allowed message size (prevents memory exhaustion attacks).
pub const MAX_MESSAGE_SIZE = 10 * 1024 * 1024; // 10 MB
pub const MAX_CHANNEL_LENGTH = 256;
pub const MAX_ERROR_MESSAGE_LENGTH = 1024;

/// Represents a parsed message. The payload slice points to the parser's
/// internal buffer and is only valid until the next consume() call.
pub const Message = struct {
    msg_type: MessageType,
    payload: []const u8,
    total_len: usize, // Including header

    /// CONNECT : [auth_token_len (1)][auth_token (variable)]
    pub fn parseConnect(self: Message) !struct { auth_token: []const u8 } {
        if (self.payload.len < 1) return error.IncompletePayload;
        const token_len = self.payload[0];
        if (self.payload.len < 1 + token_len) return error.IncompletePayload;
        return .{ .auth_token = self.payload[1..][0..token_len] };
    }

    /// SUBSCRIBE : [channel_len (1)][channel (variable)]
    pub fn parseSubscribe(self: Message) ![]const u8 {
        return self.parseStringPayload();
    }

    /// UNSUBSCRIBE : [channel_len (1)][channel (variable)]
    pub fn parseUnsubscribe(self: Message) ![]const u8 {
        return self.parseStringPayload();
    }

    /// PUBLISH : [channel_len (1)][channel (variable)][data_len (2)][data (variable)]
    pub fn parsePublish(self: Message) !struct { channel: []const u8, data: []const u8 } {
        if (self.payload.len < 1) return error.IncompletePayload;
        const channel_len = self.payload[0];
        if (self.payload.len < 1 + channel_len + 2) return error.IncompletePayload;
        const channel = self.payload[1..][0..channel_len];
        const data_len = std.mem.readInt(u16, self.payload[1 + channel_len ..][0..2], .big);
        if (self.payload.len < 1 + channel_len + 2 + data_len) return error.IncompletePayload;
        const data = self.payload[1 + channel_len + 2 ..][0..data_len];
        return .{ .channel = channel, .data = data };
    }

    /// ERROR : [error_code (1)][error_msg_len (2)][error_msg (variable)]
    pub fn parseError(self: Message) !struct { error_code: u8, error_msg: []const u8 } {
        if (self.payload.len < 3) return error.IncompletePayload;
        const error_code = self.payload[0];
        const msg_len = std.mem.readInt(u16, self.payload[1..3], .big);
        if (self.payload.len < 3 + msg_len) return error.IncompletePayload;
        const error_msg = self.payload[3..][0..msg_len];
        return .{ .error_code = error_code, .error_msg = error_msg };
    }

    fn parseStringPayload(self: Message) ![]const u8 {
        if (self.payload.len < 1) return error.IncompletePayload;
        const str_len = self.payload[0];
        if (self.payload.len < 1 + str_len) return error.IncompletePayload;
        return self.payload[1..][0..str_len];
    }
};

/// Builder for encoding messages into the binary protocol format.
pub const MessageBuilder = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) MessageBuilder {
        return .{ .allocator = allocator };
    }

    /// Build a CONNECT message: [auth_token_len (1)][auth_token (variable)]
    pub fn buildConnect(self: *MessageBuilder, auth_token: []const u8) ![]const u8 {
        if (auth_token.len > 255) return error.AuthTokenTooLong;
        const payload_len = 1 + auth_token.len;
        var payload = try self.allocator.alloc(u8, payload_len);
        payload[0] = @intCast(auth_token.len);
        @memcpy(payload[1..], auth_token);
        return self.buildMessage(.CONNECT, payload);
    }

    /// Build a CONNECTED message: [connection_id (variable string)]
    pub fn buildConnected(self: *MessageBuilder, connection_id: []const u8) ![]const u8 {
        return self.buildStringMessage(.CONNECTED, connection_id);
    }

    /// Build a SUBSCRIBED message: [channel (variable string)]
    pub fn buildSubscribed(self: *MessageBuilder, channel: []const u8) ![]const u8 {
        return self.buildStringMessage(.SUBSCRIBED, channel);
    }

    /// Build an UNSUBSCRIBED message: [channel (variable string)]
    pub fn buildUnsubscribed(self: *MessageBuilder, channel: []const u8) ![]const u8 {
        return self.buildStringMessage(.UNSUBSCRIBED, channel);
    }

    /// Build a MESSAGE: [channel_len (1)][channel (variable)][data_len (2)][data (variable)]
    pub fn buildChannelMessage(self: *MessageBuilder, channel: []const u8, data: []const u8) ![]const u8 {
        if (channel.len > 255) return error.ChannelNameTooLong;
        if (data.len > std.math.maxInt(u16)) return error.DataTooLarge;

        const payload_len = 1 + channel.len + 2 + data.len;
        var payload = try self.allocator.alloc(u8, payload_len);
        errdefer self.allocator.free(payload);

        payload[0] = @intCast(channel.len);
        @memcpy(payload[1..][0..channel.len], channel);
        std.mem.writeInt(u16, payload[1 + channel.len ..][0..2], @intCast(data.len), .big);
        @memcpy(payload[1 + channel.len + 2 ..], data);

        return self.buildMessage(.MESSAGE, payload);
    }

    /// Build an ERROR message: [error_code (1)][error_msg_len (2)][error_msg (variable)]
    pub fn buildError(self: *MessageBuilder, error_code: u8, error_msg: []const u8) ![]const u8 {
        if (error_msg.len > MAX_ERROR_MESSAGE_LENGTH) return error.ErrorMessageTooLong;

        const payload_len = 1 + 2 + error_msg.len;
        var payload = try self.allocator.alloc(u8, payload_len);
        errdefer self.allocator.free(payload);

        payload[0] = error_code;
        std.mem.writeInt(u16, payload[1..3], @intCast(error_msg.len), .big);
        @memcpy(payload[3..], error_msg);

        return self.buildMessage(.ERROR, payload);
    }

    /// Build a PONG message (empty payload)
    pub fn buildPong(self: *MessageBuilder) ![]const u8 {
        return self.buildMessage(.PONG, &[_]u8{});
    }

    fn buildStringMessage(self: *MessageBuilder, msg_type: MessageType, str: []const u8) ![]const u8 {
        if (str.len > 255) return error.StringTooLong;
        const payload_len = 1 + str.len;
        var payload = try self.allocator.alloc(u8, payload_len);
        errdefer self.allocator.free(payload);
        payload[0] = @intCast(str.len);
        @memcpy(payload[1..], str);
        return self.buildMessage(msg_type, payload);
    }

    fn buildMessage(self: *MessageBuilder, msg_type: MessageType, payload: []const u8) ![]const u8 {
        const total_len = 4 + payload.len; // 2 bytes type + 2 bytes length + payload
        const buffer = try self.allocator.alloc(u8, total_len);

        std.mem.writeInt(u16, buffer[0..2], @intFromEnum(msg_type), .big);
        std.mem.writeInt(u16, buffer[2..4], @intCast(payload.len), .big);
        @memcpy(buffer[4..], payload);

        return buffer;
    }

    /// Free a message buffer allocated by any build* method.
    pub fn free(self: *MessageBuilder, buffer: []const u8) void {
        self.allocator.free(buffer);
    }
};

/// Incremental message parser for handling fragmented data.
/// Buffers incoming data and extracts complete messages as they arrive.
pub const MessageParser = struct {
    buffer: []u8,
    capacity: usize,
    len: usize,
    max_message_size: usize,
    allocator: std.mem.Allocator,

    const INITIAL_CAPACITY = 4096;

    pub fn init(allocator: std.mem.Allocator) MessageParser {
        const buffer = allocator.alloc(u8, INITIAL_CAPACITY) catch unreachable;
        return .{
            .buffer = buffer,
            .capacity = INITIAL_CAPACITY,
            .len = 0,
            .max_message_size = MAX_MESSAGE_SIZE,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MessageParser) void {
        self.allocator.free(self.buffer);
    }

    pub fn feed(self: *MessageParser, data: []const u8) !void {
        // prevent unbounded buffer growth
        if (self.len + data.len > self.max_message_size * 2) {
            return error.BufferOverflow;
        }

        // grow buffer if needed
        const needed = self.len + data.len;
        if (needed > self.capacity) {
            var new_capacity = self.capacity * 2;
            while (new_capacity < needed) {
                new_capacity *= 2;
            }
            if (new_capacity > self.max_message_size * 2) {
                new_capacity = self.max_message_size * 2;
            }
            self.buffer = try self.allocator.realloc(self.buffer, new_capacity);
            self.capacity = new_capacity;
        }

        @memcpy(self.buffer[self.len..][0..data.len], data);
        self.len += data.len;
    }

    /// try to parse the next complete message from the buffer.
    /// returns null if more data is needed.
    /// the returned message's payload points into the internal buffer
    /// and is only valid until consume() is called.
    pub fn next(self: *MessageParser) !?Message {
        // check if we have the header (4 bytes)
        if (self.len < 4) {
            return null;
        }

        // parse header
        const type_id = std.mem.readInt(u16, self.buffer[0..2], .big);
        const payload_len = std.mem.readInt(u16, self.buffer[2..4], .big);

        if (payload_len > self.max_message_size) {
            return error.MessageTooLarge;
        }

        const total_len = 4 + payload_len;

        // check if we have the complete message
        if (self.len < total_len) {
            return null;
        }

        const msg_type = MessageType.fromInt(type_id) orelse {
            logger.warn("Unknown message type: {}", .{type_id});
            return error.UnknownMessageType;
        };

        const payload = self.buffer[4..total_len];

        return Message{
            .msg_type = msg_type,
            .payload = payload,
            .total_len = total_len,
        };
    }

    /// consume n bytes from the beginning of the buffer.
    /// should be called after processing a message.
    pub fn consume(self: *MessageParser, n: usize) void {
        if (n >= self.len) {
            self.len = 0;
        } else {
            // move remaining data to the front
            const remaining = self.len - n;
            _ = .{};
            std.mem.copyForwards(u8, self.buffer[0..remaining], self.buffer[n .. n + remaining]);
            self.len = remaining;
        }
    }

    pub fn buffered(self: *const MessageParser) usize {
        return self.len;
    }

    pub fn clear(self: *MessageParser) void {
        self.len = 0;
    }
};

test "MessageParser: parse complete CONNECT message" {
    const testing = std.testing;
    var parser = MessageParser.init(testing.allocator);
    defer parser.deinit();

    var buffer: [10]u8 = undefined;
    std.mem.writeInt(u16, buffer[0..2], @intFromEnum(MessageType.CONNECT), .big);
    std.mem.writeInt(u16, buffer[2..4], 1, .big); // payload length
    buffer[4] = 0;

    try parser.feed(&buffer);

    const msg = (try parser.next()).?;
    try testing.expectEqual(MessageType.CONNECT, msg.msg_type);
    try testing.expectEqual(@as(usize, 5), msg.total_len);

    const connect = try msg.parseConnect();
    try testing.expectEqual(@as(usize, 0), connect.auth_token.len);
}

test "MessageParser: fragmented message" {
    const testing = std.testing;
    var parser = MessageParser.init(testing.allocator);
    defer parser.deinit();

    var header: [4]u8 = undefined;
    std.mem.writeInt(u16, header[0..2], @intFromEnum(MessageType.PING), .big);
    std.mem.writeInt(u16, header[2..4], 0, .big); // empty payload

    try parser.feed(header[0..2]);
    try testing.expectEqual(@as(?Message, null), try parser.next());

    try parser.feed(header[2..]);
    const msg = (try parser.next()).?;
    try testing.expectEqual(MessageType.PING, msg.msg_type);
}

test "MessageBuilder: build CONNECTED message" {
    const testing = std.testing;
    var builder = MessageBuilder.init(testing.allocator);

    const msg = try builder.buildConnected("conn-123");
    defer builder.free(msg);

    try testing.expectEqual(@as(usize, 12), msg.len);

    const type_id = std.mem.readInt(u16, msg[0..2], .big);
    try testing.expectEqual(@intFromEnum(MessageType.CONNECTED), type_id);
}

test "Message: parse PUBLISH payload" {
    const testing = std.testing;
    const payload = &[_]u8{ 5, 'c', 'h', 'a', 't', 0, 0, 5, 'h', 'e', 'l', 'l', 'o' };

    const msg = Message{
        .msg_type = .PUBLISH,
        .payload = payload,
        .total_len = 0,
    };

    const publish = try msg.parsePublish();
    try testing.expectEqualStrings("chat", publish.channel);
    try testing.expectEqualStrings("hello", publish.data);
}
