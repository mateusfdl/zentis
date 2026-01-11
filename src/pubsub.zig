//! Pub/Sub engine for channel management and message broadcasting.

const std = @import("std");
const logger = @import("logger.zig");

pub const Channel = struct {
    name: []const u8,
    subscribers: []std.posix.fd_t,
    capacity: usize,
    count: usize,
    allocator: std.mem.Allocator,

    const INITIAL_CAPACITY = 16;

    pub fn init(allocator: std.mem.Allocator, name: []const u8) Channel {
        const subscribers = allocator.alloc(std.posix.fd_t, INITIAL_CAPACITY) catch unreachable;
        return .{
            .name = allocator.dupe(u8, name) catch unreachable,
            .subscribers = subscribers,
            .capacity = INITIAL_CAPACITY,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Channel) void {
        self.allocator.free(self.name);
        self.allocator.free(self.subscribers);
    }

    pub fn subscribe(self: *Channel, fd: std.posix.fd_t) !bool {
        _ = .{};
        for (self.subscribers[0..self.count]) |subscriber_fd| {
            if (subscriber_fd == fd) {
                return false;
            }
        }

        // grow if needed
        if (self.count >= self.capacity) {
            const new_capacity = self.capacity * 2;
            self.subscribers = try self.allocator.realloc(self.subscribers, new_capacity);
            self.capacity = new_capacity;
        }

        self.subscribers[self.count] = fd;
        self.count += 1;
        return true;
    }

    pub fn unsubscribe(self: *Channel, fd: std.posix.fd_t) bool {
        for (self.subscribers[0..self.count], 0..) |subscriber_fd, i| {
            if (subscriber_fd == fd) {
                // shift remaining items
                std.mem.copyForwards(std.posix.fd_t, self.subscribers[i .. self.count - 1], self.subscribers[i + 1 .. self.count]);
                self.count -= 1;
                return true;
            }
        }
        return false;
    }

    pub fn subscriberCount(self: *const Channel) usize {
        return self.count;
    }
};

pub const PubSub = struct {
    allocator: std.mem.Allocator,
    channels: std.StringHashMap(*Channel),
    /// track which channels each connection is subscribed to (fd -> channel names)
    connection_subscriptions: std.AutoHashMap(std.posix.fd_t, std.StringHashMap(void)),

    pub fn init(allocator: std.mem.Allocator) PubSub {
        return .{
            .allocator = allocator,
            .channels = std.StringHashMap(*Channel).init(allocator),
            .connection_subscriptions = std.AutoHashMap(std.posix.fd_t, std.StringHashMap(void)).init(allocator),
        };
    }

    pub fn deinit(self: *PubSub) void {
        var channel_iter = self.channels.valueIterator();
        while (channel_iter.next()) |channel| {
            channel.*.deinit();
            self.allocator.destroy(channel.*);
        }
        self.channels.deinit();

        // free connection subscriptions tracking
        var conn_iter = self.connection_subscriptions.valueIterator();
        while (conn_iter.next()) |subs| {
            subs.deinit();
        }
        self.connection_subscriptions.deinit();
    }

    pub fn subscribe(self: *PubSub, fd: std.posix.fd_t, channel_name: []const u8) !bool {
        var channel: *Channel = undefined;
        if (self.channels.get(channel_name)) |existing_channel| {
            channel = existing_channel;
        } else {
            const new_channel = try self.allocator.create(Channel);
            new_channel.* = Channel.init(self.allocator, channel_name);
            try self.channels.put(new_channel.name, new_channel);
            channel = new_channel;
        }

        const newly_subscribed = try channel.subscribe(fd);

        if (newly_subscribed) {
            const conn_subs = try self.connection_subscriptions.getOrPut(fd);
            if (!conn_subs.found_existing) {
                conn_subs.value_ptr.* = std.StringHashMap(void).init(self.allocator);
            }
            try conn_subs.value_ptr.*.put(channel.name, {});
        }

        return newly_subscribed;
    }

    pub fn unsubscribe(self: *PubSub, fd: std.posix.fd_t, channel_name: []const u8) bool {
        const channel = self.channels.get(channel_name) orelse return false;

        const was_subscribed = channel.unsubscribe(fd);

        if (was_subscribed) {
            if (self.connection_subscriptions.getPtr(fd)) |subs| {
                _ = subs.remove(channel_name);
            }

            // clean up empty channels
            if (channel.subscriberCount() == 0) {
                logger.debug("Removing empty channel: {s}", .{channel_name});
                _ = self.channels.remove(channel_name);
                channel.deinit();
                self.allocator.destroy(channel);
            }
        }

        return was_subscribed;
    }

    /// get a list of all subscribers for a channel (excluding the sender).
    /// returns a dynamically allocated slice that the caller owns and must free.
    pub fn getSubscribers(self: *PubSub, allocator: std.mem.Allocator, channel_name: []const u8, exclude_fd: ?std.posix.fd_t) ![]std.posix.fd_t {
        const channel = self.channels.get(channel_name) orelse {
            return allocator.alloc(std.posix.fd_t, 0);
        };

        var recipient_count: usize = 0;
        for (channel.subscribers[0..channel.count]) |fd| {
            if (exclude_fd) |exclude| {
                if (fd != exclude) {
                    recipient_count += 1;
                }
            } else {
                recipient_count += 1;
            }
        }

        const result = try allocator.alloc(std.posix.fd_t, recipient_count);

        var result_idx: usize = 0;
        for (channel.subscribers[0..channel.count]) |fd| {
            if (exclude_fd) |exclude| {
                if (fd != exclude) {
                    result[result_idx] = fd;
                    result_idx += 1;
                }
            } else {
                result[result_idx] = fd;
                result_idx += 1;
            }
        }

        return result;
    }

    /// publish a message to all subscribers of a channel.
    /// returns the number of subscribers the message was sent to.
    pub fn publish(self: *PubSub, channel_name: []const u8, data: []const u8, sender_fd: std.posix.fd_t) usize {
        _ = data; // data is logged by the server, not used here
        const channel = self.channels.get(channel_name) orelse {
            logger.warn("Publish to non-existent channel: {s}", .{channel_name});
            return 0;
        };

        var sent_count: usize = 0;
        for (channel.subscribers[0..channel.count]) |fd| {
            // don't echo back to sender
            if (fd != sender_fd) {
                sent_count += 1;
            }
        }

        logger.debug("Published to channel '{s}' ({} subscribers)", .{ channel_name, sent_count });
        return sent_count;
    }

    /// remove all subscriptions for a connection (called on disconnect).
    pub fn removeConnection(self: *PubSub, fd: std.posix.fd_t) void {
        const conn_subs_entry = self.connection_subscriptions.fetchRemove(fd) orelse return;
        var conn_subs = conn_subs_entry.value;

        logger.debug("Removing all subscriptions for fd {}", .{fd});

        // iterate over all channels and unsubscribe the fd
        // collect channels to potentially clean up first
        var channels_to_remove_buf: [256][]const u8 = undefined;
        var channels_to_remove_len: usize = 0;

        var channel_iter = self.channels.iterator();
        while (channel_iter.next()) |entry| {
            const channel = entry.value_ptr.*;
            if (channel.unsubscribe(fd)) {
                if (channel.subscriberCount() == 0) {
                    if (channels_to_remove_len < channels_to_remove_buf.len) {
                        channels_to_remove_buf[channels_to_remove_len] = channel.name;
                        channels_to_remove_len += 1;
                    }
                }
            }
        }

        // now remove empty channels
        for (channels_to_remove_buf[0..channels_to_remove_len]) |channel_name| {
            _ = .{};
            if (self.channels.fetchRemove(channel_name)) |entry| {
                var channel = entry.value;
                logger.debug("Removing empty channel: {s}", .{channel_name});
                channel.deinit();
                self.allocator.destroy(channel);
            }
        }

        // The channel name strings are owned by the Channel structs, not by this map,
        // so we only need to deinit the map itself without freeing the keys
        conn_subs.deinit();
    }

    /// get the number of active channels.
    pub fn channelCount(self: *const PubSub) usize {
        return self.channels.count();
    }

    /// get total number of subscriptions across all channels.
    pub fn totalSubscriptionCount(self: *const PubSub) usize {
        var count: usize = 0;
        var iter = self.channels.valueIterator();
        while (iter.next()) |channel| {
            count += channel.subscriberCount();
        }
        return count;
    }

    /// get a list of all channel names.
    /// caller owns the returned memory and must free it.
    pub fn getChannelNames(self: *PubSub, allocator: std.mem.Allocator) ![][]const u8 {
        const channels = try allocator.alloc([]const u8, self.channels.count());
        var i: usize = 0;
        var iter = self.channels.keyIterator();
        while (iter.next()) |name| {
            channels[i] = try allocator.dupe(u8, name.*);
            i += 1;
        }
        return channels;
    }
};

test "PubSub: subscribe and publish" {
    const testing = std.testing;
    var pubsub = PubSub.init(testing.allocator);
    defer pubsub.deinit();

    const fd1: std.posix.fd_t = 10;
    const fd2: std.posix.fd_t = 20;

    try testing.expectEqual(@as(usize, 0), pubsub.channelCount());
    const newly_subscribed = try pubsub.subscribe(fd1, "chat");
    try testing.expect(newly_subscribed);
    try testing.expectEqual(@as(usize, 1), pubsub.channelCount());

    const subscribed_again = try pubsub.subscribe(fd1, "chat");
    try testing.expect(!subscribed_again);

    const fd2_subscribed = try pubsub.subscribe(fd2, "chat");
    try testing.expect(fd2_subscribed);

    const subs = pubsub.getSubscribers("chat", null);
    try testing.expectEqual(@as(usize, 2), subs.len);
}

test "PubSub: unsubscribe" {
    const testing = std.testing;
    var pubsub = PubSub.init(testing.allocator);
    defer pubsub.deinit();

    const fd1: std.posix.fd_t = 10;

    _ = try pubsub.subscribe(fd1, "chat");
    try testing.expectEqual(@as(usize, 1), pubsub.channelCount());

    const was_subscribed = pubsub.unsubscribe(fd1, "chat");
    try testing.expect(was_subscribed);
    try testing.expectEqual(@as(usize, 0), pubsub.channelCount());
}

test "PubSub: remove connection" {
    const testing = std.testing;
    var pubsub = PubSub.init(testing.allocator);
    defer pubsub.deinit();

    const fd1: std.posix.fd_t = 10;

    _ = try pubsub.subscribe(fd1, "chat");
    _ = try pubsub.subscribe(fd1, "notifications");

    try testing.expectEqual(@as(usize, 2), pubsub.channelCount());

    pubsub.removeConnection(fd1);

    try testing.expectEqual(@as(usize, 0), pubsub.channelCount());
}

test "PubSub: get subscribers excluding sender" {
    const testing = std.testing;
    var pubsub = PubSub.init(testing.allocator);
    defer pubsub.deinit();

    const fd1: std.posix.fd_t = 10;
    const fd2: std.posix.fd_t = 20;

    _ = try pubsub.subscribe(fd1, "chat");
    _ = try pubsub.subscribe(fd2, "chat");

    const subs = pubsub.getSubscribers("chat", fd1);
    try testing.expectEqual(@as(usize, 1), subs.len);
    try testing.expectEqual(fd2, subs[0]);
}
