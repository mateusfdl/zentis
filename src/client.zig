//! Client state management for tracking per-connection information.

const std = @import("std");

/// client state tracked for each connection.
pub const ClientState = struct {
    fd: std.posix.fd_t,
    authenticated: bool,
    auth_token: ?[]const u8, // Owned, must be freed
    subscriptions: std.StringHashMap(void),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, fd: std.posix.fd_t) ClientState {
        return .{
            .fd = fd,
            .authenticated = false,
            .auth_token = null,
            .subscriptions = std.StringHashMap(void).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ClientState) void {
        if (self.auth_token) |token| {
            self.allocator.free(token);
        }

        var iter = self.subscriptions.keyIterator();
        while (iter.next()) |channel_name| {
            self.allocator.free(channel_name.*);
        }
        self.subscriptions.deinit();
    }

    /// mark client as authenticated with the given token.
    /// the token is duplicated and must be owned by the allocator.
    pub fn authenticate(self: *ClientState, auth_token: []const u8) !void {
        if (self.auth_token != null) {
            return error.AlreadyAuthenticated;
        }

        const token_copy = try self.allocator.dupe(u8, auth_token);
        self.auth_token = token_copy;
        self.authenticated = true;
    }

    pub fn addSubscription(self: *ClientState, channel_name: []const u8) !void {
        const channel_copy = try self.allocator.dupe(u8, channel_name);
        errdefer self.allocator.free(channel_copy);

        try self.subscriptions.put(channel_copy, {});
    }

    pub fn removeSubscription(self: *ClientState, channel_name: []const u8) bool {
        if (self.subscriptions.fetchRemove(channel_name)) |entry| {
            self.allocator.free(entry.key);
            return true;
        }
        return false;
    }

    pub fn subscriptionCount(self: *const ClientState) usize {
        return self.subscriptions.count();
    }

    pub fn isSubscribedTo(self: *const ClientState, channel_name: []const u8) bool {
        return self.subscriptions.get(channel_name) != null;
    }
};

test "ClientState: authenticate" {
    const testing = std.testing;
    var client = ClientState.init(testing.allocator, 10);
    defer client.deinit();

    try testing.expect(!client.authenticated);

    try client.authenticate("token123");
    try testing.expect(client.authenticated);
    try testing.expectEqualStrings("token123", client.auth_token.?);
}

test "ClientState: subscriptions" {
    const testing = std.testing;
    var client = ClientState.init(testing.allocator, 10);
    defer client.deinit();

    try client.addSubscription("chat");
    try client.addSubscription("notifications");

    try testing.expectEqual(@as(usize, 2), client.subscriptionCount());
    try testing.expect(client.isSubscribedTo("chat"));
    try testing.expect(client.isSubscribedTo("notifications"));
    try testing.expect(!client.isSubscribedTo("other"));

    try testing.expect(client.removeSubscription("chat"));
    try testing.expect(!client.isSubscribedTo("chat"));
}
