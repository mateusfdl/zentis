//! A simple fast queue/ring buffer for buffering writes.
//!
//! This implementation handles partial writes and backpressure by maintaining
//! separate read and write positions in a fixed-size circular buffer.

const std = @import("std");

/// FastQueue provides a fixed-size circular buffer for byte storage.
/// It's useful for handling partial writes where we need to buffer data
/// until the socket is ready to write more.
pub const FastQueue = struct {
    data: []u8,
    read_pos: usize,
    write_pos: usize,
    full: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !FastQueue {
        const data = try allocator.alloc(u8, capacity);
        return FastQueue{
            .data = data,
            .read_pos = 0,
            .write_pos = 0,
            .full = false,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FastQueue) void {
        self.allocator.free(self.data);
    }

    pub fn available(self: *const FastQueue) usize {
        if (self.full) {
            return self.data.len;
        }

        if (self.write_pos >= self.read_pos) {
            return self.write_pos - self.read_pos;
        }

        return self.data.len - (self.read_pos - self.write_pos);
    }

    pub fn space(self: *const FastQueue) usize {
        return self.data.len - self.available();
    }

    pub fn isEmpty(self: *const FastQueue) bool {
        return !self.full and self.read_pos == self.write_pos;
    }

    pub fn isFull(self: *const FastQueue) bool {
        return self.full;
    }

    /// Write data into the ring buffer.
    /// Returns the number of bytes actually written (may be less than src.len if full).
    pub fn write(self: *FastQueue, src: []const u8) usize {
        if (self.full) {
            return 0;
        }

        var written: usize = 0;
        for (src) |byte| {
            if (self.full) {
                break;
            }

            self.data[self.write_pos] = byte;
            self.write_pos = (self.write_pos + 1) % self.data.len;

            if (self.write_pos == self.read_pos) {
                self.full = true;
            }

            written += 1;
        }

        return written;
    }

    /// Read data from the ring buffer into dst.
    /// Returns the number of bytes actually read.
    pub fn read(self: *FastQueue, dst: []u8) usize {
        if (self.isEmpty()) {
            return 0;
        }

        var bytes_read: usize = 0;
        for (dst) |*byte| {
            if (self.isEmpty()) {
                break;
            }

            byte.* = self.data[self.read_pos];
            self.read_pos = (self.read_pos + 1) % self.data.len;
            self.full = false;
            bytes_read += 1;
        }

        return bytes_read;
    }

    /// Peek at the readable data without consuming it.
    /// Returns a slice that can be used for direct I/O operations.
    /// Note: Due to wrapping, this might return less than available() bytes.
    pub fn peekReadable(self: *const FastQueue) []const u8 {
        if (self.isEmpty()) {
            return &[_]u8{};
        }

        if (self.write_pos > self.read_pos) {
            // Simple case: no wrapping
            return self.data[self.read_pos..self.write_pos];
        } else {
            // Wrapped: return data from read_pos to end
            return self.data[self.read_pos..];
        }
    }

    /// Consume n bytes from the buffer (advance read position).
    /// This is used after successfully reading from peekReadable().
    pub fn consume(self: *FastQueue, n: usize) void {
        const available_bytes = self.available();
        const to_consume = @min(n, available_bytes);

        self.read_pos = (self.read_pos + to_consume) % self.data.len;
        self.full = false;
    }

    /// Clear the buffer (reset read and write positions).
    pub fn clear(self: *FastQueue) void {
        self.read_pos = 0;
        self.write_pos = 0;
        self.full = false;
    }
};
