const std = @import("std");
const Allocator = std.mem.Allocator;

/// RFC 4180 compliant CSV reader
pub const CsvReader = struct {
    file: std.fs.File,
    allocator: Allocator,
    delimiter: u8,
    buffer: [262144]u8, // Increased to 256KB for fewer syscalls
    buffer_pos: usize,
    buffer_len: usize,
    eof: bool,
    putback_byte: ?u8,

    pub fn init(allocator: Allocator, file: std.fs.File) CsvReader {
        return CsvReader{
            .file = file,
            .allocator = allocator,
            .delimiter = ',',
            .buffer = undefined,
            .buffer_pos = 0,
            .buffer_len = 0,
            .eof = false,
            .putback_byte = null,
        };
    }

    fn readByte(self: *CsvReader) !?u8 {
        // Check if there's a putback byte first
        if (self.putback_byte) |byte| {
            self.putback_byte = null;
            return byte;
        }

        if (self.buffer_pos >= self.buffer_len) {
            if (self.eof) return null;
            self.buffer_len = try self.file.read(&self.buffer);
            self.buffer_pos = 0;
            if (self.buffer_len == 0) {
                self.eof = true;
                return null;
            }
        }
        const byte = self.buffer[self.buffer_pos];
        self.buffer_pos += 1;
        return byte;
    }

    fn putBackByte(self: *CsvReader, byte: u8) void {
        self.putback_byte = byte;
    }

    /// Read the next CSV record
    pub fn readRecord(self: *CsvReader) !?[][]u8 {
        var fields = std.ArrayList([]u8){};
        errdefer {
            for (fields.items) |field| {
                self.allocator.free(field);
            }
            fields.deinit(self.allocator);
        }

        var field_buffer = std.ArrayList(u8){};
        defer field_buffer.deinit(self.allocator);

        var in_quotes = false;
        var at_start = true;

        while (true) {
            const byte_opt = try self.readByte();
            const byte = byte_opt orelse {
                // EOF - handle last field
                if (field_buffer.items.len > 0 or fields.items.len > 0 or !at_start) {
                    try fields.append(self.allocator, try field_buffer.toOwnedSlice(self.allocator));
                }
                if (fields.items.len == 0) {
                    return null;
                }
                return try fields.toOwnedSlice(self.allocator);
            };

            at_start = false;

            if (in_quotes) {
                if (byte == '"') {
                    // Check for escaped quote
                    const next_opt = try self.readByte();
                    if (next_opt) |next| {
                        if (next == '"') {
                            // Escaped quote
                            try field_buffer.append(self.allocator, '"');
                        } else {
                            // End of quoted field
                            in_quotes = false;
                            // Put back the byte
                            self.putBackByte(next);
                        }
                    } else {
                        // EOF after quote
                        in_quotes = false;
                    }
                } else {
                    try field_buffer.append(self.allocator, byte);
                }
            } else {
                if (byte == '"' and field_buffer.items.len == 0) {
                    // Start of quoted field
                    in_quotes = true;
                } else if (byte == self.delimiter) {
                    // End of field
                    try fields.append(self.allocator, try field_buffer.toOwnedSlice(self.allocator));
                    field_buffer = std.ArrayList(u8){};
                } else if (byte == '\r') {
                    // Handle CR - check for LF
                    const next_opt = try self.readByte();
                    if (next_opt) |next| {
                        if (next != '\n') {
                            self.putBackByte(next);
                        }
                    }
                    // End of record
                    try fields.append(self.allocator, try field_buffer.toOwnedSlice(self.allocator));
                    return try fields.toOwnedSlice(self.allocator);
                } else if (byte == '\n') {
                    // End of record
                    try fields.append(self.allocator, try field_buffer.toOwnedSlice(self.allocator));
                    return try fields.toOwnedSlice(self.allocator);
                } else {
                    try field_buffer.append(self.allocator, byte);
                }
            }
        }
    }

    /// Free a record returned by readRecord
    pub fn freeRecord(self: *CsvReader, record: [][]u8) void {
        for (record) |field| {
            self.allocator.free(field);
        }
        self.allocator.free(record);
    }
};

/// Fast CSV reader for simple cases (no quotes)
pub const FastCsvReader = struct {
    file: std.fs.File,
    allocator: Allocator,
    delimiter: u8,
    line_buffer: std.ArrayList(u8),
    buffer: [262144]u8, // Increased to 256KB to match CsvReader
    buffer_pos: usize,
    buffer_len: usize,
    eof: bool,

    pub fn init(allocator: Allocator, file: std.fs.File) FastCsvReader {
        return FastCsvReader{
            .file = file,
            .allocator = allocator,
            .delimiter = ',',
            .line_buffer = std.ArrayList(u8){},
            .buffer = undefined,
            .buffer_pos = 0,
            .buffer_len = 0,
            .eof = false,
        };
    }

    fn readByte(self: *FastCsvReader) !?u8 {
        if (self.buffer_pos >= self.buffer_len) {
            if (self.eof) return null;
            self.buffer_len = try self.file.read(&self.buffer);
            self.buffer_pos = 0;
            if (self.buffer_len == 0) {
                self.eof = true;
                return null;
            }
        }
        const byte = self.buffer[self.buffer_pos];
        self.buffer_pos += 1;
        return byte;
    }

    pub fn deinit(self: *FastCsvReader) void {
        self.line_buffer.deinit(self.allocator);
    }

    /// Read the next CSV record (fast path - assumes no escaped quotes)
    pub fn readRecord(self: *FastCsvReader) !?[][]u8 {
        self.line_buffer.clearRetainingCapacity();

        // Read line byte by byte until \n
        while (try self.readByte()) |byte| {
            if (byte == '\n') break;
            try self.line_buffer.append(self.allocator, byte);
        }

        if (self.line_buffer.items.len == 0 and self.eof) {
            return null;
        }

        // Trim trailing \r if present
        if (self.line_buffer.items.len > 0 and self.line_buffer.items[self.line_buffer.items.len - 1] == '\r') {
            _ = self.line_buffer.pop();
        }

        // Split by delimiter
        var fields = std.ArrayList([]u8){};
        errdefer {
            for (fields.items) |field| {
                self.allocator.free(field);
            }
            fields.deinit(self.allocator);
        }

        var iter = std.mem.splitScalar(u8, self.line_buffer.items, self.delimiter);
        while (iter.next()) |field| {
            try fields.append(self.allocator, try self.allocator.dupe(u8, field));
        }

        return try fields.toOwnedSlice(self.allocator);
    }

    /// Free a record returned by readRecord
    pub fn freeRecord(self: *FastCsvReader, record: [][]u8) void {
        for (record) |field| {
            self.allocator.free(field);
        }
        self.allocator.free(record);
    }
};

/// CSV writer with buffering
pub const CsvWriter = struct {
    file: std.fs.File,
    delimiter: u8,
    buffer: [1048576]u8, // 1MB buffer for fewer write syscalls
    buffer_pos: usize,

    pub fn init(file: std.fs.File) CsvWriter {
        return CsvWriter{
            .file = file,
            .delimiter = ',',
            .buffer = undefined,
            .buffer_pos = 0,
        };
    }

    fn writeToBuffer(self: *CsvWriter, data: []const u8) !void {
        var remaining = data;
        while (remaining.len > 0) {
            const space_left = self.buffer.len - self.buffer_pos;
            if (space_left == 0) {
                try self.flush();
                continue;
            }

            const to_copy = @min(remaining.len, space_left);
            @memcpy(self.buffer[self.buffer_pos..][0..to_copy], remaining[0..to_copy]);
            self.buffer_pos += to_copy;
            remaining = remaining[to_copy..];
        }
    }

    pub fn writeRecord(self: *CsvWriter, fields: []const []const u8) !void {
        for (fields, 0..) |field, i| {
            if (i > 0) {
                try self.writeToBuffer(&[_]u8{self.delimiter});
            }

            // Check if field needs quoting
            const needs_quotes = std.mem.indexOfAny(u8, field, ",\"\r\n") != null;
            if (needs_quotes) {
                try self.writeToBuffer("\"");
                // Escape quotes
                for (field) |c| {
                    if (c == '"') {
                        try self.writeToBuffer("\"\"");
                    } else {
                        try self.writeToBuffer(&[_]u8{c});
                    }
                }
                try self.writeToBuffer("\"");
            } else {
                try self.writeToBuffer(field);
            }
        }
        try self.writeToBuffer("\n");
    }

    pub fn flush(self: *CsvWriter) !void {
        if (self.buffer_pos > 0) {
            _ = try self.file.write(self.buffer[0..self.buffer_pos]);
            self.buffer_pos = 0;
        }
    }
};

test "csv reader simple" {
    const allocator = std.testing.allocator;

    // Create a temporary file
    const file_path = "test_csv_simple.csv";
    var file = try std.fs.cwd().createFile(file_path, .{ .read = true });
    defer {
        file.close();
        std.fs.cwd().deleteFile(file_path) catch {};
    }

    // Write CSV data
    try file.writeAll("name,age\nAlice,30\nBob,25\n");
    try file.seekTo(0);

    // Read CSV
    var reader = CsvReader.init(allocator, file);

    // Read header
    const header = (try reader.readRecord()).?;
    defer reader.freeRecord(header);
    try std.testing.expectEqualStrings("name", header[0]);
    try std.testing.expectEqualStrings("age", header[1]);

    // Read first row
    const row1 = (try reader.readRecord()).?;
    defer reader.freeRecord(row1);
    try std.testing.expectEqualStrings("Alice", row1[0]);
    try std.testing.expectEqualStrings("30", row1[1]);

    // Read second row
    const row2 = (try reader.readRecord()).?;
    defer reader.freeRecord(row2);
    try std.testing.expectEqualStrings("Bob", row2[0]);
    try std.testing.expectEqualStrings("25", row2[1]);

    // No more rows
    try std.testing.expect(try reader.readRecord() == null);
}
