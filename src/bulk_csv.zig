const std = @import("std");
const Allocator = std.mem.Allocator;

/// High-performance bulk CSV reader - optimized for speed over RFC 4180 compliance
/// Assumes: no quoted fields with embedded newlines/commas
pub const BulkCsvReader = struct {
    file: std.fs.File,
    allocator: Allocator,
    delimiter: u8,
    buffer: []u8,
    buffer_pos: usize,
    buffer_len: usize,
    eof: bool,
    line_start: usize,

    pub fn init(allocator: Allocator, file: std.fs.File) !BulkCsvReader {
        // Allocate 2MB buffer for bulk reading - fewer syscalls
        const buffer = try allocator.alloc(u8, 2 * 1024 * 1024);
        return BulkCsvReader{
            .file = file,
            .allocator = allocator,
            .delimiter = ',',
            .buffer = buffer,
            .buffer_pos = 0,
            .buffer_len = 0,
            .eof = false,
            .line_start = 0,
        };
    }

    pub fn deinit(self: *BulkCsvReader) void {
        self.allocator.free(self.buffer);
    }

    fn fillBuffer(self: *BulkCsvReader) !void {
        if (self.eof) return;

        // If there's partial data at the end, move it to the start
        if (self.line_start < self.buffer_len) {
            const remaining = self.buffer_len - self.line_start;
            std.mem.copyForwards(u8, self.buffer[0..remaining], self.buffer[self.line_start..self.buffer_len]);
            self.buffer_len = remaining;
            self.buffer_pos = remaining;
            self.line_start = 0;
        } else {
            self.buffer_len = 0;
            self.buffer_pos = 0;
            self.line_start = 0;
        }

        // Fill the rest of the buffer
        const bytes_read = try self.file.read(self.buffer[self.buffer_len..]);
        self.buffer_len += bytes_read;
        if (bytes_read == 0) {
            self.eof = true;
        }
    }

    /// Read next CSV record using bulk operations
    pub fn readRecord(self: *BulkCsvReader) !?[][]u8 {
        while (true) {
            // Find next newline in buffer
            const search_start = self.line_start;
            if (search_start >= self.buffer_len) {
                // Need more data
                try self.fillBuffer();
                if (self.eof and self.buffer_len == 0) {
                    return null; // No more data
                }
                if (self.line_start >= self.buffer_len) {
                    return null;
                }
                continue;
            }

            const remaining = self.buffer[search_start..self.buffer_len];
            const newline_pos = std.mem.indexOfScalar(u8, remaining, '\n');

            if (newline_pos) |pos| {
                // Found a complete line
                var line_end = search_start + pos;

                // Trim \r if present
                if (line_end > search_start and self.buffer[line_end - 1] == '\r') {
                    line_end -= 1;
                }

                const line = self.buffer[search_start..line_end];
                self.line_start = search_start + pos + 1; // Move past the \n

                // Parse the line by splitting on delimiter
                return try self.parseLine(line);
            } else {
                // No newline found - need more data
                if (self.eof) {
                    // Last line without newline
                    if (self.line_start < self.buffer_len) {
                        const line = self.buffer[self.line_start..self.buffer_len];
                        self.line_start = self.buffer_len;
                        return try self.parseLine(line);
                    }
                    return null;
                }

                // Need to read more data
                try self.fillBuffer();
                if (self.eof and self.line_start >= self.buffer_len) {
                    return null;
                }
            }
        }
    }

    fn parseLine(self: *BulkCsvReader, line: []const u8) ![][]u8 {
        var fields = std.ArrayList([]u8){};
        errdefer {
            for (fields.items) |field| {
                self.allocator.free(field);
            }
            fields.deinit(self.allocator);
        }

        // Fast path: split by delimiter
        var iter = std.mem.splitScalar(u8, line, self.delimiter);
        while (iter.next()) |field| {
            // Duplicate the field
            const field_copy = try self.allocator.dupe(u8, field);
            try fields.append(self.allocator, field_copy);
        }

        return try fields.toOwnedSlice(self.allocator);
    }

    /// Free a record returned by readRecord
    pub fn freeRecord(self: *BulkCsvReader, record: [][]u8) void {
        for (record) |field| {
            self.allocator.free(field);
        }
        self.allocator.free(record);
    }
};
