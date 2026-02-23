const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const Allocator = std.mem.Allocator;

/// Columnar processing - stores data in column arrays for vectorized operations
pub const ColumnStore = struct {
    columns: []std.ArrayList([]const u8),
    row_count: usize,
    allocator: Allocator,

    pub fn init(allocator: Allocator, num_columns: usize) !ColumnStore {
        const columns = try allocator.alloc(std.ArrayList([]const u8), num_columns);
        for (columns) |*col| {
            col.* = std.ArrayList([]const u8){};
        }
        return ColumnStore{
            .columns = columns,
            .row_count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ColumnStore) void {
        for (self.columns) |*col| {
            for (col.items) |field| {
                self.allocator.free(field);
            }
            col.deinit(self.allocator);
        }
        self.allocator.free(self.columns);
    }

    pub fn addRow(self: *ColumnStore, fields: []const []const u8) !void {
        for (fields, 0..) |field, i| {
            if (i < self.columns.len) {
                try self.columns[i].append(self.allocator, field);
            }
        }
        self.row_count += 1;
    }

    /// Evaluate WHERE clause using vectorized operations on columns
    pub fn evaluateWhereVectorized(
        self: *ColumnStore,
        expr: parser.Expression,
        lower_header: []const []const u8,
        allocator: Allocator,
    ) ![]bool {
        var results = try allocator.alloc(bool, self.row_count);
        @memset(results, true);

        // Find the column being filtered
        if (expr == .comparison) {
            const comp = expr.comparison;

            // Find column index
            var col_idx: ?usize = null;
            for (lower_header, 0..) |name, idx| {
                if (std.mem.eql(u8, name, comp.column)) {
                    col_idx = idx;
                    break;
                }
            }

            if (col_idx) |idx| {
                if (idx < self.columns.len) {
                    const column = self.columns[idx];

                    // Vectorized numeric comparison if possible
                    if (comp.numeric_value) |threshold| {
                        try evaluateNumericColumnSIMD(column.items, threshold, comp.operator, results);
                    } else {
                        // String comparison (scalar)
                        for (column.items, 0..) |value, i| {
                            results[i] = compareStrings(value, comp.value, comp.operator);
                        }
                    }
                }
            }
        }

        return results;
    }
};

/// SIMD-accelerated numeric comparison on entire column
fn evaluateNumericColumnSIMD(
    column: []const []const u8,
    threshold: f64,
    op: parser.Operator,
    results: []bool,
) !void {
    const VecSize = 8; // Process 8 values at once using AVX
    const Vec = @Vector(VecSize, f64);
    const BoolVec = @Vector(VecSize, bool);

    const threshold_vec: Vec = @splat(threshold);

    var i: usize = 0;

    // Process in SIMD chunks
    while (i + VecSize <= column.len) : (i += VecSize) {
        var values: Vec = undefined;
        var valid: BoolVec = @splat(true);

        // Parse 8 values
        for (0..VecSize) |j| {
            const val = std.fmt.parseFloat(f64, column[i + j]) catch {
                valid[j] = false;
                values[j] = 0.0;
                continue;
            };
            values[j] = val;
        }

        // Vectorized comparison
        const comparison_result: BoolVec = switch (op) {
            .greater => values > threshold_vec,
            .greater_equal => values >= threshold_vec,
            .less => values < threshold_vec,
            .less_equal => values <= threshold_vec,
            .equal => values == threshold_vec,
            .not_equal => values != threshold_vec,
        };

        // Apply valid mask and store results
        for (0..VecSize) |j| {
            results[i + j] = valid[j] and comparison_result[j];
        }
    }

    // Handle remaining elements (scalar)
    while (i < column.len) : (i += 1) {
        const val = std.fmt.parseFloat(f64, column[i]) catch {
            results[i] = false;
            continue;
        };
        results[i] = switch (op) {
            .greater => val > threshold,
            .greater_equal => val >= threshold,
            .less => val < threshold,
            .less_equal => val <= threshold,
            .equal => val == threshold,
            .not_equal => val != threshold,
        };
    }
}

fn compareStrings(a: []const u8, b: []const u8, op: parser.Operator) bool {
    const cmp = std.mem.order(u8, a, b);
    return switch (op) {
        .equal => cmp == .eq,
        .not_equal => cmp != .eq,
        .greater => cmp == .gt,
        .greater_equal => cmp == .gt or cmp == .eq,
        .less => cmp == .lt,
        .less_equal => cmp == .lt or cmp == .eq,
    };
}

/// Columnar parallel processing - combines columnar storage with parallel execution
pub fn executeColumnar(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
) !void {
    const file_size = (try input_file.stat()).size;

    // Memory-map the entire file
    const mapped = try std.posix.mmap(
        null,
        file_size,
        std.posix.PROT.READ,
        .{ .TYPE = .SHARED },
        input_file.handle,
        0,
    );
    defer std.posix.munmap(mapped);

    const data = mapped[0..file_size];

    // Parse header
    const header_end = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    const header_line = data[0..header_end];

    var header = std.ArrayList([]const u8){};
    defer header.deinit(allocator);

    var header_iter = std.mem.splitScalar(u8, header_line, ',');
    while (header_iter.next()) |col| {
        try header.append(allocator, col);
    }

    // Build lowercase header
    var lower_header = try allocator.alloc([]u8, header.items.len);
    defer {
        for (lower_header) |name| {
            allocator.free(name);
        }
        allocator.free(lower_header);
    }

    for (header.items, 0..) |col_name, idx| {
        const lower_name = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower_name, col_name);
        lower_header[idx] = lower_name;
    }

    // Create column store
    var store = try ColumnStore.init(allocator, header.items.len);
    defer store.deinit();

    // Parse all data into columns (first pass - data loading)
    const data_start = header_end + 1;
    var line_start: usize = data_start;
    var row_count: usize = 0;
    const max_rows: usize = 1000000; // Limit for memory

    while (line_start < data.len and row_count < max_rows) {
        const remaining = data[line_start..];
        const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse data.len - line_start;

        var line = remaining[0..line_end];
        if (line.len > 0 and line[line.len - 1] == '\r') {
            line = line[0 .. line.len - 1];
        }

        if (line.len > 0) {
            var fields = std.ArrayList([]const u8){};
            defer fields.deinit(allocator);

            var field_iter = std.mem.splitScalar(u8, line, ',');
            while (field_iter.next()) |field| {
                const field_copy = try allocator.dupe(u8, field);
                try fields.append(allocator, field_copy);
            }

            try store.addRow(fields.items);
            row_count += 1;
        }

        line_start += line_end + 1;
    }

    // Vectorized WHERE evaluation (second pass - filter)
    var filter_results: []bool = undefined;
    var should_free_filter = false;

    if (query.where_expr) |expr| {
        filter_results = try store.evaluateWhereVectorized(expr, lower_header, allocator);
        should_free_filter = true;
    } else {
        filter_results = try allocator.alloc(bool, store.row_count);
        @memset(filter_results, true);
        should_free_filter = true;
    }
    defer if (should_free_filter) allocator.free(filter_results);

    // Determine output columns
    var output_indices = std.ArrayList(usize){};
    defer output_indices.deinit(allocator);

    if (query.all_columns) {
        for (0..header.items.len) |idx| {
            try output_indices.append(allocator, idx);
        }
    } else {
        for (query.columns) |col| {
            const lower_col = try allocator.alloc(u8, col.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, col);

            var found = false;
            for (lower_header, 0..) |name, idx| {
                if (std.mem.eql(u8, name, lower_col)) {
                    try output_indices.append(allocator, idx);
                    found = true;
                    break;
                }
            }
            if (!found) return error.ColumnNotFound;
        }
    }

    // Write header
    var writer = csv.CsvWriter.init(output_file);
    var output_header = try allocator.alloc([]const u8, output_indices.items.len);
    defer allocator.free(output_header);

    for (output_indices.items, 0..) |idx, i| {
        output_header[i] = header.items[idx];
    }
    try writer.writeRecord(output_header);

    // Write filtered rows (third pass - output projection)
    var rows_written: i32 = 0;
    for (filter_results, 0..) |should_output, row_idx| {
        if (should_output) {
            var output_row = try allocator.alloc([]const u8, output_indices.items.len);
            defer allocator.free(output_row);

            for (output_indices.items, 0..) |col_idx, i| {
                if (col_idx < store.columns.len and row_idx < store.columns[col_idx].items.len) {
                    output_row[i] = store.columns[col_idx].items[row_idx];
                } else {
                    output_row[i] = "";
                }
            }

            try writer.writeRecord(output_row);
            rows_written += 1;

            if (query.limit >= 0 and rows_written >= query.limit) {
                break;
            }
        }
    }

    try writer.flush();
}
