const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const Allocator = std.mem.Allocator;

/// Execute a SQL query on a CSV file
pub fn execute(allocator: Allocator, query: parser.Query, output_file: std.fs.File) !void {
    // Check if reading from stdin
    const is_stdin = std.mem.eql(u8, query.file_path, "-") or std.mem.eql(u8, query.file_path, "stdin");

    if (is_stdin) {
        try executeFromStdin(allocator, query, output_file);
        return;
    }

    // Check for GROUP BY - requires sequential processing
    if (query.group_by.len > 0) {
        try executeGroupBy(allocator, query, output_file);
        return;
    }

    // Open CSV file
    const file = try std.fs.cwd().openFile(query.file_path, .{});
    defer file.close();

    // Check file size for parallel processing decision
    const file_stat = try file.stat();
    const use_parallel = file_stat.size > 10 * 1024 * 1024 and // > 10MB
        (query.limit < 0 or query.limit >= 10000); // Large or no limit

    if (use_parallel) {
        // TODO: Implement parallel execution
        std.debug.print("Parallel execution not yet implemented, falling back to sequential\n", .{});
    }

    // Sequential execution
    try executeSequential(allocator, query, file, output_file);
}

/// Execute query sequentially
fn executeSequential(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
) !void {
    var reader = csv.CsvReader.init(allocator, input_file);
    var writer = csv.CsvWriter.init(output_file);

    // Read header
    const header = try reader.readRecord() orelse return error.EmptyFile;
    defer reader.freeRecord(header);

    // Build column index map (case-insensitive)
    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();

    for (header, 0..) |col_name, idx| {
        // Store lowercase version for case-insensitive lookup
        const lower_name = try allocator.alloc(u8, col_name.len);
        defer allocator.free(lower_name);
        _ = std.ascii.lowerString(lower_name, col_name);
        try column_map.put(try allocator.dupe(u8, lower_name), idx);
    }
    defer {
        var it = column_map.keyIterator();
        while (it.next()) |key| {
            allocator.free(key.*);
        }
    }

    // Determine output columns
    var output_indices = std.ArrayList(usize){};
    defer output_indices.deinit(allocator);

    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (header, 0..) |col_name, idx| {
            try output_indices.append(allocator, idx);
            try output_header.append(allocator, col_name);
        }
    } else {
        for (query.columns) |col| {
            const lower_col = try allocator.alloc(u8, col.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, col);

            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_indices.append(allocator, idx);
            try output_header.append(allocator, header[idx]);
        }
    }

    // Write output header
    try writer.writeRecord(output_header.items);

    // Process rows
    var row_count: i32 = 0;
    var rows_written: i32 = 0;

    while (try reader.readRecord()) |record| {
        defer reader.freeRecord(record);
        row_count += 1;

        // Evaluate WHERE clause if present
        if (query.where_expr != null) {
            var row_map = std.StringHashMap([]const u8).init(allocator);
            defer row_map.deinit();

            // Build row map with lowercase column names
            for (header, 0..) |col_name, idx| {
                if (idx < record.len) {
                    const lower_name = try allocator.alloc(u8, col_name.len);
                    defer allocator.free(lower_name);
                    _ = std.ascii.lowerString(lower_name, col_name);
                    try row_map.put(try allocator.dupe(u8, lower_name), record[idx]);
                }
            }
            defer {
                var it = row_map.keyIterator();
                while (it.next()) |key| {
                    allocator.free(key.*);
                }
            }

            if (!parser.evaluate(query.where_expr.?, row_map)) {
                continue;
            }
        }

        // Project selected columns
        var output_row = try allocator.alloc([]const u8, output_indices.items.len);
        defer allocator.free(output_row);

        for (output_indices.items, 0..) |idx, i| {
            output_row[i] = if (idx < record.len) record[idx] else "";
        }

        try writer.writeRecord(output_row);
        rows_written += 1;

        // Check LIMIT
        if (query.limit >= 0 and rows_written >= query.limit) {
            break;
        }

        // Flush periodically
        if (@rem(rows_written, 8192) == 0) {
            try writer.flush();
        }
    }

    try writer.flush();
}

/// Execute query from stdin
fn executeFromStdin(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
) !void {
    const stdin = std.fs.File{ .handle = std.posix.STDIN_FILENO };
    var reader = csv.CsvReader.init(allocator, stdin);
    var writer = csv.CsvWriter.init(output_file);

    // Read header
    const header = try reader.readRecord() orelse return error.EmptyFile;
    defer reader.freeRecord(header);

    // Build column index map (case-insensitive)
    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();

    for (header, 0..) |col_name, idx| {
        const lower_name = try allocator.alloc(u8, col_name.len);
        defer allocator.free(lower_name);
        _ = std.ascii.lowerString(lower_name, col_name);
        try column_map.put(try allocator.dupe(u8, lower_name), idx);
    }
    defer {
        var it = column_map.keyIterator();
        while (it.next()) |key| {
            allocator.free(key.*);
        }
    }

    // Determine output columns
    var output_indices = std.ArrayList(usize){};
    defer output_indices.deinit(allocator);

    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (header, 0..) |col_name, idx| {
            try output_indices.append(allocator, idx);
            try output_header.append(allocator, col_name);
        }
    } else {
        for (query.columns) |col| {
            const lower_col = try allocator.alloc(u8, col.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, col);

            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_indices.append(allocator, idx);
            try output_header.append(allocator, header[idx]);
        }
    }

    // Write output header
    try writer.writeRecord(output_header.items);

    // Process rows
    var rows_written: i32 = 0;

    while (try reader.readRecord()) |record| {
        defer reader.freeRecord(record);

        // Evaluate WHERE clause if present
        if (query.where_expr != null) {
            var row_map = std.StringHashMap([]const u8).init(allocator);
            defer row_map.deinit();

            for (header, 0..) |col_name, idx| {
                if (idx < record.len) {
                    const lower_name = try allocator.alloc(u8, col_name.len);
                    defer allocator.free(lower_name);
                    _ = std.ascii.lowerString(lower_name, col_name);
                    try row_map.put(try allocator.dupe(u8, lower_name), record[idx]);
                }
            }
            defer {
                var it = row_map.keyIterator();
                while (it.next()) |key| {
                    allocator.free(key.*);
                }
            }

            if (!parser.evaluate(query.where_expr.?, row_map)) {
                continue;
            }
        }

        // Project selected columns
        var output_row = try allocator.alloc([]const u8, output_indices.items.len);
        defer allocator.free(output_row);

        for (output_indices.items, 0..) |idx, i| {
            output_row[i] = if (idx < record.len) record[idx] else "";
        }

        try writer.writeRecord(output_row);
        rows_written += 1;

        if (query.limit >= 0 and rows_written >= query.limit) {
            break;
        }
    }

    try writer.flush();
}

/// Execute GROUP BY query (stub for now)
fn executeGroupBy(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
) !void {
    _ = allocator;
    _ = query;
    _ = output_file;
    // TODO: Implement GROUP BY aggregation
    std.debug.print("GROUP BY not yet implemented\n", .{});
    return error.NotImplemented;
}
