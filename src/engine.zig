const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const bulk_csv = @import("bulk_csv.zig");
const mmap_engine = @import("mmap_engine.zig");
const parallel_mmap = @import("parallel_mmap.zig");
const columnar = @import("columnar.zig");
const parallel = @import("parallel.zig");
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

    // Check file size for processing strategy
    const file_stat = try file.stat();

    // Check if query would benefit from columnar processing (numeric WHERE on large data)
    const use_columnar = file_stat.size > 200 * 1024 * 1024 and // > 200MB (TEMP: disabled for testing)
        query.where_expr != null and
        query.where_expr.? == .comparison and
        query.where_expr.?.comparison.numeric_value != null and
        (query.limit < 0 or query.limit > 50000);

    if (use_columnar) {
        // Columnar processing with SIMD vectorization
        try columnar.executeColumnar(allocator, query, file, output_file);
        return;
    }

    // Use parallel memory-mapped I/O for large files (2+ cores, no LIMIT)
    if (file_stat.size > 10 * 1024 * 1024 and (query.limit < 0 or query.limit > 100000)) {
        const num_cores = try std.Thread.getCpuCount();
        if (num_cores > 1) {
            try parallel_mmap.executeParallelMapped(allocator, query, file, output_file);
            return;
        }
    }

    // Use memory-mapped I/O for medium-large files
    if (file_stat.size > 5 * 1024 * 1024) {
        try mmap_engine.executeMapped(allocator, query, file, output_file);
        return;
    }

    // Sequential execution for smaller files
    try executeSequential(allocator, query, file, output_file);
}

/// Execute query sequentially
fn executeSequential(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
) !void {
    // Use bulk CSV reader for much better performance
    var reader = try bulk_csv.BulkCsvReader.init(allocator, input_file);
    defer reader.deinit();

    var writer = csv.CsvWriter.init(output_file);

    // Read header
    const header = try reader.readRecord() orelse return error.EmptyFile;
    defer reader.freeRecord(header);

    // Build column index map (case-insensitive)
    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();

    // Build lowercase header once for WHERE clause evaluation
    var lower_header = try allocator.alloc([]u8, header.len);
    defer {
        for (lower_header) |lower_name| {
            allocator.free(lower_name);
        }
        allocator.free(lower_header);
    }

    for (header, 0..) |col_name, idx| {
        // Store lowercase version for case-insensitive lookup
        const lower_name = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower_name, col_name);
        lower_header[idx] = lower_name;
        try column_map.put(lower_name, idx);
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

            // Build row map using pre-computed lowercase column names
            for (lower_header, 0..) |lower_name, idx| {
                if (idx < record.len) {
                    try row_map.put(lower_name, record[idx]);
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

        // Flush periodically (less often with 1MB buffer)
        if (@rem(rows_written, 32768) == 0) {
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

    // Build lowercase header once for WHERE clause evaluation
    var lower_header = try allocator.alloc([]u8, header.len);
    defer {
        for (lower_header) |lower_name| {
            allocator.free(lower_name);
        }
        allocator.free(lower_header);
    }

    for (header, 0..) |col_name, idx| {
        const lower_name = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower_name, col_name);
        lower_header[idx] = lower_name;
        try column_map.put(lower_name, idx);
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

            // Build row map using pre-computed lowercase column names
            for (lower_header, 0..) |lower_name, idx| {
                if (idx < record.len) {
                    try row_map.put(lower_name, record[idx]);
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
