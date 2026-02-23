const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const Allocator = std.mem.Allocator;

/// Work chunk for parallel processing
const WorkChunk = struct {
    start_offset: u64,
    end_offset: u64,
    chunk_id: usize,
};

/// Result from a worker thread
const WorkerResult = struct {
    rows: std.ArrayList([][]u8),
    allocator: Allocator,

    fn deinit(self: *WorkerResult) void {
        for (self.rows.items) |row| {
            for (row) |field| {
                self.allocator.free(field);
            }
            self.allocator.free(row);
        }
        self.rows.deinit(self.allocator);
    }
};

/// Context for worker threads
const WorkerContext = struct {
    allocator: Allocator,
    file_path: []const u8,
    query: parser.Query,
    chunk: WorkChunk,
    header: [][]u8,
    column_map: std.StringHashMap(usize),
    output_indices: []const usize,
    result: ?WorkerResult,
    error_info: ?anyerror,
    mutex: std.Thread.Mutex,
};

/// Execute query in parallel across multiple threads
pub fn executeParallel(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
    num_threads: usize,
) !void {
    // Get file size
    const file_size = (try input_file.stat()).size;

    // Read header first
    try input_file.seekTo(0);
    var header_reader = csv.CsvReader.init(allocator, input_file);
    const header = try header_reader.readRecord() orelse return error.EmptyFile;
    defer {
        for (header) |col| {
            allocator.free(col);
        }
        allocator.free(header);
    }

    // Build column map
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
    var writer = csv.CsvWriter.init(output_file);
    try writer.writeRecord(output_header.items);

    // Find header end position
    const header_end = try input_file.getPos();

    // Calculate chunk size
    const data_size = file_size - header_end;
    const chunk_size = data_size / num_threads;

    // Create work chunks
    var chunks = std.ArrayList(WorkChunk){};
    defer chunks.deinit(allocator);

    var offset = header_end;
    var chunk_id: usize = 0;
    while (offset < file_size) : (chunk_id += 1) {
        const end = @min(offset + chunk_size, file_size);
        try chunks.append(allocator, WorkChunk{
            .start_offset = offset,
            .end_offset = end,
            .chunk_id = chunk_id,
        });
        offset = end;
    }

    // Create worker contexts
    var contexts = try allocator.alloc(WorkerContext, chunks.items.len);
    defer allocator.free(contexts);

    for (chunks.items, 0..) |chunk, i| {
        contexts[i] = WorkerContext{
            .allocator = allocator,
            .file_path = query.file_path,
            .query = query,
            .chunk = chunk,
            .header = header,
            .column_map = column_map,
            .output_indices = output_indices.items,
            .result = null,
            .error_info = null,
            .mutex = std.Thread.Mutex{},
        };
    }

    // Spawn worker threads
    var threads = try allocator.alloc(std.Thread, chunks.items.len);
    defer allocator.free(threads);

    for (contexts, 0..) |*ctx, i| {
        threads[i] = try std.Thread.spawn(.{}, workerThread, .{ctx});
    }

    // Wait for all threads
    for (threads) |thread| {
        thread.join();
    }

    // Collect and write results
    var total_rows: i32 = 0;
    for (contexts) |*ctx| {
        if (ctx.error_info) |err| {
            return err;
        }

        if (ctx.result) |*result| {
            defer result.deinit();

            for (result.rows.items) |row| {
                // Project output columns
                var output_row = try allocator.alloc([]const u8, output_indices.items.len);
                defer allocator.free(output_row);

                for (output_indices.items, 0..) |idx, i| {
                    output_row[i] = if (idx < row.len) row[idx] else "";
                }

                try writer.writeRecord(output_row);
                total_rows += 1;

                // Check LIMIT
                if (query.limit >= 0 and total_rows >= query.limit) {
                    break;
                }
            }

            if (query.limit >= 0 and total_rows >= query.limit) {
                break;
            }
        }
    }

    try writer.flush();
}

/// Worker thread function
fn workerThread(ctx: *WorkerContext) void {
    processChunk(ctx) catch |err| {
        ctx.mutex.lock();
        defer ctx.mutex.unlock();
        ctx.error_info = err;
    };
}

/// Process a single chunk
fn processChunk(ctx: *WorkerContext) !void {
    const allocator = ctx.allocator;

    // Open file
    const file = try std.fs.cwd().openFile(ctx.file_path, .{});
    defer file.close();

    // Seek to chunk start
    try file.seekTo(ctx.chunk.start_offset);

    // If not first chunk, skip to next line to align with row boundary
    if (ctx.chunk.start_offset > 0) {
        var buf: [1]u8 = undefined;
        while (true) {
            const n = try file.read(&buf);
            if (n == 0) break;
            if (buf[0] == '\n') break;
        }
    }

    var reader = csv.CsvReader.init(allocator, file);
    var result = WorkerResult{
        .rows = std.ArrayList([][]u8){},
        .allocator = allocator,
    };

    // Process rows in this chunk
    while (try reader.readRecord()) |record| {
        const current_pos = try file.getPos();

        // Stop if we've passed our chunk boundary
        if (current_pos > ctx.chunk.end_offset) {
            reader.freeRecord(record);
            break;
        }

        // Evaluate WHERE clause
        var should_include = true;
        if (ctx.query.where_expr != null) {
            var row_map = std.StringHashMap([]const u8).init(allocator);
            defer row_map.deinit();

            for (ctx.header, 0..) |col_name, idx| {
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

            should_include = parser.evaluate(ctx.query.where_expr.?, row_map);
        }

        if (should_include) {
            try result.rows.append(allocator, record);
        } else {
            reader.freeRecord(record);
        }
    }

    ctx.result = result;
}
