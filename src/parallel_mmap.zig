const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const simd = @import("simd.zig");
const Allocator = std.mem.Allocator;

const WorkChunk = struct {
    start: usize,
    end: usize,
    thread_id: usize,
};

const WorkerResult = struct {
    // Store output rows directly as field arrays (not full CSV lines)
    rows: std.ArrayList([][]const u8),
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

const WorkerContext = struct {
    data: []const u8,
    chunk: WorkChunk,
    query: parser.Query,
    lower_header: []const []const u8,
    output_indices: []const usize,
    where_column_idx: ?usize, // Column index for WHERE clause (if applicable)
    result: std.ArrayList([][]const u8),
    allocator: Allocator,
    mutex: *std.Thread.Mutex,
};

/// Parallel memory-mapped CSV processing
pub fn executeParallelMapped(
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

    // Find end of header line
    const header_end = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    const header_line = data[0..header_end];

    // Parse header
    var header = std.ArrayList([]const u8){};
    defer header.deinit(allocator);

    var header_iter = std.mem.splitScalar(u8, header_line, ',');
    while (header_iter.next()) |col| {
        try header.append(allocator, col);
    }

    // Build column map
    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();

    var lower_header = try allocator.alloc([]u8, header.items.len);
    defer {
        for (lower_header) |lower_name| {
            allocator.free(lower_name);
        }
        allocator.free(lower_header);
    }

    for (header.items, 0..) |col_name, idx| {
        const lower_name = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower_name, col_name);
        lower_header[idx] = lower_name;
        try column_map.put(lower_name, idx);
    }

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
            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_indices.append(allocator, idx);
        }
    }

    // Write output header
    var writer = csv.CsvWriter.init(output_file);
    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    for (output_indices.items) |idx| {
        try output_header.append(allocator, header.items[idx]);
    }
    try writer.writeRecord(output_header.items);

    // Find WHERE column index for fast lookup (avoid HashMap in hot path)
    var where_column_idx: ?usize = null;
    if (query.where_expr) |expr| {
        if (expr == .comparison) {
            const comp = expr.comparison;
            // Find the column index
            for (lower_header, 0..) |name, idx| {
                if (std.mem.eql(u8, name, comp.column)) {
                    where_column_idx = idx;
                    break;
                }
            }
        }
    }

    // Process data in parallel
    const data_start = header_end + 1;
    const data_len = data.len - data_start;

    // Get number of threads
    const num_cores = try std.Thread.getCpuCount();
    const num_threads = @min(num_cores, 8);

    // Split into chunks
    const chunk_size = data_len / num_threads;
    var chunks = std.ArrayList(WorkChunk){};
    defer chunks.deinit(allocator);

    for (0..num_threads) |i| {
        var start = data_start + (i * chunk_size);
        var end = if (i == num_threads - 1) data.len else data_start + ((i + 1) * chunk_size);

        // Adjust chunk boundaries to line boundaries
        if (i > 0) {
            // Find start of next line
            if (std.mem.indexOfScalarPos(u8, data, start, '\n')) |newline| {
                start = newline + 1;
            }
        }

        if (i < num_threads - 1) {
            // Find end of current line
            if (std.mem.indexOfScalarPos(u8, data, end, '\n')) |newline| {
                end = newline + 1;
            }
        }

        try chunks.append(allocator, WorkChunk{
            .start = start,
            .end = end,
            .thread_id = i,
        });
    }

    // Process chunks in parallel
    var threads = try allocator.alloc(std.Thread, num_threads);
    defer allocator.free(threads);

    var contexts = try allocator.alloc(WorkerContext, num_threads);
    defer allocator.free(contexts);

    var mutex = std.Thread.Mutex{};

    for (0..num_threads) |i| {
        contexts[i] = WorkerContext{
            .data = data,
            .chunk = chunks.items[i],
            .query = query,
            .lower_header = lower_header,
            .output_indices = output_indices.items,
            .where_column_idx = where_column_idx,
            .result = std.ArrayList([][]const u8){},
            .allocator = allocator,
            .mutex = &mutex,
        };

        threads[i] = try std.Thread.spawn(.{}, workerThread, .{&contexts[i]});
    }

    // Wait for all threads
    for (threads) |thread| {
        thread.join();
    }

    // Collect and write results (fields are already parsed!)
    var total_written: usize = 0;
    for (contexts) |*ctx| {
        defer ctx.result.deinit(allocator);

        for (ctx.result.items) |row| {
            defer {
                for (row) |field| {
                    allocator.free(field);
                }
                allocator.free(row);
            }

            // Rows are already in output format - just write them
            try writer.writeRecord(row);
            total_written += 1;

            if (query.limit >= 0 and total_written >= @as(usize, @intCast(query.limit))) {
                break;
            }
        }

        if (query.limit >= 0 and total_written >= @as(usize, @intCast(query.limit))) {
            break;
        }
    }

    try writer.flush();
}

fn workerThread(ctx: *WorkerContext) void {
    processChunk(ctx) catch |err| {
        std.debug.print("Worker thread error: {}\n", .{err});
    };
}

fn processChunk(ctx: *WorkerContext) !void {
    const chunk_data = ctx.data[ctx.chunk.start..ctx.chunk.end];

    // Use arena allocator for temporary allocations
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    // Preallocate fields array (assume max 20 columns)
    var fields = try std.ArrayList([]const u8).initCapacity(arena_alloc, 20);

    var line_start: usize = 0;
    while (line_start < chunk_data.len) {
        const remaining = chunk_data[line_start..];
        const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse chunk_data.len - line_start;

        var line = remaining[0..line_end];
        // Trim \r if present
        if (line.len > 0 and line[line.len - 1] == '\r') {
            line = line[0 .. line.len - 1];
        }

        if (line.len > 0) {
            // Reset for reuse
            fields.clearRetainingCapacity();

            // SIMD-accelerated CSV field parsing
            try simd.parseCSVFields(line, &fields, arena_alloc);

            // Fast WHERE evaluation using direct index lookup (avoid HashMap!)
            if (ctx.query.where_expr) |expr| {
                if (expr == .comparison) {
                    const comp = expr.comparison;

                    // Direct column access by index
                    if (ctx.where_column_idx) |col_idx| {
                        if (col_idx < fields.items.len) {
                            const field_value = fields.items[col_idx];

                            // Fast numeric comparison
                            if (comp.numeric_value) |threshold| {
                                const val = std.fmt.parseFloat(f64, field_value) catch {
                                    line_start += line_end + 1;
                                    continue;
                                };

                                const matches = switch (comp.operator) {
                                    .equal => val == threshold,
                                    .not_equal => val != threshold,
                                    .greater => val > threshold,
                                    .greater_equal => val >= threshold,
                                    .less => val < threshold,
                                    .less_equal => val <= threshold,
                                };

                                if (!matches) {
                                    line_start += line_end + 1;
                                    continue;
                                }
                            } else {
                                // String comparison
                                const matches = switch (comp.operator) {
                                    .equal => std.mem.eql(u8, field_value, comp.value),
                                    .not_equal => !std.mem.eql(u8, field_value, comp.value),
                                    else => blk: {
                                        const cmp = std.mem.order(u8, field_value, comp.value);
                                        break :blk switch (comp.operator) {
                                            .greater => cmp == .gt,
                                            .greater_equal => cmp == .gt or cmp == .eq,
                                            .less => cmp == .lt,
                                            .less_equal => cmp == .lt or cmp == .eq,
                                            else => false,
                                        };
                                    },
                                };

                                if (!matches) {
                                    line_start += line_end + 1;
                                    continue;
                                }
                            }
                        } else {
                            line_start += line_end + 1;
                            continue;
                        }
                    } else {
                        // Fallback to HashMap (complex WHERE clauses)
                        var row_map = std.StringHashMap([]const u8).init(arena_alloc);
                        for (ctx.lower_header, 0..) |lower_name, idx| {
                            if (idx < fields.items.len) {
                                try row_map.put(lower_name, fields.items[idx]);
                            }
                        }
                        if (!parser.evaluate(expr, row_map)) {
                            line_start += line_end + 1;
                            continue;
                        }
                    }
                } else {
                    // Complex expression - use HashMap fallback
                    var row_map = std.StringHashMap([]const u8).init(arena_alloc);
                    for (ctx.lower_header, 0..) |lower_name, idx| {
                        if (idx < fields.items.len) {
                            try row_map.put(lower_name, fields.items[idx]);
                        }
                    }
                    if (!parser.evaluate(expr, row_map)) {
                        line_start += line_end + 1;
                        continue;
                    }
                }
            }

            // Build output row directly (project only requested columns)
            var output_row = try ctx.allocator.alloc([]const u8, ctx.output_indices.len);
            for (ctx.output_indices, 0..) |idx, j| {
                const field = if (idx < fields.items.len) fields.items[idx] else "";
                output_row[j] = try ctx.allocator.dupe(u8, field);
            }

            try ctx.result.append(ctx.allocator, output_row);
        }

        line_start += line_end + 1;
    }
}
