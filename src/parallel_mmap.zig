const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const Allocator = std.mem.Allocator;

const WorkChunk = struct {
    start: usize,
    end: usize,
    thread_id: usize,
};

const WorkerResult = struct {
    lines: std.ArrayList([]const u8),
    allocator: Allocator,
    
    fn deinit(self: *WorkerResult) void {
        for (self.lines.items) |line| {
            self.allocator.free(line);
        }
        self.lines.deinit(self.allocator);
    }
};

const WorkerContext = struct {
    data: []const u8,
    chunk: WorkChunk,
    query: parser.Query,
    lower_header: []const []const u8,
    output_indices: []const usize,
    result: std.ArrayList([]const u8),
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
            .result = std.ArrayList([]const u8){},
            .allocator = allocator,
            .mutex = &mutex,
        };
        
        threads[i] = try std.Thread.spawn(.{}, workerThread, .{&contexts[i]});
    }
    
    // Wait for all threads
    for (threads) |thread| {
        thread.join();
    }
    
    // Collect and write results
    var total_written: usize = 0;
    for (contexts) |*ctx| {
        defer ctx.result.deinit(allocator);
        
        for (ctx.result.items) |line| {
            defer allocator.free(line);
            
            // Parse and write the line
            var fields = std.ArrayList([]const u8){};
            defer fields.deinit(allocator);
            
            var field_iter = std.mem.splitScalar(u8, line, ',');
            while (field_iter.next()) |field| {
                try fields.append(allocator, field);
            }
            
            // Project output columns
            var output_row = try allocator.alloc([]const u8, output_indices.items.len);
            defer allocator.free(output_row);
            
            for (output_indices.items, 0..) |idx, j| {
                output_row[j] = if (idx < fields.items.len) fields.items[idx] else "";
            }
            
            try writer.writeRecord(output_row);
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
    // Reuse row_map
    var row_map = std.StringHashMap([]const u8).init(arena_alloc);
    
    var line_start: usize = 0;
    while (line_start < chunk_data.len) {
        // Use SIMD-friendly bulk search for newline
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
            
            // Parse fields (zero-copy - these point into mmap)
            var field_iter = std.mem.splitScalar(u8, line, ',');
            while (field_iter.next()) |field| {
                try fields.append(arena_alloc, field);
            }
            
            // Evaluate WHERE clause
            if (ctx.query.where_expr) |expr| {
                row_map.clearRetainingCapacity();
                
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
            
            // Save matching line (need to copy since it's in mmap and may be unmapped)
            const line_copy = try ctx.allocator.dupe(u8, line);
            try ctx.result.append(ctx.allocator, line_copy);
        }
        
        line_start += line_end + 1;
    }
}
