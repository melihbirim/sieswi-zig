const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const Allocator = std.mem.Allocator;

/// Memory-mapped parallel CSV processing
pub fn executeMapped(
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
    
    // Process data starting after header
    const data_start = header_end + 1;
    var rows_written: i32 = 0;
    
    // Split into lines using bulk operations
    var line_start: usize = data_start;
    while (line_start < data.len) {
        const remaining = data[line_start..];
        const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse data.len - line_start;
        
        var line = remaining[0..line_end];
        // Trim \r if present
        if (line.len > 0 and line[line.len - 1] == '\r') {
            line = line[0 .. line.len - 1];
        }
        
        if (line.len > 0) {
            // Parse fields
            var fields = std.ArrayList([]const u8){};
            defer fields.deinit(allocator);
            
            var field_iter = std.mem.splitScalar(u8, line, ',');
            while (field_iter.next()) |field| {
                try fields.append(allocator, field);
            }
            
            // Evaluate WHERE clause
            if (query.where_expr) |expr| {
                var row_map = std.StringHashMap([]const u8).init(allocator);
                defer row_map.deinit();
                
                for (lower_header, 0..) |lower_name, idx| {
                    if (idx < fields.items.len) {
                        try row_map.put(lower_name, fields.items[idx]);
                    }
                }
                
                if (!parser.evaluate(expr, row_map)) {
                    line_start += line_end + 1;
                    continue;
                }
            }
            
            // Project output columns
            var output_row = try allocator.alloc([]const u8, output_indices.items.len);
            defer allocator.free(output_row);
            
            for (output_indices.items, 0..) |idx, i| {
                output_row[i] = if (idx < fields.items.len) fields.items[idx] else "";
            }
            
            try writer.writeRecord(output_row);
            rows_written += 1;
            
            if (query.limit >= 0 and rows_written >= query.limit) {
                break;
            }
            
            // Flush periodically
            if (@rem(rows_written, 32768) == 0) {
                try writer.flush();
            }
        }
        
        line_start += line_end + 1;
    }
    
    try writer.flush();
}
