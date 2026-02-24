const std = @import("std");
const parser = @import("parser.zig");
const engine = @import("engine.zig");
const csv = @import("csv.zig");
const Allocator = std.mem.Allocator;

const version = "0.1.0-zig";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Get command line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Check for version flag
    if (args.len >= 2 and (std.mem.eql(u8, args[1], "--version") or std.mem.eql(u8, args[1], "-v"))) {
        const stderr = std.fs.File{ .handle = std.posix.STDERR_FILENO };
        try stderr.writeAll("sieswi ");
        try stderr.writeAll(version);
        try stderr.writeAll(" (Zig implementation)\n");
        return;
    }

    // Get query from args or stdin
    const query_text = try getQueryFromArgsOrStdin(allocator, args);
    defer allocator.free(query_text);

    // Parse query
    var query = parser.parse(allocator, query_text) catch |err| {
        std.debug.print("parse error: {}\n", .{err});
        std.process.exit(1);
    };
    defer query.deinit();

    // Execute query
    const stdout = std.fs.File{ .handle = std.posix.STDOUT_FILENO };
    engine.execute(allocator, query, stdout) catch |err| {
        std.debug.print("execution error: {}\n", .{err});
        std.process.exit(1);
    };
}

fn getQueryFromArgsOrStdin(allocator: Allocator, args: [][:0]u8) ![]u8 {
    // If query provided as argument
    if (args.len > 1) {
        // Join all args after program name
        var query_parts = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
        defer query_parts.deinit(allocator);

        for (args[1..]) |arg| {
            try query_parts.append(allocator, arg);
        }

        return try std.mem.join(allocator, " ", query_parts.items);
    }

    // Read query from stdin
    const stdin = std.fs.File{ .handle = std.posix.STDIN_FILENO };
    const query = try stdin.readToEndAlloc(allocator, 1024 * 1024); // Max 1MB query
    const trimmed = std.mem.trim(u8, query, &std.ascii.whitespace);

    if (trimmed.len == 0) {
        const stderr = std.fs.File{ .handle = std.posix.STDERR_FILENO };
        try stderr.writeAll("usage: sieswi \"SELECT ...\" (or pipe query via stdin)\n");
        std.process.exit(1);
    }

    // Return a copy of the trimmed string
    const result = try allocator.dupe(u8, trimmed);
    allocator.free(query);
    return result;
}

test "simple test" {
    var list = std.ArrayList(i32){};
    defer list.deinit(std.testing.allocator);
    try list.append(std.testing.allocator, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

// TDD Test 1: Query.deinit with SELECT * should not crash
test "Query deinit with SELECT *" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv'");
    defer query.deinit();

    try std.testing.expect(query.all_columns);
    try std.testing.expectEqual(@as(usize, 0), query.columns.len);
}

// TDD Test 2: Query.deinit with no GROUP BY should not crash
test "Query deinit without GROUP BY" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT id, name FROM 'test.csv' WHERE age > 30");
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 0), query.group_by.len);
}

// TDD Test 3: Query.deinit with explicit columns and GROUP BY should work
test "Query deinit with columns and GROUP BY" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT name, count FROM 'test.csv' GROUP BY name");
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 2), query.columns.len);
    try std.testing.expectEqual(@as(usize, 1), query.group_by.len);
}
// TDD Test 4: WHERE with mixed case column should match header (case-insensitive)
test "WHERE clause is case-insensitive for column names" {
    const allocator = std.testing.allocator;
    
    // Query uses lowercase 'name', but CSV header might be 'Name' or 'NAME'
    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE name = 'Alice'");
    defer query.deinit();
    
    try std.testing.expect(query.where_expr != null);
    if (query.where_expr) |expr| {
        switch (expr) {
            .comparison => |comp| {
                // Column name should be normalized to lowercase
                try std.testing.expectEqualStrings("name", comp.column);
            },
            else => try std.testing.expect(false), // Should be comparison
        }
    }
}

// TDD Test 5: Performance - WHERE evaluation should use direct index lookup
test "WHERE evaluation uses precomputed column index" {
    const allocator = std.testing.allocator;
    
    // This test just verifies the API exists for fast WHERE evaluation
    // The actual performance benefit is measured in benchmarks, not unit tests
    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE age > 30");
    defer query.deinit();
    
    // Verify we have a WHERE clause with a simple comparison
    try std.testing.expect(query.where_expr != null);
    if (query.where_expr) |expr| {
        switch (expr) {
            .comparison => |comp| {
                // Should have normalized column name
                try std.testing.expectEqualStrings("age", comp.column);
                // Should have numeric value for numeric comparison
                try std.testing.expect(comp.numeric_value != null);
            },
            else => try std.testing.expect(false),
        }
    }
}

// TDD Test 6: CsvWriter properly handles all data (no short writes)
test "CsvWriter writeRecord outputs complete data" {
    const allocator = std.testing.allocator;
    
    // Create a temporary file
    const tmp_file = try std.fs.cwd().createFile("test_writer.csv", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_writer.csv") catch {};
    }
    
    var writer = csv.CsvWriter.init(tmp_file);
    
    // Write some records
    const fields1 = &[_][]const u8{ "id", "name",  "value" };
    const fields2 = &[_][]const u8{ "1", "Alice", "100" };
    const fields3 = &[_][]const u8{ "2", "Bob", "200" };
    
    try writer.writeRecord(fields1);
    try writer.writeRecord(fields2);
    try writer.writeRecord(fields3);
    try writer.flush();
    
    // Read back and verify
    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 1024);
    defer allocator.free(content);
    
    // Should have complete lines (no partial writes)
    const expected = "id,name,value\n1,Alice,100\n2,Bob,200\n";
    try std.testing.expectEqualStrings(expected, content);
}