const std = @import("std");
const parser = @import("parser.zig");
const simple_parser = @import("simple_parser.zig");
const engine = @import("engine.zig");
const Allocator = std.mem.Allocator;

const version = "0.2.1";

const help_text =
    \\csvq — the world's fastest CSV query engine
    \\
    \\USAGE:
    \\  csvq <file> [columns] [where] [limit] [orderby]
    \\  csvq "SELECT ... FROM 'file.csv' ..."
    \\  cat file.csv | csvq "SELECT ... FROM '-' ..."
    \\
    \\SQL MODE:
    \\  csvq "SELECT name, age FROM 'data.csv'"
    \\  csvq "SELECT * FROM 'data.csv' WHERE age > 30"
    \\  csvq "SELECT name FROM 'data.csv' WHERE age > 30 ORDER BY name ASC"
    \\  csvq "SELECT * FROM 'data.csv' ORDER BY salary DESC LIMIT 10"
    \\  csvq "SELECT * FROM 'data.csv' WHERE age > 25 AND city = 'NYC'"
    \\  csvq "SELECT * FROM 'data.csv' WHERE status = 'active' OR score >= 90"
    \\
    \\SIMPLE MODE:
    \\  csvq data.csv                                    # all columns, default limit 10
    \\  csvq data.csv "name,age,city"                    # select columns
    \\  csvq data.csv "*" "age>30"                       # WHERE filter
    \\  csvq data.csv "name,salary" "salary>0" 10 "salary:desc"
    \\  csvq data.csv "*" "" 0 "name:asc"               # 0 = no limit
    \\
    \\PIPE MODE (use '-' as filename):
    \\  cat data.csv | csvq "SELECT name FROM '-' WHERE age > 25"
    \\  tail -f logs.csv | csvq "SELECT * FROM '-' WHERE level = 'ERROR'"
    \\
    \\SUPPORTED SQL:
    \\  SELECT   column list or *
    \\  FROM     'file.csv' (single-quoted path)
    \\  WHERE    comparisons with =, !=, >, >=, <, <=
    \\           combine with AND, OR, NOT, parentheses
    \\           string values: city = 'NYC'
    \\           numeric values: age > 30
    \\  ORDER BY column ASC|DESC
    \\  LIMIT    number of rows
    \\
    \\NOT SUPPORTED:
    \\  JOIN, GROUP BY, HAVING, DISTINCT, subqueries,
    \\  aggregate functions (COUNT, SUM, AVG, etc.),
    \\  multiple ORDER BY columns, LIKE, IN, BETWEEN,
    \\  aliases (AS), UNION, INSERT/UPDATE/DELETE
    \\
    \\OPTIONS:
    \\  -h, --help       Show this help
    \\  -v, --version    Show version
    \\
    \\EXAMPLES:
    \\  csvq "SELECT * FROM 'users.csv' WHERE age >= 18 LIMIT 100"
    \\  csvq "SELECT * FROM 'data.csv' WHERE status = 'active'" > out.csv
    \\  csvq "SELECT email FROM 'users.csv'" | wc -l
    \\
;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Get command line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const stdout_file = std.fs.File{ .handle = std.posix.STDOUT_FILENO };
    const stderr_file = std.fs.File{ .handle = std.posix.STDERR_FILENO };

    // Check for flags
    if (args.len >= 2) {
        if (std.mem.eql(u8, args[1], "--version") or std.mem.eql(u8, args[1], "-v")) {
            try stderr_file.writeAll("csvq " ++ version ++ "\n");
            return;
        }
        if (std.mem.eql(u8, args[1], "--help") or std.mem.eql(u8, args[1], "-h")) {
            try stdout_file.writeAll(help_text);
            return;
        }
    }

    // Detect query mode: simple vs SQL
    var query = if (args.len > 1 and !isSQL(args[1])) blk: {
        // Simple mode: csvq file.csv [columns] [where] [limit] [orderby]
        const simple_args = args[1..];
        break :blk simple_parser.parseSimple(allocator, simple_args) catch |err| {
            std.debug.print("error: {}\n", .{err});
            std.debug.print("\nRun 'csvq --help' for usage information.\n", .{});
            std.process.exit(1);
        };
    } else blk: {
        // SQL mode: csvq "SELECT ..."
        const query_text = try getQueryFromArgsOrStdin(allocator, args);
        defer allocator.free(query_text);

        break :blk parser.parse(allocator, query_text) catch |err| {
            std.debug.print("SQL parse error: {}\n", .{err});
            std.debug.print("\nRun 'csvq --help' for usage information.\n", .{});
            std.process.exit(1);
        };
    };
    defer query.deinit();

    // Execute query
    engine.execute(allocator, query, stdout_file) catch |err| {
        std.debug.print("execution error: {}\n", .{err});
        std.process.exit(1);
    };
}

/// Detect if argument is SQL query (starts with SELECT)
fn isSQL(arg: []const u8) bool {
    const trimmed = std.mem.trim(u8, arg, &std.ascii.whitespace);
    if (trimmed.len < 6) return false;

    // Check if starts with SELECT (case-insensitive)
    var upper_buf: [6]u8 = undefined;
    const upper = std.ascii.upperString(&upper_buf, trimmed[0..6]);
    return std.mem.eql(u8, upper, "SELECT");
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
        try stderr.writeAll("error: no query provided\n\nRun 'csvq --help' for usage information.\n");
        std.process.exit(1);
    }

    // Return a copy of the trimmed string
    const result = try allocator.dupe(u8, trimmed);
    allocator.free(query);
    return result;
}
