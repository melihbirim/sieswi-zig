const std = @import("std");
const parser = @import("parser.zig");
const simple_parser = @import("simple_parser.zig");
const engine = @import("engine.zig");
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

    // Detect query mode: simple vs SQL
    var query = if (args.len > 1 and !isSQL(args[1])) blk: {
        // Simple mode: sieswi file.csv [columns] [where] [limit] [orderby]
        const simple_args = args[1..];
        break :blk simple_parser.parseSimple(allocator, simple_args) catch |err| {
            std.debug.print("simple parse error: {}\n", .{err});
            std.debug.print("usage: sieswi <file> [columns] [where] [limit] [orderby]\n", .{});
            std.debug.print("   or: sieswi \"SELECT ... FROM ...\"\n", .{});
            std.process.exit(1);
        };
    } else blk: {
        // SQL mode: sieswi "SELECT ..."
        const query_text = try getQueryFromArgsOrStdin(allocator, args);
        defer allocator.free(query_text);

        break :blk parser.parse(allocator, query_text) catch |err| {
            std.debug.print("SQL parse error: {}\n", .{err});
            std.process.exit(1);
        };
    };
    defer query.deinit();

    // Execute query
    const stdout = std.fs.File{ .handle = std.posix.STDOUT_FILENO };
    engine.execute(allocator, query, stdout) catch |err| {
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
        try stderr.writeAll("usage: sieswi \"SELECT ...\" (or pipe query via stdin)\n");
        std.process.exit(1);
    }

    // Return a copy of the trimmed string
    const result = try allocator.dupe(u8, trimmed);
    allocator.free(query);
    return result;
}
