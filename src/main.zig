const std = @import("std");
const parser = @import("parser.zig");
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
