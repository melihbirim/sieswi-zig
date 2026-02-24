const std = @import("std");
const csv = @import("csv");

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
    const fields1 = &[_][]const u8{ "id", "name", "value" };
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

// TDD Test 8: CsvWriter escapes quotes correctly
test "CsvWriter escapes quotes" {
    const allocator = std.testing.allocator;

    const tmp_file = try std.fs.cwd().createFile("test_quotes.csv", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_quotes.csv") catch {};
    }

    var writer = csv.CsvWriter.init(tmp_file);

    // Write fields with special characters
    const fields = &[_][]const u8{ "Hello", "World, \"foo\"" };
    try writer.writeRecord(fields);
    try writer.flush();

    // Read back and verify proper quoting
    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 1024);
    defer allocator.free(content);

    // Should escape quotes as "" and wrap field in quotes
    const expected = "Hello,\"World, \"\"foo\"\"\"\n";
    try std.testing.expectEqualStrings(expected, content);
}
