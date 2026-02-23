const std = @import("std");
const csv = @import("csv");

/// Example: Using sieswi's CSV reader as a library
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open a CSV file
    const file = try std.fs.cwd().openFile("data.csv", .{});
    defer file.close();

    // Create a CSV reader
    var reader = csv.CsvReader.init(allocator, file);

    // Read records one by one
    var row_count: usize = 0;
    while (try reader.readRecord()) |record| {
        defer reader.freeRecord(record);
        
        row_count += 1;
        
        // Print the record
        std.debug.print("Row {d}: ", .{row_count});
        for (record, 0..) |field, i| {
            if (i > 0) std.debug.print(", ", .{});
            std.debug.print("{s}", .{field});
        }
        std.debug.print("\n", .{});
        
        // Stop after 10 rows for demo
        if (row_count >= 10) break;
    }

    std.debug.print("\nTotal rows read: {d}\n", .{row_count});
}
