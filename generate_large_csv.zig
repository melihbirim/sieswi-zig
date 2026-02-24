const std = @import("std");

pub fn main() !void {
    const stdout = std.fs.File{ .handle = std.posix.STDOUT_FILENO };

    // Write header
    try stdout.writeAll("id,name,age,city,salary,department\n");

    const cities = [_][]const u8{ "NYC", "SF", "LA", "Chicago", "Boston", "Seattle", "Austin", "Denver" };
    const departments = [_][]const u8{ "Engineering", "Sales", "Marketing", "HR", "Finance", "Operations" };
    const names = [_][]const u8{ "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry" };

    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();

    // Use stack buffer to avoid allocations
    var line_buffer: [256]u8 = undefined;

    var i: u32 = 1;
    while (i <= 1_000_000) : (i += 1) {
        const name = names[random.intRangeAtMost(usize, 0, names.len - 1)];
        const age = random.intRangeAtMost(u32, 22, 65);
        const city = cities[random.intRangeAtMost(usize, 0, cities.len - 1)];
        const salary = random.intRangeAtMost(u32, 40000, 150000);
        const dept = departments[random.intRangeAtMost(usize, 0, departments.len - 1)];

        // Format into stack buffer (no allocation!)
        const line = try std.fmt.bufPrint(&line_buffer, "{d},{s},{d},{s},{d},{s}\n", .{ i, name, age, city, salary, dept });

        try stdout.writeAll(line);

        if (@rem(i, 50000) == 0) {
            std.debug.print("Generated {d} rows...\r", .{i});
        }
    }

    std.debug.print("\nDone! Generated 1,000,000 rows\n", .{});
}
