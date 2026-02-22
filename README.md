# sieswi-zig

**Blazing-fast SQL queries on CSV files** - Zig implementation

A Zig rewrite of [sieswi](https://github.com/melihbirim/sieswi), a high-performance CSV query engine that executes SQL-like queries on CSV files.

```bash
sieswi "SELECT name, age FROM 'data.csv' WHERE age > 25 LIMIT 10"
```

## Why Zig?

This is a Zig reimplementation of the original Go version, bringing:

- **Native performance** - Zig compiles to highly optimized machine code
- **Zero dependencies** - Pure Zig standard library implementation
- **Memory safety** - Compile-time memory safety without garbage collection
- **Low-level control** - Fine-grained control over allocations and performance

## Features

**Currently Implemented:**

- SELECT with column projection or SELECT \*
- WHERE clause with comparisons (=, !=, >, >=, <, <=)
- LIMIT for result capping
- RFC 4180 compliant CSV parsing
- Streaming execution for low memory usage
- Case-insensitive column names
- stdin support for Unix pipes

**In Progress:**

- GROUP BY with aggregations (COUNT, SUM, AVG, MIN, MAX)
- Parallel processing for large files
- Complex WHERE expressions (AND, OR, NOT)

**Planned:**

- Multi-threaded parallel execution
- JOIN operations
- ORDER BY
- Advanced operators (IN, LIKE, BETWEEN, IS NULL)

## Installation

### Prerequisites

- [Zig](https://ziglang.org/) 0.13.0, 0.14.0, or 0.15+ (Currently tested with Zig 0.15.2)

### Build from Source

```bash
git clone https://github.com/melihbirim/sieswi-zig
cd sieswi-zig
zig build -Doptimize=ReleaseFast
```

The binary will be in `zig-out/bin/sieswi`.

### Install

```bash
# Copy to your PATH
sudo cp zig-out/bin/sieswi /usr/local/bin/
```

## Quick Start

### Basic Queries

```bash
# Select all columns
sieswi "SELECT * FROM 'data.csv' LIMIT 10"

# Select specific columns
sieswi "SELECT name, age FROM 'users.csv'"

# Filter with WHERE clause
sieswi "SELECT * FROM 'sales.csv' WHERE amount > 100"

# Combine filtering and projection
sieswi "SELECT name, email FROM 'users.csv' WHERE age >= 18 LIMIT 100"
```

### Unix Pipes

```bash
# Read from stdin
cat data.csv | sieswi "SELECT name, age FROM '-' WHERE age > 25"

# Chain filters
cat orders.csv | sieswi "SELECT * FROM '-' WHERE country = 'US'" | sieswi "SELECT total FROM '-' WHERE total > 1000"

# Monitor live logs
tail -f logs.csv | sieswi "SELECT timestamp, message FROM '-' WHERE level = 'ERROR'"
```

### Output Redirection

```bash
# Write to file
sieswi "SELECT * FROM 'data.csv' WHERE status = 'active'" > active_users.csv

# Pipe to other tools
sieswi "SELECT email FROM 'users.csv'" | wc -l
```

## SQL Support

### Supported Features

- **SELECT**: Column projection (`SELECT col1, col2`) or all columns (`SELECT *`)
- **FROM**: File path (quoted or unquoted), or `-` for stdin
- **WHERE**: Comparison operators: `=`, `!=`, `>`, `>=`, `<`, `<=`
- **LIMIT**: Limit number of results
- **Numeric coercion**: Automatic string-to-number conversion in comparisons

### Not Yet Supported

- Boolean expressions (AND, OR, NOT) in WHERE
- GROUP BY with aggregations
- ORDER BY
- JOIN operations
- Advanced operators (IN, LIKE, BETWEEN, IS NULL)
- HAVING clause

## Development

### Build and Test

```bash
# Debug build
zig build

# Release build (optimized)
zig build -Doptimize=ReleaseFast

# Run tests
zig build test

# Run with arguments
zig build run -- "SELECT * FROM 'test.csv' LIMIT 5"
```

### Project Structure

```bash
src/
  main.zig          # CLI entry point
  parser.zig        # SQL query parser
  engine.zig        # Query execution engine
  csv.zig           # RFC 4180 CSV reader/writer
  aggregation.zig   # GROUP BY aggregations (in progress)
build.zig           # Build configuration
```

## Architecture

```bash
Input CSV → Parse Header → Build Column Map
                          ↓
                     Parse SQL Query
                          ↓
                  ┌───────┴────────┐
                  │                │
         Sequential Stream    Parallel Batching
          (small files)      (large files >10MB)
                  │                │
                  └───────┬────────┘
                          ↓
                   Filter (WHERE)
                          ↓
                  Project (SELECT)
                          ↓
                    Output CSV
```

### Performance Strategy

1. **Streaming First**: Small queries stream row-by-row for instant results
2. **Parallel for Scale**: Large files (>10MB) use multi-threaded batching (planned)
3. **Memory Efficient**: Fixed buffer sizes, minimal allocations
4. **RFC 4180 Compliant**: Proper CSV parsing with quoted field support

## Benchmarks

Coming soon - will compare against:

- Original sieswi (Go)
- DuckDB
- Other CSV query tools

## Contributing

This is a learning project and Zig port. Contributions welcome:

- Bug reports and fixes
- Performance improvements
- Feature implementations
- Documentation improvements

## License

MIT

## Acknowledgments

- Original [sieswi](https://github.com/melihbirim/sieswi) by [@melihbirim](https://github.com/melihbirim)
- Inspired by DuckDB's approach to CSV analytics

---

**Built with Zig** 🦎
