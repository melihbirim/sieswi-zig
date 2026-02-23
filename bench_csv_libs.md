# Zig CSV Libraries Landscape

## Known Libraries:

1. **zig-csv** (github.com/beachglasslabs/zig-csv)
   - Pure Zig RFC 4180 compliant
   - Iterator-based API
   - Used in production

2. **csv-parser** (github.com/n0s4/csv-parser)
   - Simple CSV parser
   - Minimal allocations

3. **std.csv** proposals (not in stdlib yet)

## Our Advantages

- Memory-mapped I/O for large files
- SIMD-accelerated field parsing (@Vector)
- Zero-copy with mmap pointers
- Lock-free parallel execution (7 cores)
- 256KB buffered reader for small files

## Benchmark Plan

1. Test against zig-csv on 1M row file
2. Compare pure parsing speed (no WHERE/SELECT)
3. Test memory usage
4. Test scalability (1K, 10K, 100K, 1M, 10M rows)
