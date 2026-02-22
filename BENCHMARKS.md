# Performance Benchmarks: sieswi-zig vs DuckDB

## Test Environment

- **Hardware**: macOS
- **Dataset**: 1,000,000 rows, 35MB CSV file
- **sieswi-zig**: Zig 0.15.2, ReleaseFast build
- **DuckDB**: Latest version via Homebrew

## Results Summary

### Test 1: WHERE Clause (100K rows, 3.4MB)

Query: `SELECT name, city, salary FROM medium_test.csv WHERE age > 50`

| Engine         | User Time | Total Time | CPU Usage | Mode            |
| -------------- | --------- | ---------- | --------- | --------------- |
| **sieswi-zig** | 0.51s     | 2.47s      | 99%       | Sequential      |
| **DuckDB**     | 0.10s     | 0.61s      | 19%       | Single-threaded |

**Winner**: DuckDB **5.1x faster** ⚡

---

### Test 2: WHERE with LIMIT (1M rows, 35MB)

Query: `SELECT * FROM large_test.csv WHERE age > 50 LIMIT 1000`

| Engine         | User Time | Total Time | CPU Usage | Mode              |
| -------------- | --------- | ---------- | --------- | ----------------- |
| **sieswi-zig** | 0.01s     | 0.05s      | 97%       | Early termination |
| **DuckDB**     | 0.06s     | 0.08s      | 104%      | Parallel          |

**Winner**: sieswi-zig **6x faster** ⚡ (streaming advantage)

---

### Test 3: Full Scan with WHERE (1M rows, 35MB)

Query: `SELECT name, city, salary FROM large_test.csv WHERE age > 50`

| Engine         | User Time | Total Time | CPU Usage | Cores             |
| -------------- | --------- | ---------- | --------- | ----------------- |
| **sieswi-zig** | 5.30s     | 25.38s     | 99%       | Sequential        |
| **DuckDB**     | 0.25s     | 0.12s      | 234%      | ~3 cores parallel |

**Winner**: DuckDB **21x faster** ⚡ (parallel processing)

---

### Memory Usage (1M rows with LIMIT 100)

Query: `SELECT name, city FROM large_test.csv WHERE age > 50 LIMIT 100`

| Engine         | Max Resident | Peak Footprint |
| -------------- | ------------ | -------------- |
| **sieswi-zig** | 1.8 MB       | 1.4 MB         |
| **DuckDB**     | 63.5 MB      | 51.1 MB        |

**Winner**: sieswi-zig uses **35x less memory** 🎯

---

## Key Insights

### sieswi-zig Advantages ✓

- **Extremely memory efficient**: 1.8MB vs 63.5MB (35x less)
- **Faster for LIMIT queries**: Streaming + early termination optimization
- **Minimal overhead**: Single binary, no runtime dependencies
- **Ideal for**: Resource-constrained environments, embedded systems, streaming data

### DuckDB Advantages ✓

- **Much faster for full scans**: Parallel processing across multiple cores
- **Mature optimizations**: Columnar storage, vectorization, query planning
- **Rich feature set**: Complex SQL, transactions, window functions
- **Ideal for**: Interactive analytics, complex queries, ad-hoc exploration

### Opportunities for sieswi-zig

- **Implement parallel processing**: Could reduce full scan time by 3-4x
- **Add vectorization**: SIMD operations for numeric comparisons
- **Optimize CSV parsing**: Custom SIMD-accelerated CSV reader

---

## Conclusion

sieswi-zig demonstrates excellent memory efficiency and streaming performance, making it ideal for:

- **Embedded systems** with limited memory
- **Log filtering** with LIMIT clauses
- **Quick data sampling** from large files
- **Unix pipelines** where streaming is beneficial

DuckDB excels at:

- **Complex analytical queries** requiring full table scans
- **Interactive data exploration**
- **Multi-threaded workloads** on powerful machines

Both tools complement each other - choose based on your use case!
