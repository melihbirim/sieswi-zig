# Simple Query Language (SQL)

Alternative to SQL syntax for common CSV operations. Simpler, more Unix-tool-like.

## Syntax

```bash
csvq <file> [columns] [where] [limit] [orderby]
```

## Arguments

### 1. File (required)

```bash
csvq data.csv              # Path to CSV file
csvq /path/to/data.csv     # Absolute paths supported
```

### 2. Columns (optional, default: `*`)

```bash
"*"                          # All columns (default)
"id,name,score"              # Specific columns (comma-separated, spaces OK)
""                           # Empty = all columns
```

### 3. WHERE Clause (optional, default: no filter)

**Single condition:**

```bash
"age>30"                     # Greater than
"score>=80"                  # Greater or equal
"age<40"                     # Less than
"count<=100"                 # Less or equal
"name=Alice"                 # String equality
"status!=active"             # Not equal
```

**Multiple conditions (AND/OR):**

```bash
"age>30 AND score>=80"       # Both conditions must match
"city=NYC OR city=SF"        # Either condition matches
"age>25 AND age<65 AND city=NYC"  # Multiple AND conditions
```

**Supported operators:**

- `=` - Equals
- `!=` - Not equals
- `>` - Greater than
- `<` - Less than
- `>=` - Greater or equal
- `<=` - Less or equal

**Case sensitivity:**

- Column names: case-insensitive (`age`, `Age`, `AGE` all match)
- String values: case-sensitive (`Alice` != `alice`)

### 4. Limit (optional, default: 10)

```bash
0                            # No limit (all rows)
10                           # First 10 rows (default if omitted)
100                          # First 100 rows
```

### 5. Order By (optional, default: no sorting)

```bash
"age"                        # Sort by age ascending (default)
"age:asc"                    # Sort by age ascending (explicit)
"age:desc"                   # Sort by age descending
"score:desc"                 # Sort by score descending
```

## Examples

### Basic Usage

```bash
# Show first 10 rows (default limit)
csvq data.csv

# Show all rows
csvq data.csv "*" "" 0

# Select specific columns
csvq data.csv "id,name,age"

# Quick peek at data (first 5 rows)
csvq data.csv "*" "" 5
```

### Filtering

```bash
# Simple filter
csvq data.csv "*" "age>30"

# Filter with specific columns
csvq data.csv "name,score" "score>=80"

# Multiple conditions (AND)
csvq data.csv "*" "age>25 AND score>=80"

# Multiple conditions (OR)
csvq data.csv "*" "city=NYC OR city=SF"

# Complex filter
csvq data.csv "name,age,city" "age>30 AND (city=NYC OR city=SF)" 20
```

### Sorting

```bash
# Sort by age ascending
csvq data.csv "*" "" 10 "age:asc"

# Sort by score descending, show top 10
csvq data.csv "name,score" "score>0" 10 "score:desc"

# Sort alphabetically by name
csvq data.csv "name,age" "" 0 "name:asc"
```

### Real-World Examples

```bash
# Top 10 highest scores
csvq scores.csv "name,score" "" 10 "score:desc"

# Young employees in NYC
csvq employees.csv "name,age,city" "age<30 AND city=NYC"

# All high performers, sorted by name
csvq performance.csv "*" "rating>=4.5" 0 "name:asc"

# Quick data inspection
csvq large_file.csv "*" "" 5

# Filter and limit
csvq sales.csv "product,revenue" "revenue>1000" 20
```

## Comparison with SQL

```bash
# SQL syntax (verbose)
csvq "SELECT name, score FROM 'data.csv' WHERE score >= 80 ORDER BY score DESC LIMIT 10"

# Simple syntax (concise)
csvq data.csv "name,score" "score>=80" 10 "score:desc"
```

## Implementation Notes

### Positional Arguments

All arguments are positional. To skip an argument, use empty string or defaults:

```bash
csvq data.csv                        # All defaults
csvq data.csv "id,name"              # Columns only
csvq data.csv "*" "age>30"           # Filter only
csvq data.csv "*" "" 5               # Limit only
csvq data.csv "*" "" 0 "age:desc"    # Sort only
```

### Auto-detection

When first argument doesn't start with "SELECT", use simple mode:

```bash
csvq data.csv "name,age"             # Simple mode
csvq "SELECT name FROM 'data.csv'"   # SQL mode
```

### Compatibility

Both syntaxes work simultaneously:

- Simple mode: Fast, common operations
- SQL mode: Complex queries (JOINs, GROUP BY, aggregates)

## Future Extensions

Possible additions (not in v1):

- Multiple sorts: `"age:desc,name:asc"`
- LIKE operator: `"name~Alice%"`
- IN operator: `"city IN (NYC,SF,LA)"`
- Distinct: `--distinct` flag
- Aggregates: Special columns like `COUNT(*)`, `SUM(score)`
