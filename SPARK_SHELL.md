# SparkShell - Automatic Server Management

## Overview

**SparkShell** is a Python class that automatically downloads, builds, starts, and manages SparkShell servers. Use it to execute Spark SQL from any machine without manual setup.

## Quick Start

```bash
# Install dependency
pip install requests

# Use it!
python -c "
from spark_shell import SparkShell
with SparkShell(source='.') as shell:
    print(shell.execute_sql('SELECT 1 as id'))
"
```

## Basic Usage

```python
from spark_shell import SparkShell

# Context manager handles everything automatically
with SparkShell(source="/path/to/sparkshell", port=8080) as shell:
    result = shell.execute_sql("SELECT * FROM table")
    print(result)  # Just the output!
# Server automatically shut down and cleaned up
```

## Features

✅ **Automatic download** from GitHub or local directory  
✅ **Automatic build** (SBT assembly)  
✅ **Automatic startup** with health checks  
✅ **Simple API** - just `execute_sql()` and get results  
✅ **Delta Lake support** - Create and query Delta tables  
✅ **Unity Catalog support** - Access Unity Catalog tables  
✅ **Automatic cleanup** via context manager  
✅ **Error handling** - raises `RuntimeError` with clear messages  

## API Reference

### Constructor

```python
SparkShell(
    source: str,                    # GitHub URL or local path (required)
    port: int = 8080,               # Server port
    temp_dir: Optional[str] = None, # Custom temp directory
    auto_build: bool = True,        # Auto build JAR
    auto_start: bool = True,        # Auto start server
    cleanup_on_exit: bool = True,   # Clean temp files on exit
    startup_timeout: int = 60,      # Startup timeout (seconds)
    build_timeout: int = 300,       # Build timeout (seconds)
    spark_configs: Optional[dict] = None,  # Spark configuration options
    uc_uri: Optional[str] = None,   # Unity Catalog server URI
    uc_token: Optional[str] = None, # Unity Catalog token
    uc_catalog: Optional[str] = None, # Unity Catalog catalog name (defaults to "unity")
    uc_schema: Optional[str] = None # Unity Catalog schema name
)
```

### Methods

**`execute_sql(sql: str) -> str`**
- Execute SQL command and return result string
- Raises `RuntimeError` if execution fails

**`get_server_info() -> dict`**
- Returns server info: `{"sparkVersion": "4.0.0", "port": 8080, ...}`

**`setup()`** - Download/copy code (called automatically)  
**`build()`** - Build assembly JAR (called automatically)  
**`start()`** - Start server (called automatically)  
**`shutdown()`** - Stop server (called automatically on exit)  
**`cleanup()`** - Remove temp files (called automatically on exit)

## Examples

### Example 1: Create Table and Query

```python
with SparkShell(source=".", port=8080) as shell:
    # Create table
    shell.execute_sql("CREATE TABLE users (id INT, name STRING) USING parquet")
    
    # Insert data
    shell.execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    
    # Query
    result = shell.execute_sql("SELECT * FROM users ORDER BY id")
    print(result)
    
    # Cleanup
    shell.execute_sql("DROP TABLE users")
```

### Example 2: From GitHub

```python
github_url = "https://github.com/user/repo/tree/main/experimental/sparkshell"
with SparkShell(source=github_url, port=8080) as shell:
    result = shell.execute_sql("SELECT current_date() as today")
    print(result)
```

### Example 3: Error Handling

```python
with SparkShell(source=".") as shell:
    try:
        result = shell.execute_sql("SELECT * FROM nonexistent")
    except RuntimeError as e:
        print(f"Error: {e}")
```

### Example 4: Manual Control

```python
shell = SparkShell(source=".", auto_start=False, cleanup_on_exit=False)
try:
    shell.setup()    # Download/copy
    shell.build()    # Build JAR (2-5 min first time)
    shell.start()    # Start server
    result = shell.execute_sql("SELECT 1")
finally:
    shell.shutdown()
    shell.cleanup()
```

### Example 5: Multiple Queries

```python
with SparkShell(source=".") as shell:
    queries = [
        "CREATE TABLE test (id INT) USING parquet",
        "INSERT INTO test VALUES (1), (2), (3)",
        "SELECT COUNT(*) FROM test",
        "DROP TABLE test"
    ]
    for sql in queries:
        result = shell.execute_sql(sql)
        print(f"{sql[:30]}... => {result[:50]}")
```

### Example 6: Custom Spark Configurations

```python
# Pass custom Spark configurations
spark_configs = {
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "10",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.warehouse.dir": "/custom/warehouse"
}

with SparkShell(source=".", port=8080, spark_configs=spark_configs) as shell:
    result = shell.execute_sql("SELECT 1")
    print(result)
```

### Example 7: Unity Catalog Configuration

```python
# Configure Unity Catalog with dedicated parameters
with SparkShell(
    source=".", 
    port=8080,
    uc_uri="http://localhost:8081",
    uc_token="your-uc-token",
    uc_catalog="unity",       # Optional, defaults to "unity"
    uc_schema="my_schema"     # Optional
) as shell:
    # Tables can be referenced with three-level namespace
    result = shell.execute_sql("SELECT * FROM unity.my_schema.my_table")
    print(result)
    
    # Or use short name (since catalog and schema are set as default)
    result = shell.execute_sql("SELECT * FROM my_table")
    print(result)
    
    # Create table in Unity Catalog
    shell.execute_sql("CREATE TABLE my_table (id INT, name STRING) USING DELTA")

# Minimal UC config (catalog defaults to "unity")
with SparkShell(source=".", uc_uri="http://localhost:8081", uc_token="token") as shell:
    result = shell.execute_sql("SHOW CATALOGS")
    print(result)
```

## Source Specifications

### Local Paths
```python
SparkShell(source=".")                       # Current directory
SparkShell(source="/absolute/path")          # Absolute path
SparkShell(source="~/projects/sparkshell")   # Home expansion
```

### GitHub URLs
```python
# Full repo
SparkShell(source="https://github.com/user/repo")

# Specific directory (sparse checkout)
SparkShell(source="https://github.com/user/repo/tree/main/path/to/sparkshell")
```

## What Happens Automatically

When you use `with SparkShell(...)`:

1. ✅ Creates temp directory (`/tmp/sparkshell_XXXXX/`)
2. ✅ Downloads or copies SparkApp code
3. ✅ Runs `build/sbt assembly` (builds JAR, 2-5 min first time)
4. ✅ Starts Java server in background
5. ✅ Waits for health check to succeed
6. ✅ Returns ready instance

**Your code runs** - use `execute_sql()`

7. ✅ Shuts down server gracefully
8. ✅ Cleans up temp files

## Requirements

- **Python**: 3.7+
- **Java**: 11+ (for running Spark)
- **Git**: For GitHub downloads
- **Disk**: ~200MB for dependencies + JAR
- **Memory**: ~1GB for Spark (local mode)
- **Python Package**: `requests`

## Performance

| Operation | First Run | Subsequent |
|-----------|-----------|------------|
| Setup | 1-10 sec | 1-10 sec |
| Build | 2-5 min | 30-60 sec |
| Startup | 10-30 sec | 10-30 sec |
| **Total** | **3-6 min** | **1-2 min** |

## Troubleshooting

### Port Already in Use
```python
SparkShell(source=".", port=9090)  # Use different port
```

### Build Timeout
```python
SparkShell(source=".", build_timeout=600)  # 10 minutes
```

### Server Startup Timeout
```python
SparkShell(source=".", startup_timeout=120)  # 2 minutes
```

### Keep Files for Debugging
```python
shell = SparkShell(source=".", cleanup_on_exit=False)
# ... use shell ...
print(f"Temp dir: {shell.work_dir}")
print(f"Logs: {shell.work_dir}/sparkshell.log")
```

## Command-Line Usage

```bash
# Run with examples
python spark_shell.py /path/to/sparkshell

# Execute specific SQL
python spark_shell.py . --sql "SELECT current_timestamp()"

# Custom port
python spark_shell.py . --port 9090

# Keep temp files
python spark_shell.py . --no-cleanup
```

## Testing

```bash
# Run integration tests
python tests/test_spark_shell.py

# Or with pytest
python -m pytest tests/test_spark_shell.py -v

# Run example
python example.py

# Test Unity Catalog connectivity
python tests/test_unity_catalog.py --uri http://localhost:8081 --token your-token
```

## Best Practices

✅ **DO**: Use context manager for automatic cleanup  
✅ **DO**: Reuse instance for multiple queries  
✅ **DO**: Handle RuntimeError for SQL errors  
✅ **DO**: Use appropriate timeouts for your environment  

❌ **DON'T**: Create new instance for each query  
❌ **DON'T**: Ignore errors  
❌ **DON'T**: Expose server to public internet (no auth)  

## Files

- **`spark_shell.py`** - Main implementation (import this)
- **`example.py`** - Usage example
- **`tests/test_spark_shell.py`** - Integration tests

---

**Quick Reference:**

```python
# Import
from spark_shell import SparkShell

# Use
with SparkShell(source="/path", port=8080) as shell:
    result = shell.execute_sql("SELECT 1")
    print(result)
```

That's it! Everything else is automatic.

