# SparkShell - Spark SQL REST Server

A simple REST API server that executes Spark SQL commands and returns results as JSON.

## Features

- REST API with JSON request/response format
- Execute SQL commands via simple HTTP POST requests
- Returns formatted query results or command execution status
- Supports both queries (SELECT) and commands (CREATE, INSERT, etc.)
- **Delta Lake support** - Create and query Delta tables with ACID transactions
- **Unity Catalog support** - Access Unity Catalog tables with three-level namespace
- **Cloud Storage support** - S3, Azure Blob Storage, and Google Cloud Storage
- Easy to use with curl, Postman, or any HTTP client

## Building the Application

This project is completely self-contained with its own SBT installation.

```bash
cd experimental/sparkshell
build/sbt compile
```

## Running the Server

### Interactive Mode

Run the server in the foreground (you'll see all logs):

```bash
cd experimental/sparkshell
build/sbt run
```

Default port is 8080 if not specified.

To specify a custom port:
```bash
build/sbt "run 3000"
```

### Background Mode (Daemon)

The project includes convenient scripts to manage the server as a background process:

**Start the server:**
```bash
bin/start.sh [port]
```
Example:
```bash
bin/start.sh          # Start on default port 8080
bin/start.sh 3000     # Start on custom port 3000
```

**Stop the server:**
```bash
bin/stop.sh
```

**Restart the server:**
```bash
bin/restart.sh [port]
```

**Check server status:**
```bash
bin/status.sh
```

**View logs:**
```bash
tail -f sparkshell.log
```

The background scripts will:
- Save the process PID to `sparkshell.pid`
- Write all logs to `sparkshell.log`
- Handle graceful shutdown with fallback to force kill if needed
- Detect and clean up stale PID files

## API Endpoints

### Health Check
```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "ok",
  "message": "SparkShell server is running"
}
```

### Server Info
```bash
curl http://localhost:8080/info
```

Response:
```json
{
  "sparkVersion": "3.5.0",
  "port": "8080",
  "endpoints": {
    "health": "GET /health",
    "execute": "POST /sql",
    "info": "GET /info"
  }
}
```

### Execute SQL
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 as id, '\''Alice'\'' as name"}'
```

Response:
```json
{
  "success": true,
  "result": "id | name\n-----------\n1 | Alice\n\nTotal rows: 1",
  "error": null
}
```

## Usage Examples with curl

### Create a Table
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE users (id INT, name STRING, age INT)"}'
```

### Insert Data
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO users VALUES (1, '\''Alice'\'', 30), (2, '\''Bob'\'', 25)"}'
```

### Query Data
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

### Query with WHERE Clause
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT name, age FROM users WHERE age > 25"}'
```

### Error Response Example
If there's an error, you'll get:
```json
{
  "success": false,
  "result": null,
  "error": "Table or view not found: nonexistent_table"
}
```

### Delta Lake Examples

#### Create a Delta Table
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE delta_users (id INT, name STRING, age INT) USING DELTA"}'
```

#### Insert into Delta Table
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO delta_users VALUES (1, '\''Alice'\'', 30), (2, '\''Bob'\'', 25)"}'
```

#### Query Delta Table
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM delta_users"}'
```

#### Update Delta Table
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "UPDATE delta_users SET age = 31 WHERE name = '\''Alice'\''"}'
```

#### Delete from Delta Table
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "DELETE FROM delta_users WHERE age < 30"}'
```

#### Time Travel (Query Previous Version)
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM delta_users VERSION AS OF 0"}'
```

## Python Client

A Python client module is included for easy integration with Python applications.

### Installation

Install the required dependencies:
```bash
pip install -r requirements.txt
# or
pip install requests
```

### Usage

#### Using SparkShell (Recommended - Automatic Server Management)

```python
from spark_shell import SparkShell

# Automatically starts server, executes SQL, and cleans up
with SparkShell(source=".", port=8080) as shell:
    result = shell.execute_sql("SELECT * FROM users")
    print(result)  # Just the output!
```

#### Using REST API Directly (Advanced)

If you already have a running server, you can make direct HTTP requests:

# Check server health
health = client.health_check()
print(health)  # {"status": "ok", "message": "SparkShell server is running"}

# Get server info
info = client.server_info()
print(f"Spark Version: {info['sparkVersion']}")

# Execute SQL
success, result, error = client.execute_sql("SELECT 1 as id, 'Alice' as name")
if success:
    print(result)
else:
    print(f"Error: {error}")
```

#### Using Python requests

```python
import requests

# Execute SQL
response = requests.post(
    "http://localhost:8080/sql",
    json={"sql": "SELECT * FROM users"}
)
data = response.json()
if data["success"]:
    print(data["result"])
```

#### Passing Custom Spark Configurations

```python
from spark_shell import SparkShell

# Configure Spark settings
spark_configs = {
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "10",
    "spark.sql.adaptive.enabled": "true"
}

with SparkShell(source=".", port=8080, spark_configs=spark_configs) as shell:
    result = shell.execute_sql("SELECT * FROM users")
    print(result)
```

#### Configuring Unity Catalog

Unity Catalog can be configured using dedicated parameters:

```python
from spark_shell import SparkShell

# Full configuration
with SparkShell(
    source=".", 
    port=8080,
    uc_uri="http://localhost:8081",
    uc_token="your-uc-token",
    uc_catalog="unity",       # Optional, defaults to "unity"
    uc_schema="my_schema"     # Optional
) as shell:
    # Query with three-level namespace
    result = shell.execute_sql("SELECT * FROM unity.my_schema.my_table")
    print(result)
    
    # Or use short name (catalog.schema already set as default)
    result = shell.execute_sql("SELECT * FROM my_table")
    print(result)

# Minimal configuration (uc_catalog defaults to "unity")
with SparkShell(source=".", uc_uri="http://localhost:8081", uc_token="token") as shell:
    result = shell.execute_sql("SHOW CATALOGS")
    print(result)
```

#### Configuring Cloud Storage

SparkShell includes built-in support for cloud storage (S3, Azure, GCS). Configure access via Spark configurations:

**AWS S3 Example:**
```python
from spark_shell import SparkShell

spark_configs = {
    "spark.hadoop.fs.s3a.access.key": "your-access-key",
    "spark.hadoop.fs.s3a.secret.key": "your-secret-key",
    # Optional: for specific endpoint
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com"
}

with SparkShell(source=".", spark_configs=spark_configs) as shell:
    # Create table on S3
    shell.execute_sql("""
        CREATE TABLE my_table (id INT, name STRING) 
        USING DELTA 
        LOCATION 's3a://my-bucket/path/to/table'
    """)
    
    # Insert and query data
    shell.execute_sql("INSERT INTO my_table VALUES (1, 'Alice'), (2, 'Bob')")
    result = shell.execute_sql("SELECT * FROM my_table")
    print(result)
```

**Azure Blob Storage Example:**
```python
spark_configs = {
    "spark.hadoop.fs.azure.account.key.mystorageaccount.dfs.core.windows.net": "your-storage-key"
}

with SparkShell(source=".", spark_configs=spark_configs) as shell:
    shell.execute_sql("""
        CREATE TABLE my_table (id INT, name STRING) 
        USING DELTA 
        LOCATION 'abfss://container@mystorageaccount.dfs.core.windows.net/path/to/table'
    """)
```

**Google Cloud Storage Example:**
```python
spark_configs = {
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/path/to/keyfile.json",
    "spark.hadoop.fs.gs.project.id": "your-project-id"
}

with SparkShell(source=".", spark_configs=spark_configs) as shell:
    shell.execute_sql("""
        CREATE TABLE my_table (id INT, name STRING) 
        USING DELTA 
        LOCATION 'gs://my-bucket/path/to/table'
    """)
```

#### Running the Example Scripts

The project includes comprehensive examples:

```bash
# Run example with automatic server management
python example.py
```

These will demonstrate:
- Automatic server management
- Creating tables
- Inserting data
- Querying data
- Aggregations
- Error handling
- Context manager usage

### SparkShell API Reference

**SparkShell(source, port=8080, spark_configs=None, uc_uri=None, uc_token=None, uc_catalog=None, uc_schema=None, ...)**
- `source`: Path to SparkShell code (local or GitHub URL)
- `port`: Server port (default: 8080)
- `spark_configs`: Dict of Spark configuration options (optional)
  - Example: `{"spark.executor.memory": "2g", "spark.sql.shuffle.partitions": "10"}`
- `uc_uri`: Unity Catalog server URI (optional)
- `uc_token`: Unity Catalog authentication token (optional)
- `uc_catalog`: Unity Catalog catalog name (optional, defaults to "unity" if UC is configured)
- `uc_schema`: Unity Catalog schema name (optional)
- `execute_sql(sql: str) -> str`: Execute SQL and return result string (raises RuntimeError on failure)
- `get_server_info() -> dict`: Get server information including Spark version
- `server_info() -> dict`: Get server information including Spark version
- `is_healthy() -> bool`: Check if server is healthy (returns True/False)

**execute_sql(sql, host="localhost", port=8080)**
- Convenience function for quick SQL execution without creating a client instance

## Testing

The project includes comprehensive test suites for both Scala and Python code.

### Running All Tests

Use the unified test runner:

```bash
./run-tests
```

This script will:
- Run all Scala (SBT) tests
- Run all Python integration tests
- Exit with error code if any tests fail
- Show a summary of results

### Running Individual Test Suites

**Scala Tests (SBT):**
```bash
# Run all Scala tests
build/sbt test

# Run specific test suite
build/sbt "testOnly com.sparkshell.SparkSqlExecutorSpec"
build/sbt "testOnly com.sparkshell.JsonSerializationSpec"
```

**Python Tests:**
```bash
# Install test dependencies first
pip install -r requirements.txt

# Run Python tests (automatically starts/stops server)
python tests/test_spark_shell.py

# Or with pytest
python -m pytest tests/test_spark_shell.py -v

# Test Unity Catalog connectivity (requires UC server)
python tests/test_unity_catalog.py --uri http://localhost:8081 --token your-token --uc-catalog unity --uc-schema default
```

### Test Coverage

**Scala Tests** (14 tests):
- SparkSqlExecutor tests: SQL execution, error handling, aggregations
- JSON serialization tests: Request/response serialization

**Python Tests** (13+ tests):
- Health check and server info endpoints
- Simple SELECT queries
- CREATE TABLE and INSERT operations
- SELECT with WHERE clauses
- Aggregation queries (COUNT, AVG, MAX)
- Error handling (invalid SQL, empty SQL)
- Offline server detection

All tests use temporary tables that are cleaned up after completion.

## Project Structure

```
experimental/sparkshell/
├── bin/                      # Management scripts
│   ├── start.sh             # Start server in background
│   ├── stop.sh              # Stop server
│   ├── restart.sh           # Restart server
│   └── status.sh            # Check server status
├── build/                    # Self-contained SBT installation
├── src/main/
│   └── scala/com/sparkapp/
│       ├── RestApi.scala             # REST API implementation
│       ├── SparkAppServer.scala      # Server entry point
│       └── SparkSqlExecutor.scala    # SQL execution logic
├── src/test/scala/com/sparkapp/  # Scala tests
│   ├── SparkSqlExecutorSpec.scala
│   └── JsonSerializationSpec.scala
├── tests/                    # Python tests
│   ├── test_spark_shell.py   # Integration tests
│   └── __init__.py
├── spark_shell.py            # SparkShell class (automatic server mgmt)
├── example.py                # Simple usage example
├── run-tests                 # Unified test runner
├── build.sbt                 # Build configuration
└── README.md
```

## Architecture

1. **RestApi**: REST API implementation using Spark Java framework
2. **SparkSqlExecutor**: Executes SQL commands using Spark and formats results
3. **SparkAppServer**: Main entry point that initializes Spark and starts the REST server

## Notes

- The server runs Spark in local mode (`local[*]`)
- Log level is set to WARN to reduce noise
- The server will gracefully shutdown Spark when terminated
- Default port is 8080 (configurable)
- Uses Spark Java framework (not Apache Spark) for HTTP routing
- Supports CORS for cross-origin requests
