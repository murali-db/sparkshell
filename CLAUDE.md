# SparkShell - Standalone Project

## Project Purpose

**SparkShell** enables **Open Source Spark to work seamlessly with Databricks Unity Catalog and Delta Lake**. This standalone project was extracted from TD's experimental work in the Delta Lake repository to make it independently maintainable and extensible.

### What Problem Does This Solve?

Organizations want to:
- **Develop locally** with Open Source Spark (free, no cloud costs)
- **Connect to** Databricks Unity Catalog for metadata and governance
- **Execute** Delta Lake ACID operations (DELETE/UPDATE/MERGE) from OSS Spark
- **Share data** seamlessly between OSS and Databricks Runtime (DBR)
- **Deploy confidently** knowing behavior is identical across environments

SparkShell makes this trivial:

```python
from spark_shell import SparkShell, UCConfig

uc_config = UCConfig(
    uri="https://your-databricks.cloud.databricks.com/",
    token="your-token",
    catalog="main",
    schema="default"
)

with SparkShell(source=".", uc_config=uc_config) as shell:
    # OSS Spark executing against Databricks Unity Catalog!
    result = shell.execute_sql("SELECT * FROM my_table")
    print(result)
```

## Project Origin

### History

This code was originally developed by **Tathagata Das (TD)** in the Delta Lake repository:
- **Location**: `github.com/tdas/delta/tree/oss-in-dbr/experimental/sparkshell`
- **Branch**: `oss-in-dbr` (Open Source Spark in Databricks Runtime)
- **Purpose**: Research and prove OSS/DBR interoperability
- **Commits**: 18 commits (b98468b → 1a0383f) building the complete system

### Why Standalone?

**Original**: Part of Delta Lake experimental folder
**Now**: Independent project with its own lifecycle

**Benefits of standalone**:
1. **Independent versioning** - Not tied to Delta Lake releases
2. **Faster development** - No need to coordinate with Delta Lake repo
3. **Configurable Delta** - Can point to any Delta Lake version/fork
4. **Broader adoption** - Easier to discover and use
5. **Clear purpose** - Focused on OSS/UC/Delta interoperability

## Architecture Overview

### The Stack

```
┌─────────────────────────────────────────────────────┐
│  Python Layer: spark_shell.py                        │
│  - Lifecycle management (download, build, cache)    │
│  - Configuration (UCConfig, OpConfig, SparkConfig)  │
│  - REST API client                                   │
└────────────────────┬────────────────────────────────┘
                     │ HTTP POST /sql
                     ↓
┌─────────────────────────────────────────────────────┐
│  Scala REST Server: SparkShellServer.scala          │
│  - HTTP endpoints (/health, /info, /sql)           │
│  - SparkSqlExecutor (query execution)              │
└────────────────────┬────────────────────────────────┘
                     │ spark.sql(query)
                     ↓
┌─────────────────────────────────────────────────────┐
│  Apache Spark SQL + Delta Lake                      │
│  - SQL parser and optimizer                         │
│  - Delta Lake ACID transactions                     │
│  - Unity Catalog connector                          │
└────────────────────┬────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         │                       │
         ↓                       ↓
┌─────────────────┐   ┌─────────────────────────────┐
│  Delta Lake     │   │  Unity Catalog (Databricks) │
│  - ACID txns    │   │  - REST API metadata        │
│  - Time travel  │   │  - Access control          │
│  - Txn log      │   │  - Table locations         │
└────────┬────────┘   └────────┬────────────────────┘
         │                     │
         └──────────┬──────────┘
                    ↓
         ┌─────────────────────┐
         │  Cloud Storage       │
         │  - S3 / ADLS / GCS  │
         │  - Parquet files    │
         │  - Delta txn logs   │
         └─────────────────────┘
```

### Key Components

**1. Python Management Layer** (`spark_shell.py`)
- Automatic download/copy of source code
- SBT build with assembly JAR creation
- Intelligent build caching (~/.sparkshell_cache/)
- Server lifecycle management (start, stop, health checks)
- Configuration classes (UCConfig, OpConfig, SparkConfig)

**2. Scala REST Server** (`src/main/scala/com/sparkshell/`)
- `SparkShellServer.scala`: Entry point and SparkSession setup
- `RestApi.scala`: HTTP endpoints using Spark Java framework
- `SparkSqlExecutor.scala`: SQL execution and result formatting

**3. Build System** (`build.sbt`, `build/sbt`)
- Self-contained SBT installation (no global dependency)
- Assembly JAR with all dependencies (~200MB)
- Custom .sbtopts for memory management (prevents OOM)

**4. Unity Catalog Integration**
- `io.unitycatalog.spark.UCSingleCatalog` plugin
- REST API communication with Databricks
- Token-based authentication
- Three-level namespace support (catalog.schema.table)

**5. Delta Lake Integration**
- Currently: Maven dependency `io.delta:delta-spark:3.0.0`
- Provides: ACID transactions, time travel, UPDATE/DELETE/MERGE
- Transaction log protocol for consistency

## Current State

### What Works

✅ **Zero-config setup**: Single line to start
✅ **Automatic building**: SBT assembly with caching
✅ **Unity Catalog**: Full OSS Spark integration
✅ **Delta Lake**: ACID operations from OSS
✅ **Configurable Delta**: Point to any Delta repo/branch
✅ **Cloud storage**: S3, Azure, GCS support
✅ **Context manager**: Automatic lifecycle
✅ **Comprehensive tests**: 20+ Python tests, Scala unit tests
✅ **Documentation**: README, SPARK_SHELL.md, .claude_instructions

### DeltaConfig Support

✅ **Build from GitHub**: Clone and build Delta from any repository/branch
✅ **Cache isolation**: Different Delta configs use separate caches
✅ **Version extraction**: Automatic Delta version detection from version.sbt
✅ **Local Maven publishing**: Delta built and published to ~/.m2/repository/
✅ **Environment variables**: DELTA_VERSION and DELTA_USE_LOCAL passed to SBT

Example:
```python
from spark_shell import SparkShell, DeltaConfig

delta_config = DeltaConfig(
    source_repo="https://github.com/tdas/delta",
    source_branch="oss-in-dbr"
)

with SparkShell(source=".", delta_config=delta_config) as shell:
    result = shell.execute_sql("SELECT * FROM my_table")
```

### Build Time Expectations

- **First run**: 10-15 minutes (Delta build + SparkShell build)
- **Cached run**: 1-2 minutes (uses cached Delta + SparkShell)
- **Different Delta branch**: New full build required
- **Same Delta branch**: Uses cache

## ✅ Completed: Configurable Delta Dependency

DeltaConfig has been successfully implemented! SparkShell can now build against custom Delta Lake repositories and branches.

### Implementation Summary

**DeltaConfig dataclass** (`spark_shell.py`)
```python
@dataclass
class DeltaConfig:
    """Delta Lake dependency configuration."""
    source_repo: str                    # GitHub URL (required)
    source_branch: str = "master"       # Branch name (default: master)
```

**Features Implemented:**
- ✅ Clone Delta from any GitHub repository
- ✅ Checkout specific branch
- ✅ Build Delta with `publishLocal`
- ✅ Extract Delta version from `version.sbt`
- ✅ Pass DELTA_VERSION and DELTA_USE_LOCAL to SBT
- ✅ Cache isolation per Delta configuration
- ✅ Java 17 compatibility for Spark 4.0
- ✅ ANTLR 4.9.3 version pinning for Delta parser
- ✅ Comprehensive error handling
- ✅ Unit tests + integration tests (21 total tests)

**Files Modified:**
- `spark_shell.py`: Added DeltaConfig, _setup_delta(), _build_delta(), _get_delta_version(), Java 17 support
- `build.sbt`: Environment variable support + ANTLR 4.9.3 pinning
- `tests/`: 21 tests (13 unit + 8 integration)

### Usage

```python
from spark_shell import SparkShell, DeltaConfig

# Build from TD's oss-in-dbr branch
delta_config = DeltaConfig(
    source_repo="https://github.com/tdas/delta",
    source_branch="oss-in-dbr"
)

with SparkShell(source=".", delta_config=delta_config) as shell:
    # Execute Delta operations
    shell.execute_sql("CREATE TABLE test (id INT) USING DELTA")
    shell.execute_sql("INSERT INTO test VALUES (1), (2), (3)")
    shell.execute_sql("UPDATE test SET id = 10 WHERE id = 1")
    result = shell.execute_sql("SELECT * FROM test")
```

### Stacked PR Implementation

DeltaConfig was implemented through 4 stacked PRs:

1. **PR #1: Foundation** - DeltaConfig class + cache isolation
2. **PR #2: Environment vars** - SBT environment variable support
3. **PR #3: Build methods** - Delta clone/build implementation
4. **PR #4: Integration** - Wire everything together + integration test

Each PR includes comprehensive unit tests and documentation.

### Why This Matters

**For Development**:
- Test SparkShell against unreleased Delta features
- Verify compatibility with Delta Lake branches
- Debug Delta-specific issues locally

**For Users**:
- Pin to specific Delta versions for stability
- Use custom Delta forks if needed
- Upgrade Delta independently of SparkShell

**For Research**:
- Test OSS/DBR interoperability with different Delta versions
- Validate Delta protocol changes
- Benchmark Delta performance improvements

## What TD Built (Summary)

TD's 18 commits created a production-ready system:

**Key Features**:
1. **Intelligent build caching** - 50-66% time savings
2. **Configuration architecture** - UCConfig, OpConfig, SparkConfig
3. **Standalone module** - Embedded .sbtopts, zero external deps
4. **Delta + UC integration** - ACID ops, three-level namespace
5. **SBT memory management** - Custom .sbtopts prevents OOM
6. **Output control** - Verbose mode, selective logging
7. **Automatic lifecycle** - Context manager, auto setup/build
8. **Comprehensive testing** - 13+ Python tests, Scala unit tests

**Performance**:
- First run: 3-6 minutes
- Cached run: 1-2 minutes
- 50-66% speedup from caching

**Code Quality**:
- +3,011 lines added, -591 removed
- Full type hints and docstrings
- Comprehensive error handling
- Three documentation files (1,056 lines total)

## Development Guidelines

### Working with SparkShell

**Run tests**:
```bash
./run-tests.sh  # Runs all Scala + Python tests
```

**Build manually**:
```bash
build/sbt assembly
```

**Run example**:
```bash
python spark_shell_example.py
```

### Code Organization

Follow the patterns established by TD:
- **Use dataclasses** for configuration groups
- **Respect verbose mode** for all command output
- **Cache aggressively** to improve performance
- **Validate early** before expensive operations
- **Test comprehensively** with regression tests

See `.claude_instructions` for detailed development guidelines.

## 🔄 In Progress: FGAC (Fine-Grained Access Control) Support

### Goal
Enable OSS Spark to read tables with FGAC policies (column masking, row-level security) from Databricks Unity Catalog.

### How It Works
1. UC's new `MATERIALIZED_JSON` mode returns **presigned HTTPS URLs** pointing to JSON data
2. FGAC policies are applied server-side before data is returned
3. Client just fetches JSON from presigned URLs - no raw credentials needed

### Delta Branch
**Repository**: `https://github.com/murali-db/delta`
**Branch**: `server-side-planning-D-credentials-injection`

### Implementation Status - ALL CODE CHANGES COMPLETE

**All commits pushed and ready for testing:**

**Delta Repo (`murali-db/delta` branch `server-side-planning-D-credentials-injection`):**
1. `46545999c` - Add PresignedUrlJsonPartitionReader (204 insertions)
2. `346bf748e` - Fix Scaladoc errors (wrap JSON examples in `{{{...}}}`)
3. `66c26d792` - Fix duplicate import (remove `scala.jdk.CollectionConverters._`)
4. `acbf8ed63` - **CRITICAL**: Add `?implementation=MATERIALIZED_JSON` to plan endpoint
5. `dd067a3d6` - Fix scalastyle line length (split URL string)
6. `da99aed21` - Add debug prints to trace UC response
7. `7752af107` - Print full JSON response (not just 500 chars)
8. `0a0ee5fb0` - **CRITICAL**: Enable Bearer token authentication for plan endpoint
9. `1e69c5d9f` - **CRITICAL**: Fix Iceberg REST catalog endpoint URL structure

**SparkShell Repo (`murali-db/sparkshell` branch `master`):**
10. `4e9960a` - Remove GCS connector due to protobuf version conflict

**Features Implemented:**
- ✅ `PresignedUrlJsonPartitionReader` class in `ServerSidePlannedTable.scala`
- ✅ Detection logic: `filePath.startsWith("https://")` routes to JSON reader
- ✅ JSON parsing: array-of-arrays format `[[val1, val2], [val3, val4]]`
- ✅ Schema-aware conversion: JSON → InternalRow
- ✅ `?implementation=MATERIALIZED_JSON` query parameter
- ✅ Bearer token authentication for plan endpoint
- ✅ Correct URL structure: `/iceberg-rest/v1/catalogs/{catalog}/namespaces/...`
- ✅ Debug print statements for troubleshooting
- ✅ GCS connector removed from SparkShell (protobuf conflict fix)

**Key Files Modified in Delta:**
- `spark/.../serverSidePlanning/ServerSidePlannedTable.scala` - JSON reader + detection + debug prints
- `spark/.../serverSidePlanning/UnityCatalogMetadata.scala` - Endpoint URL construction (fixed to `/iceberg-rest/v1/catalogs/$catalogName`)
- `iceberg/.../serverSidePlanning/IcebergRESTCatalogPlanningClient.scala` - Plan request + auth + debug prints

**Endpoint URL Structure (CORRECTED):**
```
POST /api/2.1/unity-catalog/iceberg-rest/v1/catalogs/{catalog}/namespaces/{schema}/tables/{table}/plan?implementation=MATERIALIZED_JSON
```

**Test with curl:**
```bash
export UC_TOKEN="<PAT>"
export UC_HOST="https://e2-dogfood.staging.cloud.databricks.com"
export CATALOG="main"
export SCHEMA="default"
export TABLE="spark_shell_test_table"

curl -X POST \
  "${UC_HOST}/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/${CATALOG}/namespaces/${SCHEMA}/tables/${TABLE}/plan?implementation=MATERIALIZED_JSON" \
  -H "Authorization: Bearer ${UC_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"snapshot-id": 0}'
```

**Debug Output (when running):**
```
[DEBUG IcebergRESTCatalogPlanningClient] Raw response (first 1000 chars): ...
[DEBUG IcebergRESTCatalogPlanningClient] Number of file scan tasks: X
[DEBUG IcebergRESTCatalogPlanningClient] File path: https://... or s3://...
[DEBUG PresignedUrlJsonPartitionReader] Fetching URL: https://...
[DEBUG PresignedUrlJsonPartitionReader] HTTP status: 200
[DEBUG PresignedUrlJsonPartitionReader] === FULL JSON RESPONSE ===
...complete JSON data...
[DEBUG PresignedUrlJsonPartitionReader] === END JSON RESPONSE ===
```

### Usage
```python
from spark_shell import SparkShell, UCConfig, DeltaConfig, OpConfig

uc_config = UCConfig(
    uri="https://your-workspace.cloud.databricks.com/",
    token="your-token",
    catalog="main",
    schema="default"
)

delta_config = DeltaConfig(
    source_repo="https://github.com/murali-db/delta",
    source_branch="server-side-planning-D-credentials-injection"
)

# Note: build_timeout=1200 recommended for first build (Delta build takes 15-20 min)
with SparkShell(source=".", uc_config=uc_config, delta_config=delta_config,
                op_config=OpConfig(verbose=True, build_timeout=1200)) as shell:
    # Query FGAC table - masked columns will show masked values!
    result = shell.execute_sql("SELECT * FROM fgac_table")
    print(result)
```

### Critical Bugs Fixed During Implementation

1. **Missing query parameter**: UC defaults to `FULL_TABLE_PARQUET` without `?implementation=MATERIALIZED_JSON`
2. **Authorization header commented out**: Bearer token was not being sent to plan endpoint
3. **Wrong URL structure**: Was `/iceberg/v1/namespaces/...`, corrected to `/iceberg-rest/v1/catalogs/{catalog}/namespaces/...`
4. **GCS connector protobuf conflict**: Removed unused GCS connector that caused Spark session init failure
5. **Duplicate imports**: `scala.jdk.CollectionConverters` conflicted with `scala.collection.JavaConverters`
6. **Scaladoc errors**: JSON examples with `[[...]]` confused Scaladoc parser

### Next Steps
1. Clear Databricks notebook cache and rebuild with latest commits
2. Test curl command to verify UC returns presigned URLs
3. Verify JSON format matches expected array-of-arrays structure
4. End-to-end test with FGAC table
5. Remove debug print statements once working

### Data Flow
```
SELECT * FROM fgac_table → SparkShell → Delta → UC POST /plan?implementation=MATERIALIZED_JSON
    ↓
UC decides: Normal table → S3 paths | FGAC table → Presigned HTTPS URLs
    ↓
createReader(): if (filePath.startsWith("https://")) → PresignedUrlJsonPartitionReader
                else → ServerSidePlannedFilePartitionReader (Parquet)
```
**Key insight**: User doesn't change SQL - UC decides format based on FGAC policies.

## Vision

SparkShell aims to be the **de facto tool for OSS Spark + Databricks Unity Catalog integration**.

**Short term**:
- ✅ Standalone repository
- ✅ Configurable Delta dependency (DeltaConfig)
- 🔄 Published PyPI package (next)
- 🔄 Docker image for portability

**Medium term**:
- Enhanced Delta version management
- Support for other catalog systems
- Performance optimizations
- Extended cloud storage support

**Long term**:
- Official Databricks integration
- Enterprise authentication options
- Multi-cluster support
- Streaming query support

## Contributors

- **Tathagata Das (TD)**: Original implementation in Delta Lake repo
- **Murali Ramanujam**: Standalone extraction and Delta flexibility

## License

[To be determined - likely Apache 2.0 to match Delta Lake]

---

**Quick Start**:
```bash
git clone https://github.com/murali-db/sparkshell
cd sparkshell
pip install -r requirements.txt
python spark_shell_example.py
```

That's it! Welcome to SparkShell. 🚀
