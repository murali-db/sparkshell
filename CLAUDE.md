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
✅ **Cloud storage**: S3, Azure, GCS support
✅ **Context manager**: Automatic lifecycle
✅ **Comprehensive tests**: 13+ Python tests, Scala unit tests
✅ **Documentation**: README, SPARK_SHELL.md, .claude_instructions

### Current Dependencies

From `build.sbt`:
```scala
libraryDependencies ++= Seq(
  "io.delta" %% "delta-spark" % "3.0.0",  // ← Hardcoded version
  "io.unitycatalog" % "unitycatalog-spark" % "0.1.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  // ... cloud storage connectors
)
```

### Limitations

❌ **Delta version is hardcoded**: Must use 3.0.0
❌ **Cannot point to custom Delta builds**: No source repo support
❌ **Cannot test Delta branches**: No flexibility for development

## Next Steps: Configurable Delta Dependency

### Goal

Add `DeltaConfig` to point SparkShell to specific Delta repositories/versions:

```python
from spark_shell import SparkShell, UCConfig, DeltaConfig

# Option 1: Specific Maven version
delta_config = DeltaConfig(version="3.1.0")

# Option 2: Build from Delta repo/branch
delta_config = DeltaConfig(
    source_repo="https://github.com/tdas/delta",
    source_branch="oss-in-dbr"
)

# Option 3: Use local Delta repo
delta_config = DeltaConfig(source_path="/home/murali/delta")

with SparkShell(
    source=".",
    uc_config=uc_config,
    delta_config=delta_config
) as shell:
    result = shell.execute_sql("SELECT 1")
```

### Implementation Plan

**1. Add DeltaConfig dataclass** (`spark_shell.py`)
```python
@dataclass
class DeltaConfig:
    """Delta Lake dependency configuration."""
    # Maven-based (default)
    version: str = "3.0.0"
    maven_repo: Optional[str] = None

    # Source-based (optional)
    source_repo: Optional[str] = None  # GitHub URL
    source_branch: str = "master"       # Branch name
    source_path: Optional[str] = None   # Local path
```

**2. Update SparkShell class**
- Add `delta_config` parameter to `__init__`
- Implement `_setup_delta_from_source()` method
- Modify `build()` to handle Delta source builds
- Update cache keys to include Delta config

**3. Make build.sbt configurable**
```scala
val deltaVersion = sys.env.getOrElse("DELTA_VERSION", "3.0.0")
libraryDependencies += "io.delta" %% "delta-spark" % deltaVersion
```

**4. Build Delta from source when needed**
- Clone Delta repo to work_dir/delta
- Run `build/sbt publishLocal` in Delta directory
- Update resolvers in build.sbt to use local Maven

**5. Update cache system**
- Include Delta config in cache key hash
- Separate caches for different Delta versions/sources
- Prevent cache collisions

**6. Add tests**
- Test Maven version configuration
- Test source repo builds
- Test local path support
- Verify cache isolation

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

## Vision

SparkShell aims to be the **de facto tool for OSS Spark + Databricks Unity Catalog integration**.

**Short term**:
- ✅ Standalone repository (this step!)
- 🔄 Configurable Delta dependency (next)
- 🔄 Published PyPI package
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
