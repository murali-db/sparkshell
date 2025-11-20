#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Integration tests for SparkShell class.

These tests will automatically start a SparkApp server using SparkShell,
run tests against it, and clean up automatically.

Tests include:
- Configuration classes (UCConfig, OpConfig, SparkConfig)
- SparkShell initialization with config classes
- Default configuration values

Run with: python -m pytest tests/test_spark_shell.py -v
Or: python tests/test_spark_shell.py
"""

import unittest
import sys
import os
import shutil
import tempfile
import atexit

# Create a temporary directory and copy only spark_shell.py to it
# This ensures spark_shell.py is truly standalone with no dependencies on other repo files
_test_dir = os.path.dirname(os.path.abspath(__file__))
_sparkshell_dir = os.path.dirname(_test_dir)
_temp_dir = tempfile.mkdtemp(prefix="sparkshell_standalone_test_")
_spark_shell_src = os.path.join(_sparkshell_dir, "spark_shell.py")
_spark_shell_dest = os.path.join(_temp_dir, "spark_shell.py")

# Copy spark_shell.py to temp directory
shutil.copy2(_spark_shell_src, _spark_shell_dest)
print(f"[TEST SETUP] Copied spark_shell.py to temporary directory: {_temp_dir}")
print(f"[TEST SETUP] This ensures spark_shell.py is standalone with no repo dependencies")

# Add temp directory to path to import the standalone spark_shell
sys.path.insert(0, _temp_dir)

# Clean up temp directory on exit
def _cleanup_temp_dir():
    if os.path.exists(_temp_dir):
        shutil.rmtree(_temp_dir)
        print(f"\n[TEST CLEANUP] Removed temporary test directory: {_temp_dir}")

atexit.register(_cleanup_temp_dir)

from spark_shell import SparkShell, UCConfig, OpConfig, SparkConfig


class TestSparkShell(unittest.TestCase):
    """Test cases for SparkShell class."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures - start SparkShell server."""
        print("\n" + "="*70)
        print("Setting up SparkShell for testing...")
        print("="*70)
        
        # Get the parent directory (sparkshell directory)
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)
        
        # Create SparkShell instance
        # Use a unique port to avoid conflicts
        cls.shell = SparkShell(
            source=sparkshell_dir,
            port=8090
        )
        
        # Start it manually (not using context manager since we want it for all tests)
        cls.shell.setup()
        print("✓ Setup complete")
        
        cls.shell.build()
        print("✓ Build complete")
        
        cls.shell.start()
        print("✓ Server started on port 8090")
        print("="*70 + "\n")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests - shutdown SparkShell server."""
        print("\n" + "="*70)
        print("Cleaning up SparkShell...")
        print("="*70)
        
        # Drop test table if it exists
        try:
            cls.shell.execute_sql("DROP TABLE IF EXISTS test_users")
            print("✓ Test table dropped")
        except:
            pass
        
        cls.shell.shutdown()
        print("✓ Server shutdown")
        
        cls.shell.cleanup()
        print("✓ Cleanup complete")
        print("="*70 + "\n")

    def test_01_get_server_info(self):
        """Test get_server_info method."""
        info = self.shell.get_server_info()
        self.assertIsInstance(info, dict)
        self.assertIn("sparkVersion", info)
        self.assertIn("port", info)
        self.assertEqual(str(info["port"]), "8090")
        self.assertIn("endpoints", info)
        print(f"✓ Server info: Spark {info['sparkVersion']} on port {info['port']}")

    def test_02_table_operations(self):
        """Test CREATE, INSERT, SELECT, and WHERE operations."""
        # Drop table if exists
        try:
            self.shell.execute_sql("DROP TABLE IF EXISTS test_users")
        except:
            pass

        # Create table
        result = self.shell.execute_sql(
            "CREATE TABLE test_users (id INT, name STRING, age INT) USING parquet"
        )
        self.assertIsNotNone(result)
        
        # Insert data
        result = self.shell.execute_sql(
            "INSERT INTO test_users VALUES (1, 'Alice', 30), (2, 'Bob', 25)"
        )
        self.assertIsNotNone(result)
        
        # Query data
        result = self.shell.execute_sql("SELECT * FROM test_users WHERE age > 25")
        self.assertIsNotNone(result)
        self.assertIn("Alice", result)
        self.assertNotIn("Bob", result)  # age = 25, not > 25
        print("✓ Table operations (CREATE/INSERT/SELECT/WHERE) successful")

    def test_03_aggregation(self):
        """Test aggregation query."""
        result = self.shell.execute_sql(
            "SELECT COUNT(*) as count, AVG(age) as avg_age FROM test_users"
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertIn("count", result)
        self.assertIn("avg_age", result)
        print("✓ Aggregation query successful")

    def test_04_delta_operations(self):
        """Test Delta Lake operations."""
        # Drop table if exists
        try:
            self.shell.execute_sql("DROP TABLE IF EXISTS delta_test")
        except:
            pass
        
        # Create Delta table
        result = self.shell.execute_sql(
            "CREATE TABLE delta_test (id INT, value STRING, amount DOUBLE) USING DELTA"
        )
        self.assertIsNotNone(result)
        print("✓ Create Delta table successful")
        
        # Insert data into Delta table
        result = self.shell.execute_sql(
            "INSERT INTO delta_test VALUES (1, 'a', 10.5), (2, 'b', 20.0), (3, 'c', 30.5)"
        )
        self.assertIsNotNone(result)
        
        # Query Delta table
        result = self.shell.execute_sql("SELECT * FROM delta_test ORDER BY id")
        self.assertIn("a", result)
        self.assertIn("b", result)
        self.assertIn("c", result)
        
        # Update Delta table
        result = self.shell.execute_sql("UPDATE delta_test SET amount = 15.0 WHERE id = 1")
        self.assertIsNotNone(result)
        
        # Verify update
        result = self.shell.execute_sql("SELECT amount FROM delta_test WHERE id = 1")
        self.assertIn("15", result)
        
        # Delete from Delta table
        result = self.shell.execute_sql("DELETE FROM delta_test WHERE id = 3")
        self.assertIsNotNone(result)
        
        # Verify delete
        result = self.shell.execute_sql("SELECT COUNT(*) as count FROM delta_test")
        self.assertIn("2", result)
        
        # Cleanup
        self.shell.execute_sql("DROP TABLE delta_test")
        print("✓ Delta operations (CREATE/INSERT/UPDATE/DELETE) successful")
    
    def test_05_error_handling(self):
        """Test error handling with invalid and empty SQL."""
        # Invalid SQL
        with self.assertRaises(RuntimeError) as context:
            self.shell.execute_sql("SELECT * FROM nonexistent_table")
        error_msg = str(context.exception)
        self.assertTrue(
            "table" in error_msg.lower() or "view" in error_msg.lower() or "not found" in error_msg.lower(),
            f"Expected error message, got: {error_msg}"
        )
        
        # Empty SQL
        with self.assertRaises(RuntimeError):
            self.shell.execute_sql("")
        
        print("✓ Error handling works correctly")


class TestSparkShellContextManager(unittest.TestCase):
    """Test SparkShell context manager functionality."""

    def test_context_manager(self):
        """Test SparkShell with context manager (with statement)."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)

        op_config = OpConfig(cleanup_on_exit=True)
        with SparkShell(source=sparkshell_dir, port=8091, op_config=op_config) as shell:
            # Test SQL execution
            result = shell.execute_sql("SELECT 1 as test_value")
            self.assertIsNotNone(result)
            self.assertIn("test_value", result)
        
        # After exiting context, server should be shutdown
        self.assertFalse(shell.is_ready)
        print("✓ Context manager works correctly")


class TestSparkShellManualControl(unittest.TestCase):
    """Test SparkShell manual control (step-by-step)."""

    def test_manual_lifecycle(self):
        """Test manual control of SparkShell lifecycle."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)
        
        op_config = OpConfig(auto_start=False, cleanup_on_exit=False)
        shell = SparkShell(
            source=sparkshell_dir,
            port=8092,
            op_config=op_config
        )

        try:
            # Test manual start (setup and build happen automatically)
            shell.start()
            self.assertIsNotNone(shell.work_dir)
            self.assertIsNotNone(shell.jar_path)
            print("✓ Manual start successful (auto setup/build)")
            
            # Test SQL execution
            self.assertTrue(shell.is_ready)
            result = shell.execute_sql("SELECT 1 as val")
            self.assertIn("val", result)
            print("✓ Manual SQL execution successful")
            
        finally:
            shell.shutdown()
            shell.cleanup()
            print("✓ Manual cleanup successful")


class TestConfigurationClasses(unittest.TestCase):
    """Test configuration classes (UCConfig, OpConfig, SparkConfig)."""

    def test_op_config(self):
        """Test OpConfig dataclass."""
        op_config = OpConfig(
            verbose=False,
            auto_start=False,
            cleanup_on_exit=True,
            startup_timeout=120,
            build_timeout=600
        )

        self.assertFalse(op_config.verbose)
        self.assertFalse(op_config.auto_start)
        self.assertTrue(op_config.cleanup_on_exit)
        self.assertEqual(op_config.startup_timeout, 120)
        self.assertEqual(op_config.build_timeout, 600)
        print("✓ OpConfig works correctly")

    def test_uc_config(self):
        """Test UCConfig dataclass."""
        uc_config = UCConfig(
            uri="http://localhost:8081",
            token="test-token",
            catalog="my_catalog",
            schema="my_schema"
        )

        self.assertEqual(uc_config.uri, "http://localhost:8081")
        self.assertEqual(uc_config.token, "test-token")
        self.assertEqual(uc_config.catalog, "my_catalog")
        self.assertEqual(uc_config.schema, "my_schema")
        print("✓ UCConfig works correctly")

    def test_spark_config(self):
        """Test SparkConfig dataclass."""
        spark_config = SparkConfig(
            configs={
                "spark.executor.memory": "2g",
                "spark.driver.memory": "1g",
                "spark.sql.shuffle.partitions": "10"
            }
        )

        self.assertEqual(spark_config.configs["spark.executor.memory"], "2g")
        self.assertEqual(spark_config.configs["spark.driver.memory"], "1g")
        self.assertEqual(spark_config.configs["spark.sql.shuffle.partitions"], "10")
        print("✓ SparkConfig works correctly")

    def test_sparkshell_with_config_classes(self):
        """Test SparkShell initialization with configuration classes."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)

        op_config = OpConfig(verbose=False, auto_start=False)
        spark_config = SparkConfig(configs={"spark.sql.shuffle.partitions": "10"})
        uc_config = UCConfig(uri="http://localhost:8081", token="test-token")

        shell = SparkShell(
            source=sparkshell_dir,
            port=8093,
            op_config=op_config,
            spark_config=spark_config,
            uc_config=uc_config
        )

        # Verify configs were applied
        self.assertFalse(shell.op_config.verbose)
        self.assertEqual(shell.spark_config.configs["spark.sql.shuffle.partitions"], "10")
        self.assertEqual(shell.uc_config.uri, "http://localhost:8081")
        self.assertEqual(shell.uc_config.token, "test-token")
        print("✓ SparkShell with config classes works correctly")

    def test_default_configs(self):
        """Test SparkShell with default configuration objects."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)

        # No config objects provided - should use defaults
        shell = SparkShell(
            source=sparkshell_dir,
            port=8094
        )

        # Verify default configs were created
        self.assertTrue(shell.op_config.verbose)  # Default is True
        self.assertTrue(shell.op_config.auto_start)  # Default is True
        self.assertTrue(shell.op_config.cleanup_on_exit)  # Default is True
        self.assertEqual(shell.op_config.startup_timeout, 60)  # Default
        self.assertEqual(shell.op_config.build_timeout, 300)  # Default
        self.assertEqual(shell.uc_config.catalog, "unity")  # Default
        print("✓ Default configs work correctly")


class TestSbtoptsPlacement(unittest.TestCase):
    """Test cases to ensure .sbtopts is correctly placed in work_dir."""

    def test_sbtopts_placement_fresh_and_cached(self):
        """Test that .sbtopts is placed correctly for both fresh and cached builds."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)

        # Test fresh build - create a build that will be cached
        shell1 = SparkShell(
            source=sparkshell_dir,
            port=8095,
            op_config=OpConfig(
                verbose=False,
                auto_start=False,
                cleanup_on_exit=False  # Don't clean up so cache persists
            )
        )

        try:
            shell1.setup()
            shell1.build()
            cache_dir = shell1._get_cache_dir()

            # Verify .sbtopts exists after fresh build
            sbtopts_path = shell1.work_dir / ".sbtopts"
            self.assertTrue(sbtopts_path.exists(),
                           f".sbtopts should exist at {sbtopts_path}")
            print("✓ .sbtopts correctly placed for fresh build")

            # Test cached build - create a second shell that will use the cache
            shell2 = SparkShell(
                source=sparkshell_dir,
                port=8096,
                op_config=OpConfig(
                    verbose=False,
                    auto_start=False,
                    cleanup_on_exit=True
                )
            )

            try:
                shell2.setup()

                # Verify cache exists before build
                self.assertTrue(shell2._has_cached_build(),
                               "Cache should exist from first build")

                # Build (should use cache)
                shell2.build()

                # Verify work_dir points to cache
                self.assertEqual(shell2.work_dir, cache_dir,
                                "work_dir should point to cache directory")

                # Verify .sbtopts exists in the cache directory
                sbtopts_cached_path = cache_dir / ".sbtopts"
                self.assertTrue(sbtopts_cached_path.exists(),
                               f".sbtopts should exist in cache at {sbtopts_cached_path}")
                print("✓ .sbtopts correctly placed when using cached build")

            finally:
                # Clean up shell2's temp work_dir if it exists
                if shell2.work_dir and shell2.work_dir != cache_dir and shell2.work_dir.exists():
                    import shutil
                    shutil.rmtree(shell2.work_dir)

        finally:
            # Clean up cache from shell1
            if shell1.work_dir and shell1.work_dir.exists():
                import shutil
                shutil.rmtree(shell1.work_dir)


if __name__ == "__main__":
    # Run tests with verbose output
    print("\n" + "="*70)
    print("SparkShell Integration Tests")
    print("="*70)
    print("This will build and start SparkShell, run tests, and cleanup.")
    print("First run may take 3-6 minutes due to SBT build.")
    print("="*70 + "\n")

    unittest.main(verbosity=2)

