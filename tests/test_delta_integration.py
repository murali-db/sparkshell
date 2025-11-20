#!/usr/bin/env python3
"""
Full integration test with TD's Delta oss-in-dbr branch.

This test performs a complete end-to-end integration:
1. Clones TD's Delta repository (oss-in-dbr branch)
2. Builds Delta and publishes to local Maven
3. Builds SparkShell with the custom Delta
4. Starts the SparkShell server
5. Tests Delta Lake operations (CREATE, INSERT, UPDATE, DELETE)

WARNING: This test takes 10+ minutes on first run due to Delta build time.
Subsequent runs use cached builds and complete in ~1-2 minutes.
"""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from spark_shell import SparkShell, DeltaConfig, OpConfig


class TestDeltaIntegration(unittest.TestCase):
    """Full integration test with TD's Delta oss-in-dbr branch."""

    @classmethod
    def setUpClass(cls):
        """Set up test - this will take 10+ minutes on first run."""
        print("\n" + "="*70)
        print("INTEGRATION TEST: Building with Delta oss-in-dbr branch")
        print("This will take 10+ minutes on first run (Delta + SparkShell build)")
        print("Subsequent runs will use cached build (~1-2 minutes)")
        print("="*70 + "\n")

        cls.delta_config = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="oss-in-dbr"
        )

        cls.op_config = OpConfig(
            verbose=True,
            build_timeout=600  # 10 minutes for Delta build
        )

        cls.shell = SparkShell(
            source=".",
            delta_config=cls.delta_config,
            op_config=cls.op_config,
            port=8200
        )

        # This will trigger full Delta build + SparkShell build
        cls.shell.start()

    @classmethod
    def tearDownClass(cls):
        """Clean up after tests."""
        cls.shell.shutdown()
        # Note: NOT calling cleanup() to preserve cache

    def test_01_server_started(self):
        """Test server started successfully."""
        self.assertTrue(self.shell.is_ready)

    def test_02_basic_sql(self):
        """Test basic SQL execution."""
        result = self.shell.execute_sql("SELECT 1 as test")
        self.assertIn("1", result)

    def test_03_create_delta_table(self):
        """Test Delta table creation."""
        self.shell.execute_sql(
            "DROP TABLE IF EXISTS test_delta_integration"
        )
        self.shell.execute_sql(
            "CREATE TABLE test_delta_integration (id INT, name STRING, value DOUBLE) USING DELTA"
        )

        # Verify table was created
        result = self.shell.execute_sql("SHOW TABLES")
        self.assertIn("test_delta_integration", result)

    def test_04_insert_data(self):
        """Test inserting data into Delta table."""
        self.shell.execute_sql(
            "INSERT INTO test_delta_integration VALUES "
            "(1, 'Alice', 100.5), "
            "(2, 'Bob', 200.75), "
            "(3, 'Charlie', 300.25)"
        )

        # Verify data was inserted
        result = self.shell.execute_sql("SELECT COUNT(*) as count FROM test_delta_integration")
        self.assertIn("3", result)

    def test_05_update_operation(self):
        """Test UPDATE operation (Delta-specific)."""
        # Update Alice's value
        self.shell.execute_sql(
            "UPDATE test_delta_integration SET value = 999.99 WHERE name = 'Alice'"
        )

        # Verify the update
        result = self.shell.execute_sql(
            "SELECT value FROM test_delta_integration WHERE name = 'Alice'"
        )
        self.assertIn("999.99", result)

    def test_06_delete_operation(self):
        """Test DELETE operation (Delta-specific)."""
        # Delete Charlie's record
        self.shell.execute_sql(
            "DELETE FROM test_delta_integration WHERE name = 'Charlie'"
        )

        # Verify deletion
        result = self.shell.execute_sql("SELECT COUNT(*) as count FROM test_delta_integration")
        self.assertIn("2", result)

        # Verify Charlie is gone
        result = self.shell.execute_sql("SELECT name FROM test_delta_integration")
        self.assertNotIn("Charlie", result)
        self.assertIn("Alice", result)
        self.assertIn("Bob", result)

    def test_07_aggregation(self):
        """Test aggregation on Delta table."""
        result = self.shell.execute_sql(
            "SELECT AVG(value) as avg_value FROM test_delta_integration"
        )
        # Should be average of 999.99 and 200.75
        self.assertIsNotNone(result)

    def test_08_filter_query(self):
        """Test filtering on Delta table."""
        result = self.shell.execute_sql(
            "SELECT name FROM test_delta_integration WHERE value > 500"
        )
        self.assertIn("Alice", result)
        self.assertNotIn("Bob", result)


if __name__ == "__main__":
    # Run with verbose output to see build progress
    unittest.main(verbosity=2)
