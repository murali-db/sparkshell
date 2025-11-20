#!/usr/bin/env python3
"""
Tests for DeltaConfig class and cache key isolation.

These tests verify:
1. DeltaConfig can be created with valid parameters
2. DeltaConfig validates URL format
3. Cache keys include Delta configuration
4. Different Delta configs produce different cache directories
"""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from spark_shell import SparkShell, DeltaConfig, OpConfig


class TestDeltaConfigFoundation(unittest.TestCase):
    """Test DeltaConfig class and cache key isolation."""

    def test_delta_config_creation(self):
        """Test DeltaConfig can be created with valid parameters."""
        config = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="oss-in-dbr"
        )
        self.assertEqual(config.source_repo, "https://github.com/tdas/delta")
        self.assertEqual(config.source_branch, "oss-in-dbr")

    def test_delta_config_default_branch(self):
        """Test DeltaConfig defaults to master branch."""
        config = DeltaConfig(source_repo="https://github.com/delta-io/delta")
        self.assertEqual(config.source_branch, "master")

    def test_delta_config_invalid_url(self):
        """Test DeltaConfig rejects non-URL source_repo."""
        with self.assertRaises(ValueError) as ctx:
            DeltaConfig(source_repo="/local/path")

        self.assertIn("must be a URL", str(ctx.exception))
        self.assertIn("/local/path", str(ctx.exception))

    def test_delta_config_get_cache_key_component(self):
        """Test get_cache_key_component returns unique string."""
        config1 = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="oss-in-dbr"
        )
        config2 = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="master"
        )

        key1 = config1.get_cache_key_component()
        key2 = config2.get_cache_key_component()

        # Keys should be different
        self.assertNotEqual(key1, key2)

        # Keys should include repo and branch
        self.assertIn("delta", key1)
        self.assertIn("oss-in-dbr", key1)
        self.assertIn("master", key2)

    def test_cache_key_includes_delta_config(self):
        """Test cache key includes Delta configuration."""
        delta1 = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="oss-in-dbr"
        )
        delta2 = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="master"
        )

        # Use quiet mode for cleaner test output
        op_config = OpConfig(verbose=False)

        shell1 = SparkShell(source=".", delta_config=delta1, port=8100, op_config=op_config)
        shell2 = SparkShell(source=".", delta_config=delta2, port=8101, op_config=op_config)

        # Different Delta branches should produce different cache keys
        self.assertNotEqual(shell1._get_source_hash(), shell2._get_source_hash())

    def test_cache_isolation(self):
        """Test different Delta configs use different cache directories."""
        delta1 = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="oss-in-dbr"
        )
        delta2 = DeltaConfig(
            source_repo="https://github.com/delta-io/delta",
            source_branch="master"
        )

        # Use quiet mode for cleaner test output
        op_config = OpConfig(verbose=False)

        shell1 = SparkShell(source=".", delta_config=delta1, port=8100, op_config=op_config)
        shell2 = SparkShell(source=".", delta_config=delta2, port=8101, op_config=op_config)

        # Should have different cache directories
        self.assertNotEqual(shell1._get_cache_dir(), shell2._get_cache_dir())

    def test_same_config_same_cache(self):
        """Test identical Delta configs produce the same cache directory."""
        delta1 = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="oss-in-dbr"
        )
        delta2 = DeltaConfig(
            source_repo="https://github.com/tdas/delta",
            source_branch="oss-in-dbr"
        )

        # Use quiet mode for cleaner test output
        op_config = OpConfig(verbose=False)

        shell1 = SparkShell(source=".", delta_config=delta1, port=8100, op_config=op_config)
        shell2 = SparkShell(source=".", delta_config=delta2, port=8101, op_config=op_config)

        # Identical configs should produce same cache directory
        self.assertEqual(shell1._get_cache_dir(), shell2._get_cache_dir())

    def test_sparkshell_with_default_delta_config(self):
        """Test SparkShell uses default Delta config if none provided."""
        op_config = OpConfig(verbose=False)
        shell = SparkShell(source=".", port=8100, op_config=op_config)

        # Should have default Delta config
        self.assertIsNotNone(shell.delta_config)
        self.assertEqual(shell.delta_config.source_repo, "https://github.com/delta-io/delta")
        self.assertEqual(shell.delta_config.source_branch, "master")


if __name__ == "__main__":
    unittest.main()
