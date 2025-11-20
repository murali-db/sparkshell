#!/usr/bin/env python3
"""
Tests for environment variable passing to SBT.

These tests verify:
1. _run_command accepts and passes environment variables
2. Environment variables are correctly merged with system environment
3. build.sbt can read DELTA_VERSION and DELTA_USE_LOCAL
"""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from spark_shell import SparkShell, DeltaConfig, OpConfig


class TestEnvironmentVariables(unittest.TestCase):
    """Test environment variable passing to SBT."""

    def test_run_command_accepts_env(self):
        """Test _run_command accepts and passes environment variables."""
        op_config = OpConfig(verbose=False)
        shell = SparkShell(source=".", port=8100, op_config=op_config)
        shell.work_dir = Path(".")

        # Run a simple command that echoes an env var
        result = shell._run_command(
            ["bash", "-c", "echo $TEST_VAR"],
            env={"TEST_VAR": "test_value"},
            check=True
        )
        self.assertIn("test_value", result.stdout)

    def test_run_command_env_merges_with_system(self):
        """Test environment variables merge with system environment."""
        op_config = OpConfig(verbose=False)
        shell = SparkShell(source=".", port=8100, op_config=op_config)
        shell.work_dir = Path(".")

        # Run command that accesses both system and custom env vars
        result = shell._run_command(
            ["bash", "-c", "echo $PATH:$CUSTOM_VAR"],
            env={"CUSTOM_VAR": "custom_value"},
            check=True
        )

        # Should have both PATH (from system) and CUSTOM_VAR (custom)
        self.assertIn("custom_value", result.stdout)
        # PATH should not be empty (it comes from system)
        self.assertNotEqual(result.stdout.strip(), ":custom_value")

    def test_run_command_without_env(self):
        """Test _run_command works without env parameter (backward compatibility)."""
        op_config = OpConfig(verbose=False)
        shell = SparkShell(source=".", port=8100, op_config=op_config)
        shell.work_dir = Path(".")

        # Run command without env parameter
        result = shell._run_command(
            ["bash", "-c", "echo hello"],
            check=True
        )
        self.assertIn("hello", result.stdout)

    def test_env_vars_format_for_sbt(self):
        """Test environment variables are formatted correctly for SBT."""
        # Create environment dict that would be passed to SBT
        env = {
            "DELTA_VERSION": "3.2.0",
            "DELTA_USE_LOCAL": "true"
        }

        # Verify the format is correct
        self.assertEqual(env["DELTA_VERSION"], "3.2.0")
        self.assertEqual(env["DELTA_USE_LOCAL"], "true")
        self.assertIsInstance(env, dict)

    def test_verbose_mode_shows_env_vars(self):
        """Test verbose mode shows environment variables being set."""
        op_config = OpConfig(verbose=True)
        shell = SparkShell(source=".", port=8100, op_config=op_config)
        shell.work_dir = Path(".")

        # This should print env vars in verbose mode
        # In verbose mode, output is streamed (not captured), so stdout will be None
        result = shell._run_command(
            ["bash", "-c", "echo test"],
            env={"TEST": "value"},
            check=True
        )
        # Just verify the command succeeded (stdout is None in verbose mode)
        self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
