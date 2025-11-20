#!/usr/bin/env python3
"""
SparkShell Example - Demonstrates usage of SparkShell class with configuration classes.

Usage:
    python spark_shell_example.py <source> [--port PORT] [--no-cleanup] [--sql SQL] [--quiet]

Examples:
    # Run with local directory (verbose by default)
    python spark_shell_example.py . --port 8080

    # Run with custom SQL
    python spark_shell_example.py . --sql "SELECT 1 as id, 'Alice' as name"

    # Run in quiet mode (no verbose output)
    python spark_shell_example.py . --quiet

    # Run from GitHub (if downloading from repo)
    python spark_shell_example.py https://github.com/user/repo/tree/main/path/to/sparkshell
"""

import sys
import argparse
from spark_shell import SparkShell, UCConfig, OpConfig, SparkConfig


def main():
    """Example usage of SparkShell."""
    parser = argparse.ArgumentParser(description="SparkShell - Manage SparkApp server")
    parser.add_argument("source", help="GitHub URL or local directory path")
    parser.add_argument("--port", type=int, default=8080, help="Server port (default: 8080)")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't cleanup temp files")
    parser.add_argument("--sql", help="SQL command to execute")
    parser.add_argument("--quiet", action="store_true", help="Disable verbose output (default: verbose)")

    args = parser.parse_args()

    # Example usage
    try:
        # Create configuration objects
        op_config = OpConfig(
            verbose=not args.quiet,
            cleanup_on_exit=not args.no_cleanup,
            auto_start=True,
            startup_timeout=60,
            build_timeout=300
        )

        spark_config = SparkConfig(
            configs={
                "spark.sql.shuffle.partitions": "10",
                "spark.sql.adaptive.enabled": "true"
            }
        )

        # Note: UCConfig would be configured if we had UC credentials
        # uc_config = UCConfig(uri="http://localhost:8081", token="my-token")

        with SparkShell(
            source=args.source,
            port=args.port,
            op_config=op_config,
            spark_config=spark_config
        ) as shell:
            # Get server info
            info = shell.get_server_info()
            print(f"\n{'='*60}")
            print(f"Server Info: Spark {info.get('sparkVersion', 'unknown')}")
            print(f"Port: {info.get('port', 'unknown')}")
            print(f"{'='*60}\n")

            # Execute SQL if provided
            if args.sql:
                print(f"Executing: {args.sql}")
                result = shell.execute_sql(args.sql)
                print(f"\nResult:\n{result}\n")
            else:
                # Run example queries
                print("Running example queries...\n")

                # Create table
                result = shell.execute_sql(
                    "CREATE TABLE test_users (id INT, name STRING, age INT) USING parquet"
                )
                print(f"1. Create table:\n{result}\n")

                # Insert data
                result = shell.execute_sql(
                    "INSERT INTO test_users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"
                )
                print(f"2. Insert data:\n{result}\n")

                # Query data
                result = shell.execute_sql("SELECT * FROM test_users ORDER BY age")
                print(f"3. Query data:\n{result}\n")

                # Aggregation
                result = shell.execute_sql("SELECT AVG(age) as avg_age FROM test_users")
                print(f"4. Aggregation:\n{result}\n")

                # Drop table
                result = shell.execute_sql("DROP TABLE test_users")
                print(f"5. Drop table:\n{result}\n")

            print("✓ All operations completed successfully!")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
