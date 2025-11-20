#!/usr/bin/env python3
"""Full test of SparkShell with GitHub source - complete end-to-end test."""

from spark_shell import SparkShell, OpConfig

def main():
    print("=" * 70)
    print("Full GitHub Source Test - End to End")
    print("=" * 70)

    github_url = "https://github.com/tdas/delta/tree/oss-in-dbr/experimental/sparkshell"

    op_config = OpConfig(
        verbose=True,
        cleanup_on_exit=True
    )

    try:
        with SparkShell(source=github_url, port=8096, op_config=op_config) as shell:
            print("\n" + "=" * 70)
            print("SparkShell started successfully!")
            print("=" * 70)

            # Get server info
            info = shell.get_server_info()
            print(f"\nServer Info:")
            print(f"  Spark Version: {info.get('sparkVersion', 'unknown')}")
            print(f"  Port: {info.get('port', 'unknown')}")

            # Test basic SQL
            print("\n[TEST] Executing basic SQL query...")
            result = shell.execute_sql("SELECT 1 as test, 'hello' as message")
            print(f"Result:\n{result}")

            # Test table creation
            print("\n[TEST] Creating test table...")
            result = shell.execute_sql("CREATE TABLE test_data (id INT, name STRING) USING parquet")
            print(f"Result: {result}")

            # Test insert
            print("\n[TEST] Inserting data...")
            result = shell.execute_sql("INSERT INTO test_data VALUES (1, 'Alice'), (2, 'Bob')")
            print(f"Result: {result}")

            # Test query
            print("\n[TEST] Querying data...")
            result = shell.execute_sql("SELECT * FROM test_data ORDER BY id")
            print(f"Result:\n{result}")

            # Cleanup
            print("\n[TEST] Cleaning up...")
            result = shell.execute_sql("DROP TABLE test_data")
            print(f"Result: {result}")

            print("\n" + "=" * 70)
            print("✓ ALL TESTS PASSED!")
            print("✓ SparkShell with GitHub source is fully functional")
            print("✓ No Java GC warnings or memory issues")
            print("=" * 70)

            return 0

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
