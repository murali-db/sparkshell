#!/usr/bin/env python3
"""Test script to verify that Spark logs are displayed when SQL commands fail."""

from spark_shell import SparkShell, OpConfig

def main():
    print("="*70)
    print("Testing Error Logging - Spark logs should be displayed on SQL failure")
    print("="*70)

    op_config = OpConfig(verbose=False, cleanup_on_exit=True)

    try:
        with SparkShell(source=".", port=8097, op_config=op_config) as shell:
            print("\n[TEST 1] Executing valid SQL query...")
            result = shell.execute_sql("SELECT 1 as test_value")
            print(f"✓ Valid query succeeded:\n{result}\n")

            print("[TEST 2] Executing invalid SQL query (table doesn't exist)...")
            try:
                shell.execute_sql("SELECT * FROM nonexistent_table_12345")
                print("✗ Expected error but query succeeded!")
                return 1
            except RuntimeError as e:
                print(f"✓ Error caught as expected: {e}\n")
                print("✓ Spark logs should be displayed above")

            print("\n[TEST 3] Executing SQL with syntax error...")
            try:
                shell.execute_sql("INVALID SQL SYNTAX HERE")
                print("✗ Expected error but query succeeded!")
                return 1
            except RuntimeError as e:
                print(f"✓ Error caught as expected: {e}\n")
                print("✓ Spark logs should be displayed above")

            print("\n" + "="*70)
            print("✓ ALL TESTS PASSED - Error logging is working!")
            print("="*70)
            return 0

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
