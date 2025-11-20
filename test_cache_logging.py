#!/usr/bin/env python3
"""Test script to verify detailed cache logging."""

from spark_shell import SparkShell, OpConfig

def main():
    print("="*70)
    print("Testing Cache Logging")
    print("="*70)

    # Test 1: Fresh build (no cache)
    print("\n[TEST 1] Fresh build - should NOT use cache")
    print("-"*70)

    op_config1 = OpConfig(verbose=False, cleanup_on_exit=False, auto_start=False)
    shell1 = SparkShell(source=".", port=9001, op_config=op_config1)

    try:
        shell1.setup()
        shell1.build()
        print(f"✓ First build complete")
    finally:
        shell1.cleanup()

    # Test 2: Cached build (should use cache)
    print("\n[TEST 2] Second build with same source - SHOULD use cache")
    print("-"*70)

    op_config2 = OpConfig(verbose=False, cleanup_on_exit=True, auto_start=False)
    shell2 = SparkShell(source=".", port=9002, op_config=op_config2)

    try:
        shell2.setup()
        shell2.build()
        print(f"✓ Second build complete (should have used cache)")
    finally:
        shell2.cleanup()

    # Test 3: Force refresh (should NOT use cache)
    print("\n[TEST 3] Force refresh - should NOT use cache")
    print("-"*70)

    op_config3 = OpConfig(verbose=False, cleanup_on_exit=True, auto_start=False)
    shell3 = SparkShell(source=".", port=9003, op_config=op_config3)

    try:
        shell3.setup(force_refresh=True)
        shell3.build(force_refresh=True)
        print(f"✓ Force refresh build complete")
    finally:
        shell3.cleanup()

    print("\n" + "="*70)
    print("✓ ALL CACHE LOGGING TESTS COMPLETE")
    print("="*70)
    return 0

if __name__ == "__main__":
    exit(main())
