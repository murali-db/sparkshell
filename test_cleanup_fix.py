#!/usr/bin/env python3
"""Test to verify cleanup doesn't delete the cache directory."""

from spark_shell import SparkShell, OpConfig
from pathlib import Path

def main():
    print("="*70)
    print("Testing Cleanup Fix - Cache should NOT be deleted")
    print("="*70)

    # Test 1: Build and cache (cleanup_on_exit=False to keep cache)
    print("\n[TEST 1] First build - create cache")
    print("-"*70)

    op_config1 = OpConfig(verbose=False, cleanup_on_exit=False, auto_start=False)
    shell1 = SparkShell(source=".", port=9011, op_config=op_config1)

    try:
        shell1.setup()
        shell1.build()
        cache_dir = shell1._get_cache_dir()
        jar_path = cache_dir / "target" / "scala-2.13" / "sparkshell.jar"

        print(f"Cache directory: {cache_dir}")
        print(f"JAR exists: {jar_path.exists()}")
    finally:
        shell1.cleanup()

    # Verify cache still exists after cleanup
    print(f"\nAfter cleanup - Cache still exists: {cache_dir.exists()}")
    print(f"After cleanup - JAR still exists: {jar_path.exists()}")

    if not cache_dir.exists() or not jar_path.exists():
        print("✗ FAILED: Cache was deleted!")
        return 1

    # Test 2: Use cached build with cleanup_on_exit=True
    print("\n[TEST 2] Second build - use cache with cleanup_on_exit=True")
    print("-"*70)

    op_config2 = OpConfig(verbose=False, cleanup_on_exit=True, auto_start=False)
    shell2 = SparkShell(source=".", port=9012, op_config=op_config2)

    try:
        shell2.setup()
        shell2.build()

        # Verify work_dir is the cache directory
        print(f"work_dir: {shell2.work_dir}")
        print(f"cache_dir: {cache_dir}")
        print(f"work_dir == cache_dir: {shell2.work_dir == cache_dir}")

        if shell2.work_dir != cache_dir:
            print("✗ FAILED: work_dir is not cache_dir!")
            return 1

    finally:
        # This should NOT delete the cache
        shell2.cleanup()

    # Verify cache STILL exists after cleanup
    print(f"\nAfter cleanup (with cleanup_on_exit=True):")
    print(f"  Cache exists: {cache_dir.exists()}")
    print(f"  JAR exists: {jar_path.exists()}")

    if not cache_dir.exists() or not jar_path.exists():
        print("✗ FAILED: Cache was deleted when it shouldn't have been!")
        return 1

    print("\n" + "="*70)
    print("✓ ALL TESTS PASSED")
    print("✓ Cache is preserved correctly")
    print("="*70)

    # Clean up the cache manually at the end
    import shutil
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        print(f"Manual cleanup: Removed {cache_dir}")

    return 0

if __name__ == "__main__":
    exit(main())
