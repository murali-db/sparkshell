#!/usr/bin/env python3
"""Test to verify clean output when verbose=False."""

from spark_shell import SparkShell, OpConfig

def main():
    print("="*70)
    print("Testing Quiet Output (verbose=False)")
    print("="*70)

    # Test with verbose=False (default for most users)
    print("\n[TEST] Using cached build with verbose=False")
    print("-"*70)

    op_config = OpConfig(verbose=False, cleanup_on_exit=True, auto_start=False)
    shell = SparkShell(source=".", port=9021, op_config=op_config)

    try:
        shell.setup()
        shell.build()
        print(f"\n✓ Build completed")
    finally:
        shell.cleanup()

    print("\n" + "="*70)
    print("✓ Test complete - output should be minimal and clean")
    print("="*70)

    return 0

if __name__ == "__main__":
    exit(main())
