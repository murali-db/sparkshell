#!/usr/bin/env python3
"""Test to verify SBT build output is shown even with verbose=False."""

from spark_shell import SparkShell, OpConfig
import shutil
from pathlib import Path

def main():
    print("="*70)
    print("Testing Build Output with verbose=False")
    print("="*70)

    # Clear cache to force a fresh build
    cache_base = Path.home() / ".sparkshell_cache"
    source_path = Path(".").resolve()
    import hashlib
    source_hash = hashlib.sha256(str(source_path).encode()).hexdigest()[:16]
    cache_dir = cache_base / source_hash

    if cache_dir.exists():
        print(f"\nClearing cache: {cache_dir}")
        shutil.rmtree(cache_dir)

    print("\n[TEST] Building with verbose=False - SBT output should be visible")
    print("-"*70)

    op_config = OpConfig(verbose=False, cleanup_on_exit=True, auto_start=False)
    shell = SparkShell(source=".", port=9031, op_config=op_config)

    try:
        shell.setup()
        print("\n>>> About to run build - you should see SBT compilation output below:")
        shell.build()
        print("\n✓ Build completed - SBT output should have been displayed above")
    finally:
        shell.cleanup()

    print("\n" + "="*70)
    print("✓ Test complete")
    print("="*70)

    return 0

if __name__ == "__main__":
    exit(main())
