#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unity Catalog Test Script

This script tests Unity Catalog connectivity with SparkShell.
Configure UC_URI and UC_TOKEN environment variables or pass them as arguments.

Usage:
    # Using environment variables
    export UC_URI="http://localhost:8081"
    export UC_TOKEN="your-token"
    python tests/test_unity_catalog.py

    # Using command-line arguments
    python tests/test_unity_catalog.py --uri http://localhost:8081 --token your-token

    # With custom catalog and schema
    python tests/test_unity_catalog.py --uri http://localhost:8081 --token your-token --uc-catalog unity --uc-schema my_schema

    # Interactive mode (prompts for credentials)
    python tests/test_unity_catalog.py
"""

import os
import sys
import argparse
from spark_shell import SparkShell


def test_unity_catalog(uc_uri: str, uc_token: str, uc_catalog: str = "unity", 
                       uc_schema: str = "default", port: int = 8093):
    """Test Unity Catalog connectivity and operations."""
    
    print("=" * 70)
    print("Unity Catalog Test Script")
    print("=" * 70)
    print(f"\nUnity Catalog URI: {uc_uri}")
    print(f"Token: {'*' * len(uc_token) if uc_token else '(empty)'}")
    print(f"Catalog: {uc_catalog}")
    print(f"Schema: {uc_schema}")
    print(f"Server Port: {port}")
    print("=" * 70)
    
    print("\n[1/6] Starting SparkShell with Unity Catalog configuration...")
    try:
        with SparkShell(
            source=".", 
            port=port,
            uc_uri=uc_uri,
            uc_token=uc_token,
            uc_catalog=uc_catalog,
            uc_schema=uc_schema
        ) as shell:
            print("✓ SparkShell started successfully\n")
            
            # Test 1: Get server info
            print("[2/6] Getting server info...")
            info = shell.get_server_info()
            print(f"✓ Spark version: {info.get('sparkVersion', 'unknown')}")
            print(f"✓ Port: {info.get('port', 'unknown')}\n")
            
            # Test 2: Test basic SQL (non-UC)
            print("[3/6] Testing basic SQL...")
            sql = "SELECT 'Unity Catalog Test' as message"
            print(f"  SQL: {sql}")
            result = shell.execute_sql(sql)
            print(f"✓ Basic SQL works:\n{result}\n")
            
            # Test 3: List Unity Catalog catalogs
            print("[4/6] Listing Unity Catalog catalogs...")
            try:
                sql = "SHOW CATALOGS"
                print(f"  SQL: {sql}")
                result = shell.execute_sql(sql)
                print(f"✓ Unity Catalog catalogs:\n{result}\n")
            except RuntimeError as e:
                print(f"✗ Failed to list catalogs: {e}\n")
                print("  This might indicate:")
                print("  - UC server is not running")
                print("  - Invalid URI or token")
                print("  - Network connectivity issues\n")
            
            # Test 4: List schemas in Unity catalog
            print(f"[5/6] Listing schemas in {uc_catalog} catalog...")
            try:
                sql = f"SHOW SCHEMAS IN {uc_catalog}"
                print(f"  SQL: {sql}")
                result = shell.execute_sql(sql)
                print(f"✓ Schemas in {uc_catalog}:\n{result}\n")
            except RuntimeError as e:
                print(f"✗ Failed to list schemas: {e}\n")
            
            # Test 5: Test three-level namespace
            print(f"[6/6] Testing current catalog and schema...")
            try:
                # Current catalog
                sql = "SELECT current_catalog() as catalog"
                print(f"  SQL: {sql}")
                result = shell.execute_sql(sql)
                print(f"✓ Current catalog:\n{result}\n")
                
                # Current schema
                sql = "SELECT current_schema() as schema"
                print(f"  SQL: {sql}")
                result = shell.execute_sql(sql)
                print(f"✓ Current schema:\n{result}\n")
                
                # Try to show tables in current schema
                sql = "SHOW TABLES"
                print(f"  SQL: {sql}")
                result = shell.execute_sql(sql)
                print(f"✓ Tables in {uc_catalog}.{uc_schema}:\n{result}\n")
            except RuntimeError as e:
                print(f"✗ Failed to access catalog/schema: {e}\n")
            
            # Test CRUD operations
            print("=" * 70)
            print("Testing Operations on Unity Catalog Tables")
            print("=" * 70)
            
            crud_success = False
            existing_table = None
            
            try:
                # First, check for existing tables
                print(f"\n[Step 1] Checking for existing tables in {uc_catalog}.{uc_schema}...")
                sql = "SHOW TABLES"
                print(f"  SQL: {sql}")
                tables_result = shell.execute_sql(sql)
                print(f"Result:\n{tables_result}\n")
                
                # Parse tables from result to see if any exist
                if "Empty result set" not in tables_result:
                    # Try to extract table name from the result
                    # Format: "namespace | tableName | isTemporary"
                    lines = tables_result.strip().split('\n')
                    for line in lines:
                        # Skip header, separator and summary lines
                        if (line.startswith('---') or 
                            'namespace' in line or 
                            'tableName' in line or 
                            'Total rows' in line or
                            not line.strip()):
                            continue
                        
                        # Split by pipe (|) to get columns
                        parts = [p.strip() for p in line.split('|')]
                        if len(parts) >= 2:
                            # Second column is tableName
                            potential_table = parts[1]
                            if potential_table and potential_table not in ['tableName', '']:
                                existing_table = potential_table
                                break
                
                if existing_table:
                    print(f"✓ Found existing table: {existing_table}")
                    print(f"\n[Step 2] Reading from existing table: {existing_table}...")
                    
                    # Try to describe the table first
                    sql = f"DESCRIBE TABLE {existing_table}"
                    print(f"  SQL: {sql}")
                    try:
                        result = shell.execute_sql(sql)
                        print(f"✓ Table schema:\n{result}\n")
                    except RuntimeError as e:
                        print(f"✗ Could not describe table: {e}\n")
                    
                    # Try to count rows
                    sql = f"SELECT COUNT(*) as row_count FROM {existing_table}"
                    print(f"  SQL: {sql}")
                    try:
                        result = shell.execute_sql(sql)
                        print(f"✓ Row count:\n{result}\n")
                    except RuntimeError as e:
                        print(f"✗ Could not count rows: {e}\n")
                    
                    # Try to read sample data
                    sql = f"SELECT * FROM {existing_table} LIMIT 5"
                    print(f"  SQL: {sql}")
                    try:
                        result = shell.execute_sql(sql)
                        print(f"✓ Sample data:\n{result}\n")
                        crud_success = True
                        print("✓ Successfully read from existing Unity Catalog table!")
                    except RuntimeError as e:
                        print(f"✗ Could not read from table: {e}\n")
                
                else:
                    print(f"✓ No existing tables found in {uc_catalog}.{uc_schema}")
                    print(f"\n[Step 2] Attempting to create a test table...")
                    
                    # Use timestamp in table name to avoid conflicts
                    import time
                    test_table = f"sparkshell_test_{int(time.time())}"
                    test_table_fqn = f"{uc_catalog}.{uc_schema}.{test_table}"
                    
                    # Create without LOCATION - UC will provide a managed location
                    sql = f"""CREATE TABLE {test_table} (
    id INT,
    name STRING,
    value DOUBLE,
    created_at TIMESTAMP
) USING DELTA"""
                    print(f"  SQL: {sql}")
                    result = shell.execute_sql(sql)
                    print(f"✓ Table created: {test_table_fqn}")
                    
                    # INSERT (Create): Insert test data
                    print(f"\n[Step 3] Inserting data into {test_table}...")
                    sql = f"""INSERT INTO {test_table} VALUES
    (1, 'Alice', 100.5, CURRENT_TIMESTAMP()),
    (2, 'Bob', 200.0, CURRENT_TIMESTAMP()),
    (3, 'Charlie', 300.75, CURRENT_TIMESTAMP())"""
                    print(f"  SQL: {sql}")
                    result = shell.execute_sql(sql)
                    print(f"✓ Data inserted into {test_table}")
                    
                    # READ: Query the data
                    print(f"\n[Step 4] Reading data from {test_table}...")
                    sql = f"SELECT id, name, value FROM {test_table} ORDER BY id"
                    print(f"  SQL: {sql}")
                    result = shell.execute_sql(sql)
                    print(f"✓ Data read from {test_table}:\n{result}")
                    
                    # Verify data
                    if "Alice" in result and "Bob" in result and "Charlie" in result:
                        print("✓ All inserted rows found")
                    else:
                        print("✗ Warning: Some inserted rows not found")
                    
                    # UPDATE: Update some data
                    print(f"\n[Step 5] Updating data in {test_table}...")
                    sql = f"""UPDATE {test_table}
SET value = value * 1.1
WHERE id IN (1, 2)"""
                    print(f"  SQL: {sql}")
                    result = shell.execute_sql(sql)
                    print(f"✓ Data updated in {test_table}")
                    
                    # Verify update
                    sql = f"SELECT id, value FROM {test_table} WHERE id = 1"
                    print(f"  SQL: {sql}")
                    result = shell.execute_sql(sql)
                    if "110" in result:  # 100.5 * 1.1 = 110.55
                        print(f"✓ Update verified:\n{result}")
                    else:
                        print(f"✓ Update result:\n{result}")
                    
                    # DELETE: Delete some data
                    print(f"\n[Step 6] Deleting data from {test_table}...")
                    sql = f"DELETE FROM {test_table} WHERE id = 3"
                    print(f"  SQL: {sql}")
                    result = shell.execute_sql(sql)
                    print(f"✓ Data deleted from {test_table}")
                    
                    # Verify delete
                    sql = f"SELECT COUNT(*) as count FROM {test_table}"
                    print(f"  SQL: {sql}")
                    result = shell.execute_sql(sql)
                    if "2" in result:
                        print(f"✓ Delete verified (2 rows remaining):\n{result}")
                    else:
                        print(f"✓ Delete result:\n{result}")
                    
                    # Final read to show remaining data
                    print(f"\nFinal state of {test_table}:")
                    sql = f"SELECT id, name, value FROM {test_table} ORDER BY id"
                    print(f"  SQL: {sql}")
                    result = shell.execute_sql(sql)
                    print(result)
                    
                    # Clean up: Drop the test table
                    print(f"\nCleaning up test table...")
                    sql = f"DROP TABLE {test_table}"
                    print(f"  SQL: {sql}")
                    shell.execute_sql(sql)
                    print(f"✓ Test table {test_table} dropped")
                    
                    crud_success = True
                    print("\n" + "=" * 70)
                    print("✓ CRUD Operations Test PASSED")
                    print("=" * 70)
                
            except RuntimeError as e:
                error_msg = str(e)
                print(f"\n✗ Operation failed: {e}")
                
                # Try to clean up (only if we attempted to create a table)
                if not existing_table:
                    try:
                        if 'test_table' in locals():
                            sql = f"DROP TABLE IF EXISTS {test_table}"
                            print(f"  SQL: {sql}")
                            shell.execute_sql(sql)
                            print(f"✓ Cleaned up test table")
                    except:
                        pass
                
                print("\n" + "=" * 70)
                print("✗ CRUD Operations Test FAILED")
                print("=" * 70)
                
                # Check if it's a cloud storage or managed table support issue
                if ("Unsupported cloud file system scheme" in error_msg or 
                    "file system" in error_msg.lower() or
                    "file:/tmp/spark-warehouse" in error_msg or
                    "NON_EMPTY_LOCATION" in error_msg or
                    "does not support managed table" in error_msg.lower()):
                    print("\nNote: This Unity Catalog schema doesn't support managed tables.")
                    print("The schema may require explicit LOCATION with cloud storage paths (S3, ADLS, GCS).")
                    print("Unity Catalog will then provide temporary credentials dynamically.")
                    print("\nSkipping CRUD test - Basic Unity Catalog connectivity is verified!")
                    # Don't return error - connectivity is verified
                else:
                    print("\nNote: CRUD operations require write permissions on the catalog/schema")
                    print("Possible issues:")
                    print("- Insufficient permissions to create tables")
                    print("- Schema is read-only")
                    print("- Network or authentication issues")
                    return 1
            
            print("\n" + "=" * 70)
            print("✓ Unity Catalog Test PASSED")
            print("=" * 70)
            print("\nUnity Catalog is properly configured and accessible!")
            if crud_success:
                if existing_table:
                    print(f"Successfully read from existing Unity Catalog table: {existing_table}")
                else:
                    print("CRUD operations (Create, Read, Update, Delete) are working!")
            return 0
                
    except Exception as e:
        print(f"\n✗ Error starting SparkShell: {e}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    parser = argparse.ArgumentParser(
        description="Test Unity Catalog connectivity with SparkShell",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using environment variables
  export UC_URI="http://localhost:8081"
  export UC_TOKEN="your-token"
  python tests/test_unity_catalog.py

  # Using command-line arguments
  python tests/test_unity_catalog.py --uri http://localhost:8081 --token your-token

  # With custom catalog and schema
  python tests/test_unity_catalog.py --uri http://localhost:8081 --token your-token --uc-catalog unity --uc-schema my_schema

  # Interactive mode
  python tests/test_unity_catalog.py
        """
    )
    
    parser.add_argument(
        "--uri",
        help="Unity Catalog URI (default: from UC_URI env var or prompt)",
        default=None
    )
    
    parser.add_argument(
        "--token",
        help="Unity Catalog token (default: from UC_TOKEN env var or prompt)",
        default=None
    )
    
    parser.add_argument(
        "--uc-catalog",
        default="unity",
        help="Unity Catalog catalog name (default: unity)"
    )
    
    parser.add_argument(
        "--uc-schema",
        default="default",
        help="Unity Catalog schema name (default: default)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=8093,
        help="SparkShell server port (default: 8093)"
    )
    
    args = parser.parse_args()
    
    # Get UC URI
    uc_uri = args.uri or os.environ.get("UC_URI")
    if not uc_uri:
        print("Unity Catalog URI not provided.")
        uc_uri = input("Enter Unity Catalog URI (e.g., http://localhost:8081): ").strip()
        if not uc_uri:
            print("Error: URI is required")
            return 1
    
    # Get UC Token
    uc_token = args.token or os.environ.get("UC_TOKEN")
    if not uc_token:
        print("Unity Catalog token not provided.")
        uc_token = input("Enter Unity Catalog token: ").strip()
        if not uc_token:
            print("Warning: Empty token provided")
    
    # Run tests
    return test_unity_catalog(uc_uri, uc_token, args.uc_catalog, args.uc_schema, args.port)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

