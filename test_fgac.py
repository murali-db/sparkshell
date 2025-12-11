#!/usr/bin/env python3
"""
Test FGAC (Fine-Grained Access Control) with LOCAL pre-built Delta.

Usage:
    export UC_TOKEN="your-token-here"
    python3.10 test_fgac.py

Prerequisites:
    1. Build Delta locally: cd ../delta && build/sbt "spark/publishLocal" "iceberg/publishLocal"
    2. Build SparkShell: DELTA_VERSION=3.4.0-SNAPSHOT DELTA_USE_LOCAL=true build/sbt assembly
"""
import os
import sys
import subprocess
import time
import requests

# Configuration
JAR_PATH = "/home/murali.ramanujam/td-sparkshell/sparkshell/target/scala-2.13/sparkshell.jar"
UC_URI = "https://e2-dogfood.staging.cloud.databricks.com/"
CATALOG = "migration_bugbash"
SCHEMA = "philipzhu2_rlscm"

def main():
    token = os.environ.get("UC_TOKEN")
    if not token:
        print("ERROR: Set UC_TOKEN environment variable")
        print("  export UC_TOKEN='your-databricks-pat'")
        sys.exit(1)

    if not os.path.exists(JAR_PATH):
        print(f"ERROR: JAR not found at {JAR_PATH}")
        print("Build it first with: DELTA_VERSION=3.4.0-SNAPSHOT DELTA_USE_LOCAL=true build/sbt assembly")
        sys.exit(1)

    print(f"Using pre-built JAR: {JAR_PATH}")
    print(f"  Catalog: {CATALOG}")
    print(f"  Schema: {SCHEMA}")

    # Start the server directly using java
    # Format: java -cp JAR [java-opts] CLASS [port] [spark.conf=value ...]
    java_17 = "/usr/lib/jvm/java-17-openjdk-amd64/bin/java"
    cmd = [
        java_17,
        "-Xmx4g",
        "-cp", JAR_PATH,
        "com.sparkshell.SparkShellServer",
        "8765",  # port (positional)
        # Spark configs passed as key=value args
        f"spark.sql.catalog.spark_catalog=io.unitycatalog.spark.UCSingleCatalog",
        f"spark.sql.catalog.spark_catalog.uri={UC_URI}",
        f"spark.sql.catalog.spark_catalog.token={token}",
        f"spark.sql.defaultCatalog={CATALOG}",
        f"spark.sql.catalog.{CATALOG}=io.unitycatalog.spark.UCSingleCatalog",
        f"spark.sql.catalog.{CATALOG}.uri={UC_URI}",
        f"spark.sql.catalog.{CATALOG}.token={token}",
    ]

    print("\nStarting SparkShell server...")
    print(f"Command: java -cp {JAR_PATH} ... com.sparkshell.SparkShellServer --port 8765")

    # Start server, showing all output
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # Wait for server to start (look for "Server started" or health check)
    print("Waiting for server to start...")
    import select
    import fcntl

    # Make stdout non-blocking so we can read and check health
    fd = proc.stdout.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    server_output = []
    for i in range(60):  # Wait up to 60 seconds
        time.sleep(2)
        # Read any available output
        try:
            while True:
                line = proc.stdout.readline()
                if line:
                    server_output.append(line)
                    print(f"[SERVER] {line.rstrip()}")
                else:
                    break
        except:
            pass

        try:
            resp = requests.get("http://localhost:8765/health", timeout=2)
            if resp.status_code == 200:
                print("Server is ready!")
                break
        except:
            pass
        # Check if process died
        if proc.poll() is not None:
            print("Server process died!")
            # Read remaining output
            remaining = proc.stdout.read()
            if remaining:
                print(remaining)
            sys.exit(1)
    else:
        print("Timeout waiting for server")
        proc.terminate()
        sys.exit(1)

    # Execute query
    try:
        print(f"\n=== Querying {CATALOG}.{SCHEMA}.test_table_1099 ===")
        # Use fully-qualified table name: catalog.schema.table
        sql = f"SELECT * FROM {CATALOG}.{SCHEMA}.test_table_1099 LIMIT 5"
        print(f"SQL: {sql}")
        resp = requests.post(
            "http://localhost:8765/sql",
            json={"sql": sql},
            timeout=120
        )
        print(f"Response status: {resp.status_code}")
        print(f"Result:\n{resp.text}")

        # Give server time to write debug output
        time.sleep(2)

        # Read any debug output from server
        print("\n=== Server debug output after query ===")
        try:
            while True:
                line = proc.stdout.readline()
                if line:
                    print(f"[SERVER] {line.rstrip()}")
                else:
                    break
        except:
            pass
    finally:
        print("\nShutting down server...")
        proc.terminate()
        proc.wait(timeout=10)

if __name__ == "__main__":
    main()
