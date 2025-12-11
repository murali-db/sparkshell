#!/usr/bin/env python3
"""
Test FGAC (Fine-Grained Access Control) with LOCAL pre-built Delta.

Usage:
    export UC_TOKEN="your-token-here"
    python3.10 test_fgac.py

Prerequisites:
    1. Build Delta locally: cd ../delta && build/sbt "spark/publishLocal" "iceberg/publishLocal"
    2. Build SparkShell: DELTA_VERSION=3.4.0-SNAPSHOT DELTA_USE_LOCAL=true build/sbt assembly

Output:
    - Console: Real-time server output
    - test_fgac_server.log: Complete server log file
"""
import os
import sys
import subprocess
import time
import requests
from pathlib import Path

# Log file path (next to this script)
SCRIPT_DIR = Path(__file__).parent
LOG_FILE = SCRIPT_DIR / "test_fgac_server.log"

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
    print(f"Log file: {LOG_FILE}")

    # Open log file for writing
    log_file = open(LOG_FILE, 'w')
    log_file.write(f"=== SparkShell FGAC Test Log ===\n")
    log_file.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    log_file.write(f"JAR: {JAR_PATH}\n")
    log_file.write(f"Catalog: {CATALOG}\n")
    log_file.write(f"Schema: {SCHEMA}\n")
    log_file.write(f"Command: {' '.join(cmd)}\n")
    log_file.write("=" * 60 + "\n\n")
    log_file.flush()

    def log_and_print(line, prefix="[SERVER]"):
        """Write to both console and log file."""
        log_file.write(line)
        log_file.flush()
        print(f"{prefix} {line.rstrip()}")

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
                    log_and_print(line)
                else:
                    break
        except:
            pass

        try:
            resp = requests.get("http://localhost:8765/health", timeout=2)
            if resp.status_code == 200:
                print("Server is ready!")
                log_file.write("\n=== Server Ready ===\n\n")
                log_file.flush()
                break
        except:
            pass
        # Check if process died
        if proc.poll() is not None:
            print("Server process died!")
            log_file.write("\n=== SERVER DIED ===\n")
            # Read remaining output
            remaining = proc.stdout.read()
            if remaining:
                log_and_print(remaining)
            log_file.close()
            sys.exit(1)
    else:
        print("Timeout waiting for server")
        log_file.write("\n=== TIMEOUT ===\n")
        log_file.close()
        proc.terminate()
        sys.exit(1)

    # Execute query
    try:
        print(f"\n=== Querying {CATALOG}.{SCHEMA}.test_table_1099 ===")
        log_file.write(f"\n=== Query Execution ===\n")
        # Use fully-qualified table name: catalog.schema.table
        sql = f"SELECT * FROM {CATALOG}.{SCHEMA}.test_table_1099 LIMIT 5"
        print(f"SQL: {sql}")
        log_file.write(f"SQL: {sql}\n")
        log_file.flush()

        resp = requests.post(
            "http://localhost:8765/sql",
            json={"sql": sql},
            timeout=120
        )
        print(f"Response status: {resp.status_code}")
        print(f"Result:\n{resp.text}")
        log_file.write(f"Response status: {resp.status_code}\n")
        log_file.write(f"Result:\n{resp.text}\n")
        log_file.flush()

        # Give server time to write debug output
        time.sleep(2)

        # Read any debug output from server
        print("\n=== Server debug output after query ===")
        log_file.write("\n=== Server Debug Output After Query ===\n")
        try:
            while True:
                line = proc.stdout.readline()
                if line:
                    log_and_print(line)
                else:
                    break
        except:
            pass
    finally:
        print("\nShutting down server...")
        log_file.write("\n=== Shutdown ===\n")
        log_file.close()
        proc.terminate()
        proc.wait(timeout=10)
        print(f"\nFull log saved to: {LOG_FILE}")

if __name__ == "__main__":
    main()
