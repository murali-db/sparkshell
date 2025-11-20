#!/bin/bash

# SparkShell Server Stop Script

# Get the project root directory (parent of bin/)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

PID_FILE="$PROJECT_DIR/sparkshell.pid"

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo "SparkShell server is not running (PID file not found)"
    exit 0
fi

PID=$(cat "$PID_FILE")

# Check if process is running
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "SparkShell server is not running (process $PID not found)"
    rm -f "$PID_FILE"
    exit 0
fi

echo "Stopping SparkShell server (PID: $PID)..."

# Send SIGTERM to gracefully stop the server
kill "$PID"

# Wait for the process to stop (max 30 seconds)
for i in {1..30}; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "SparkShell server stopped successfully"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# If still running, force kill
echo "Server did not stop gracefully, forcing shutdown..."
kill -9 "$PID"
sleep 2

if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "SparkShell server stopped forcefully"
    rm -f "$PID_FILE"
    exit 0
else
    echo "Failed to stop SparkShell server"
    exit 1
fi
