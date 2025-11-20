#!/bin/bash

# SparkShell Server Status Script

# Get the project root directory (parent of bin/)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

PID_FILE="$PROJECT_DIR/sparkshell.pid"
LOG_FILE="$PROJECT_DIR/sparkshell.log"

echo "=========================================="
echo "SparkShell Server Status"
echo "=========================================="

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo "Status: NOT RUNNING"
    echo "PID file not found at: $PID_FILE"
    exit 0
fi

PID=$(cat "$PID_FILE")

# Check if process is running
if ps -p "$PID" > /dev/null 2>&1; then
    echo "Status: RUNNING"
    echo "PID: $PID"

    # Try to extract port from command line
    PORT=$(ps -p "$PID" -o command= | grep -oE "run [0-9]+" | awk '{print $2}')
    if [ -z "$PORT" ]; then
        PORT="8080 (default)"
    fi
    echo "Port: $PORT"

    # Show process info
    echo ""
    echo "Process Info:"
    ps -p "$PID" -o pid,ppid,user,%cpu,%mem,etime,command

    # Show last few log lines
    if [ -f "$LOG_FILE" ]; then
        echo ""
        echo "Recent logs (last 10 lines):"
        echo "------------------------------------------"
        tail -10 "$LOG_FILE"
    fi
else
    echo "Status: NOT RUNNING"
    echo "PID file exists but process $PID is not running"
    echo "Cleaning up stale PID file..."
    rm -f "$PID_FILE"
fi

echo "=========================================="
