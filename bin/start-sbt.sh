#!/bin/bash

# SparkShell Server Start Script

# Get the project root directory (parent of bin/)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

PID_FILE="$PROJECT_DIR/sparkshell.pid"
LOG_FILE="$PROJECT_DIR/sparkshell.log"
PORT="${1:-8080}"

# Check if server is already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "SparkShell server is already running with PID $PID"
        exit 1
    else
        echo "Removing stale PID file..."
        rm -f "$PID_FILE"
    fi
fi

echo "Starting SparkShell server on port $PORT..."
echo "Logs will be written to: $LOG_FILE"

# Start the server in the background
nohup ./build/sbt "run $PORT" > "$LOG_FILE" 2>&1 &
SERVER_PID=$!

# Save the PID
echo $SERVER_PID > "$PID_FILE"

# Wait a few seconds to check if it started successfully
sleep 5

if ps -p $SERVER_PID > /dev/null 2>&1; then
    echo "SparkShell server started successfully with PID $SERVER_PID"
    echo "Use 'bin/stop.sh' to stop the server"
    echo "Use 'tail -f $LOG_FILE' to view logs"
else
    echo "Failed to start SparkShell server. Check $LOG_FILE for errors"
    rm -f "$PID_FILE"
    exit 1
fi
