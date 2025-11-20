#!/bin/bash

# SparkShell Server Start Script (using assembly JAR)

# Get the project root directory (parent of bin/)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

PID_FILE="$PROJECT_DIR/sparkshell.pid"
LOG_FILE="$PROJECT_DIR/sparkshell.log"
JAR_FILE="$PROJECT_DIR/target/scala-2.13/sparkshell.jar"
PORT="${1:-8080}"

# Check if JAR exists
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: Assembly JAR not found at $JAR_FILE"
    echo "Please build it first with: build/sbt assembly"
    exit 1
fi

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

# Start the server in the background with the assembly JAR
nohup java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -Djdk.reflect.useDirectMethodHandle=false \
  -jar "$JAR_FILE" "$PORT" > "$LOG_FILE" 2>&1 &

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
