#!/bin/bash

# SparkShell Server Restart Script

# Get the bin directory where this script is located
BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PORT="${1:-8080}"

echo "Restarting SparkShell server..."

# Stop the server
"$BIN_DIR/stop.sh"

# Wait a moment to ensure clean shutdown
sleep 2

# Start the server
"$BIN_DIR/start.sh" "$PORT"
