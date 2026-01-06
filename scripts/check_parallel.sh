#!/bin/bash
# Script to check if petaly is running in parallel mode
# Usage: ./scripts/check_parallel.sh [PID]

if [ -z "$1" ]; then
    # Find petaly Python process
    PID=$(ps aux | grep -E "python.*petaly|petaly" | grep -v grep | awk '{print $2}' | head -1)
    if [ -z "$PID" ]; then
        echo "No petaly process found. Please provide PID as argument: $0 <PID>"
        exit 1
    fi
    echo "Found petaly process: PID=$PID"
else
    PID=$1
fi

echo "=========================================="
echo "Monitoring process PID: $PID"
echo "=========================================="
echo ""

# Check thread count
THREAD_COUNT=$(ps -M $PID 2>/dev/null | wc -l)
if [ $? -eq 0 ]; then
    echo "Thread count: $((THREAD_COUNT - 1))"  # Subtract header line
    echo ""
    echo "Thread details:"
    ps -M $PID
else
    echo "Process $PID not found or access denied"
    exit 1
fi

echo ""
echo "=========================================="
echo "Real-time monitoring (Ctrl+C to stop):"
echo "=========================================="
echo "Watch thread count: watch -n 1 'ps -M $PID | wc -l'"
echo "Watch process tree: watch -n 1 'pstree -p $PID'"
echo ""

# Show process tree
echo "Process tree:"
pstree -p $PID 2>/dev/null || echo "pstree not available"

