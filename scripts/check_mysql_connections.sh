#!/bin/bash
# Script to check MySQL connections
# Usage: ./scripts/check_mysql_connections.sh [database_name] [user]
# Note: You'll be prompted for password, or use .my.cnf for passwordless access

DB_NAME=${1:-""}
DB_USER=${2:-"root"}

echo "=========================================="
echo "MySQL Connection Monitor"
if [ -n "$DB_NAME" ]; then
    echo "Database: $DB_NAME"
fi
echo "User: $DB_USER"
echo "=========================================="
echo ""

# Check if mysql is available
if ! command -v mysql &> /dev/null; then
    echo "Error: mysql client not found. Please install MySQL client."
    exit 1
fi

# Function to run query
run_query() {
    if [ -n "$DB_NAME" ]; then
        mysql -u "$DB_USER" -p "$DB_NAME" -e "$1" 2>/dev/null
    else
        mysql -u "$DB_USER" -p -e "$1" 2>/dev/null
    fi
}

# Total connections
echo "Total connections:"
run_query "SELECT COUNT(*) as total_connections FROM information_schema.PROCESSLIST;"

# Active vs Idle
echo ""
echo "Connection Status:"
run_query "SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN COMMAND != 'Sleep' THEN 1 ELSE 0 END) as active,
    SUM(CASE WHEN COMMAND = 'Sleep' THEN 1 ELSE 0 END) as idle
FROM information_schema.PROCESSLIST;"

# Connections by user
echo ""
echo "Connections by User:"
run_query "SELECT USER, COUNT(*) as connections FROM information_schema.PROCESSLIST GROUP BY USER;"

# Max connections
echo ""
echo "Max Connections Limit:"
run_query "SHOW VARIABLES LIKE 'max_connections';"

# Current thread status
echo ""
echo "Thread Status:"
run_query "SHOW STATUS WHERE Variable_name IN ('Threads_connected', 'Threads_running', 'Max_used_connections');"

# Detailed connection info
if [ -n "$DB_NAME" ]; then
    echo ""
    echo "=========================================="
    echo "Connection Details for database '$DB_NAME':"
    echo "=========================================="
    run_query "SELECT ID, USER, HOST, COMMAND, TIME, STATE, LEFT(INFO, 60) as QUERY_PREVIEW FROM information_schema.PROCESSLIST WHERE DB = '$DB_NAME' ORDER BY TIME DESC;"
else
    echo ""
    echo "=========================================="
    echo "All Connection Details:"
    echo "=========================================="
    run_query "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 60) as QUERY_PREVIEW FROM information_schema.PROCESSLIST ORDER BY TIME DESC;"
fi

echo ""
echo "=========================================="
echo "Real-time monitoring (Ctrl+C to stop):"
echo "=========================================="
if [ -n "$DB_NAME" ]; then
    echo "Run: watch -n 1 \"mysql -u $DB_USER -p $DB_NAME -e \\\"SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB = '$DB_NAME';\\\" -N\""
else
    echo "Run: watch -n 1 \"mysql -u $DB_USER -p -e \\\"SELECT COUNT(*) FROM information_schema.PROCESSLIST;\\\" -N\""
fi

