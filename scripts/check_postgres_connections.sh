#!/bin/bash
# Script to check PostgreSQL connections
# Usage: ./scripts/check_postgres_connections.sh [database_name] [user]

DB_NAME=${1:-"postgres"}
DB_USER=${2:-"postgres"}

echo "=========================================="
echo "PostgreSQL Connection Monitor"
echo "Database: $DB_NAME"
echo "User: $DB_USER"
echo "=========================================="
echo ""

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo "Error: psql not found. Please install PostgreSQL client."
    exit 1
fi

# Function to run query
run_query() {
    psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "$1" 2>/dev/null
}

# Total connections
TOTAL=$(run_query "SELECT count(*) FROM pg_stat_activity;")
echo "Total connections: $TOTAL"

# Active connections
ACTIVE=$(run_query "SELECT count(*) FROM pg_stat_activity WHERE state = 'active';")
echo "Active connections: $ACTIVE"

# Idle connections
IDLE=$(run_query "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle';")
echo "Idle connections: $IDLE"

# Connections to current database
DB_CONN=$(run_query "SELECT count(*) FROM pg_stat_activity WHERE datname = '$DB_NAME';")
echo "Connections to '$DB_NAME': $DB_CONN"

# Max connections
MAX_CONN=$(run_query "SHOW max_connections;")
echo "Max connections allowed: $MAX_CONN"

echo ""
echo "=========================================="
echo "Connection Details:"
echo "=========================================="
psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT pid, usename, datname, state, query_start, LEFT(query, 50) as query_preview FROM pg_stat_activity WHERE datname = '$DB_NAME' ORDER BY query_start DESC;" 2>/dev/null

echo ""
echo "=========================================="
echo "Real-time monitoring (Ctrl+C to stop):"
echo "=========================================="
echo "Run: watch -n 1 \"psql -U $DB_USER -d $DB_NAME -t -c \\\"SELECT count(*) FROM pg_stat_activity WHERE datname = '$DB_NAME';\\\"\""

