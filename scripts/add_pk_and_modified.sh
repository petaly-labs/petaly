#!/bin/bash
# Script to add primary key and/or last_modified columns to a PostgreSQL table
# - If PK exists: adds only modified_at column
# - If no PK: adds both id (PK) and modified_at columns
# Creates a new table with the added columns from an existing table
# Uses connections.yaml for database connection details

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Add PK and/or Modified Column to Table${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Default connections.yaml path
DEFAULT_CONNECTIONS_PATH="$HOME/.petaly/connections.yaml"

read -p "Path to connections.yaml [${DEFAULT_CONNECTIONS_PATH}]: " CONNECTIONS_PATH
CONNECTIONS_PATH=${CONNECTIONS_PATH:-$DEFAULT_CONNECTIONS_PATH}

if [ ! -f "$CONNECTIONS_PATH" ]; then
    echo -e "${RED}Error: connections.yaml not found at: ${CONNECTIONS_PATH}${NC}"
    exit 1
fi

# List available PostgreSQL connections
echo ""
echo -e "${YELLOW}Available PostgreSQL connections:${NC}"
grep -B1 "connector_type: postgres" "$CONNECTIONS_PATH" | grep -E "^  [a-zA-Z]" | sed 's/://g' | sed 's/^  /  - /'

echo ""
read -p "Enter connection name: " CONNECTION_NAME

if [ -z "$CONNECTION_NAME" ]; then
    echo -e "${RED}Error: Connection name cannot be empty${NC}"
    exit 1
fi

# Parse connection details from YAML using grep/sed (simple parsing)
# This works for simple YAML structure without complex nesting
parse_yaml_value() {
    local key=$1
    local connection=$2
    local file=$3
    
    # Extract the connection block and find the key
    awk -v conn="$connection" -v key="$key" '
        /^  [a-zA-Z]/ { in_block = ($1 == conn":") }
        in_block && $1 == key":" { gsub(/^[^:]+: */, ""); gsub(/["'"'"']/, ""); print; exit }
    ' "$file"
}

DB_HOST=$(parse_yaml_value "database_host" "$CONNECTION_NAME" "$CONNECTIONS_PATH")
DB_PORT=$(parse_yaml_value "database_port" "$CONNECTION_NAME" "$CONNECTIONS_PATH")
DB_NAME=$(parse_yaml_value "database_name" "$CONNECTION_NAME" "$CONNECTIONS_PATH")
DB_USER=$(parse_yaml_value "database_user" "$CONNECTION_NAME" "$CONNECTIONS_PATH")
DB_PASS=$(parse_yaml_value "database_password" "$CONNECTION_NAME" "$CONNECTIONS_PATH")

# Validate connection details
if [ -z "$DB_HOST" ] || [ -z "$DB_NAME" ] || [ -z "$DB_USER" ]; then
    echo -e "${RED}Error: Could not parse connection details for '${CONNECTION_NAME}'${NC}"
    echo "Please check your connections.yaml file."
    exit 1
fi

# Set defaults
DB_PORT=${DB_PORT:-5432}

echo ""
echo -e "${CYAN}Connection details:${NC}"
echo "  Host: ${DB_HOST}"
echo "  Port: ${DB_PORT}"
echo "  Database: ${DB_NAME}"
echo "  User: ${DB_USER}"

# Prompt for schema
read -p "PostgreSQL schema [public]: " SCHEMA_NAME
SCHEMA_NAME=${SCHEMA_NAME:-public}

# Set password for psql
export PGPASSWORD="$DB_PASS"

echo ""
echo -e "${YELLOW}Available tables in schema '${SCHEMA_NAME}':${NC}"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '${SCHEMA_NAME}' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name;
" 2>/dev/null || {
    echo -e "${RED}Error: Could not connect to database${NC}"
    exit 1
}

echo ""
read -p "Enter source table name: " SOURCE_TABLE

if [ -z "$SOURCE_TABLE" ]; then
    echo -e "${RED}Error: Table name cannot be empty${NC}"
    exit 1
fi

# Check if table exists
TABLE_EXISTS=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
    SELECT COUNT(*) FROM information_schema.tables 
    WHERE table_schema = '${SCHEMA_NAME}' AND table_name = '${SOURCE_TABLE}';
" | tr -d ' ')

if [ "$TABLE_EXISTS" -eq 0 ]; then
    echo -e "${RED}Error: Table '${SCHEMA_NAME}.${SOURCE_TABLE}' does not exist${NC}"
    exit 1
fi

# Check if primary key exists
PK_COLUMN_EXISTING=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name 
        AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = '${SCHEMA_NAME}'
    AND tc.table_name = '${SOURCE_TABLE}'
    LIMIT 1;
" | tr -d ' ')

# Default new table name
DEFAULT_NEW_TABLE="${SOURCE_TABLE}_incremental"
read -p "Enter new table name [${DEFAULT_NEW_TABLE}]: " NEW_TABLE
NEW_TABLE=${NEW_TABLE:-$DEFAULT_NEW_TABLE}

# Column names
if [ -z "$PK_COLUMN_EXISTING" ]; then
    echo ""
    echo -e "${YELLOW}No primary key found - will add new PK column${NC}"
    read -p "Primary key column name [id]: " PK_COLUMN
    PK_COLUMN=${PK_COLUMN:-id}
    ADD_PK=true
else
    echo ""
    echo -e "${GREEN}Primary key exists: '${PK_COLUMN_EXISTING}' - will use it for ordering${NC}"
    PK_COLUMN="$PK_COLUMN_EXISTING"
    ADD_PK=false
fi

read -p "Modified timestamp column name [modified_at]: " MODIFIED_COLUMN
MODIFIED_COLUMN=${MODIFIED_COLUMN:-modified_at}

echo ""
echo -e "${YELLOW}Summary:${NC}"
echo "  Source table: ${SCHEMA_NAME}.${SOURCE_TABLE}"
echo "  New table:    ${SCHEMA_NAME}.${NEW_TABLE}"
if [ "$ADD_PK" = true ]; then
    echo "  PK column:    ${PK_COLUMN} (NEW - BIGSERIAL)"
else
    echo "  PK column:    ${PK_COLUMN} (EXISTING)"
fi
echo "  Modified:     ${MODIFIED_COLUMN} (TIMESTAMP WITH TIME ZONE)"
echo ""
echo -e "${CYAN}Modified column will have incremental timestamps (1 second apart)${NC}"
echo ""

read -p "Proceed? (y/n) [y]: " CONFIRM
CONFIRM=${CONFIRM:-y}

if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo -e "${GREEN}Creating new table...${NC}"

if [ "$ADD_PK" = true ]; then
    # Case 1: Add both PK and modified_at
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF

-- Drop new table if exists
DROP TABLE IF EXISTS ${SCHEMA_NAME}.${NEW_TABLE};

-- Get row count for incremental timestamps
DO \$\$
DECLARE
    v_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ${SCHEMA_NAME}.${SOURCE_TABLE};
    RAISE NOTICE 'Processing % rows with incremental timestamps...', v_count;
END \$\$;

-- Create new table with PK and modified_at columns + all original columns
-- modified_at gets incremental values: oldest row = oldest timestamp
CREATE TABLE ${SCHEMA_NAME}.${NEW_TABLE} AS
SELECT 
    ROW_NUMBER() OVER () AS ${PK_COLUMN},
    t.*,
    CURRENT_TIMESTAMP - ((SELECT COUNT(*) FROM ${SCHEMA_NAME}.${SOURCE_TABLE}) - ROW_NUMBER() OVER ()) * INTERVAL '1 second' AS ${MODIFIED_COLUMN}
FROM ${SCHEMA_NAME}.${SOURCE_TABLE} t;

-- Add primary key constraint
ALTER TABLE ${SCHEMA_NAME}.${NEW_TABLE} 
    ALTER COLUMN ${PK_COLUMN} SET NOT NULL,
    ADD PRIMARY KEY (${PK_COLUMN});

-- Make the PK column auto-increment for new inserts
CREATE SEQUENCE IF NOT EXISTS ${SCHEMA_NAME}.${NEW_TABLE}_${PK_COLUMN}_seq;
SELECT setval('${SCHEMA_NAME}.${NEW_TABLE}_${PK_COLUMN}_seq', 
              (SELECT MAX(${PK_COLUMN}) FROM ${SCHEMA_NAME}.${NEW_TABLE}));
ALTER TABLE ${SCHEMA_NAME}.${NEW_TABLE} 
    ALTER COLUMN ${PK_COLUMN} SET DEFAULT nextval('${SCHEMA_NAME}.${NEW_TABLE}_${PK_COLUMN}_seq');

-- Set default for modified_at
ALTER TABLE ${SCHEMA_NAME}.${NEW_TABLE} 
    ALTER COLUMN ${MODIFIED_COLUMN} SET DEFAULT CURRENT_TIMESTAMP;

-- Create trigger function for auto-update modified_at
CREATE OR REPLACE FUNCTION ${SCHEMA_NAME}.update_${NEW_TABLE}_modified()
RETURNS TRIGGER AS \$trg\$
BEGIN
    NEW.${MODIFIED_COLUMN} = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
\$trg\$ LANGUAGE plpgsql;

-- Create trigger (fires on both INSERT and UPDATE)
DROP TRIGGER IF EXISTS trg_${NEW_TABLE}_modified ON ${SCHEMA_NAME}.${NEW_TABLE};
CREATE TRIGGER trg_${NEW_TABLE}_modified
    BEFORE INSERT OR UPDATE ON ${SCHEMA_NAME}.${NEW_TABLE}
    FOR EACH ROW
    EXECUTE FUNCTION ${SCHEMA_NAME}.update_${NEW_TABLE}_modified();

EOF

else
    # Case 2: PK exists - add only modified_at with incremental values
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF

-- Drop new table if exists
DROP TABLE IF EXISTS ${SCHEMA_NAME}.${NEW_TABLE};

-- Get row count for incremental timestamps
DO \$\$
DECLARE
    v_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ${SCHEMA_NAME}.${SOURCE_TABLE};
    RAISE NOTICE 'Processing % rows with incremental timestamps...', v_count;
END \$\$;

-- Create new table with modified_at column (ordered by existing PK for incremental values)
-- modified_at gets incremental values based on PK order: lowest PK = oldest timestamp
CREATE TABLE ${SCHEMA_NAME}.${NEW_TABLE} AS
SELECT 
    t.*,
    CURRENT_TIMESTAMP - ((SELECT COUNT(*) FROM ${SCHEMA_NAME}.${SOURCE_TABLE}) - ROW_NUMBER() OVER (ORDER BY t.${PK_COLUMN})) * INTERVAL '1 second' AS ${MODIFIED_COLUMN}
FROM ${SCHEMA_NAME}.${SOURCE_TABLE} t;

-- Re-add primary key constraint
ALTER TABLE ${SCHEMA_NAME}.${NEW_TABLE} 
    ADD PRIMARY KEY (${PK_COLUMN});

-- Set default for modified_at
ALTER TABLE ${SCHEMA_NAME}.${NEW_TABLE} 
    ALTER COLUMN ${MODIFIED_COLUMN} SET DEFAULT CURRENT_TIMESTAMP;

-- Create trigger function for auto-update modified_at
CREATE OR REPLACE FUNCTION ${SCHEMA_NAME}.update_${NEW_TABLE}_modified()
RETURNS TRIGGER AS \$trg\$
BEGIN
    NEW.${MODIFIED_COLUMN} = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
\$trg\$ LANGUAGE plpgsql;

-- Create trigger (fires on both INSERT and UPDATE)
DROP TRIGGER IF EXISTS trg_${NEW_TABLE}_modified ON ${SCHEMA_NAME}.${NEW_TABLE};
CREATE TRIGGER trg_${NEW_TABLE}_modified
    BEFORE INSERT OR UPDATE ON ${SCHEMA_NAME}.${NEW_TABLE}
    FOR EACH ROW
    EXECUTE FUNCTION ${SCHEMA_NAME}.update_${NEW_TABLE}_modified();

EOF

fi

# Show results
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF

\echo ''
\echo 'Table structure:'
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns 
WHERE table_schema = '${SCHEMA_NAME}' AND table_name = '${NEW_TABLE}'
ORDER BY ordinal_position;

\echo ''
\echo 'Row count:'
SELECT COUNT(*) as total_rows FROM ${SCHEMA_NAME}.${NEW_TABLE};

\echo ''
\echo 'Timestamp range (incremental values):'
SELECT 
    MIN(${MODIFIED_COLUMN}) as oldest_timestamp,
    MAX(${MODIFIED_COLUMN}) as newest_timestamp,
    MAX(${MODIFIED_COLUMN}) - MIN(${MODIFIED_COLUMN}) as time_span
FROM ${SCHEMA_NAME}.${NEW_TABLE};

\echo ''
\echo 'Sample data (first 5 rows ordered by ${MODIFIED_COLUMN}):'
SELECT ${PK_COLUMN}, ${MODIFIED_COLUMN} FROM ${SCHEMA_NAME}.${NEW_TABLE} ORDER BY ${MODIFIED_COLUMN} LIMIT 5;

\echo ''
\echo 'Sample data (last 5 rows ordered by ${MODIFIED_COLUMN}):'
SELECT ${PK_COLUMN}, ${MODIFIED_COLUMN} FROM ${SCHEMA_NAME}.${NEW_TABLE} ORDER BY ${MODIFIED_COLUMN} DESC LIMIT 5;

EOF

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Done! Created table: ${SCHEMA_NAME}.${NEW_TABLE}${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "The new table has:"
if [ "$ADD_PK" = true ]; then
    echo "  - ${PK_COLUMN}: Auto-incrementing primary key (NEW)"
else
    echo "  - ${PK_COLUMN}: Primary key (from source table)"
fi
echo "  - ${MODIFIED_COLUMN}: Auto-updating timestamp on UPDATE"
echo "  - All columns from the original table"
echo ""
echo -e "${CYAN}Incremental timestamps assigned:${NC}"
echo "  - Lowest PK value → oldest timestamp"
echo "  - Highest PK value → newest timestamp (close to current time)"
echo "  - Each row 1 second apart"
