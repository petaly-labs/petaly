# CLI Reference

This guide provides a comprehensive reference for all Petaly CLI commands and common workflows.

## Basic Setup

### 1. Initialize Workspace
```bash
# Initialize workspace (creates directories and connections.yaml)
petaly init --workspace
```

### 2. Check Configuration
```bash
# Show workspace structure
petaly show --workspace

# List all pipelines
petaly show

# List all connections (if connections.yaml exists)
petaly show -c <connection_name>  # Shows specific connection
```

## Connection Management

### 3. Create Connections
```bash
# Create a new connection (will prompt for type: source/target)
petaly init -c postgres_source

# Create another connection
petaly init -c bigquery_target

# Modify existing connection
petaly init -c postgres_source
```

### 4. View Connections
```bash
# Show specific connection details
petaly show -c postgres_source

# Show another connection
petaly show -c bigquery_target
```

## Pipeline Management

### 5. Create Pipeline
```bash
# Create a new pipeline (will prompt for connection usage)
petaly init -p my_test_pipeline

# Create pipeline with specific objects
petaly init -p my_test_pipeline -o table1,table2
```

### 6. View Pipelines
```bash
# List all pipelines
petaly show

# Show specific pipeline details
petaly show -p my_test_pipeline
```

## Pipeline Execution

### 7. Run Pipeline
```bash
# Run full pipeline (source + target)
petaly run -p my_test_pipeline

# Run source only (extract)
petaly run -p my_test_pipeline -s

# Run target only (load)
petaly run -p my_test_pipeline -t
```

## Cleanup

### 8. Cleanup Operations
```bash
# Cleanup specific objects from pipeline
petaly cleanup -p my_test_pipeline -o table1
```

## Testing Different Formats

### 9. Test with JSON Format
```bash
# Edit petaly.ini to set:
# pipeline_file_format=json
# connections_file_format=json

# Then test:
petaly init --workspace
petaly init -c json_target
petaly init -p my_json_pipeline
```

## Testing Auto-Creation

### 10. Test Auto-Creation of Files
```bash
# If connections.yaml doesn't exist, it should be created during workspace init
# If pipeline.yaml doesn't exist, it should be created when accessing pipeline

# Test pipeline auto-creation
petaly show -p non_existent_pipeline
# Should create pipeline.yaml from skeleton

# Test connections auto-creation (if not created during workspace init)
# Will be created when first accessed
```

## Help Commands

### 11. Get Help
```bash
# Show general help
petaly -h

# Show help for specific command
petaly init -h
petaly show -h
petaly run -h
petaly cleanup -h
```

## Complete Workflow Example

```bash
# 1. Initialize workspace
petaly init --workspace

# 2. Create source connection
petaly init -c postgres_prod

# 3. Create target connection  
petaly init -c bigquery_warehouse

# 4. Create pipeline (will use connections)
petaly init -p csv_to_bigquery

# 5. Add data objects
# (will be prompted during pipeline init, or add later)
petaly init -p csv_to_bigquery -o customers,orders

# 6. View pipeline
petaly show -p csv_to_bigquery

# 7. Run pipeline
petaly run -p csv_to_bigquery

# 8. Check logs
# Logs are saved in logs_dir_path from petaly.ini
```

## Command Reference

### `petaly init`
Initialize Petaly workspace, connections, or pipelines.

**Options:**
- `--workspace`: Initialize workspace structure
- `-c, --connection <name>`: Create or modify a connection
- `-p, --pipeline <name>`: Create or modify a pipeline
- `-o, --object <objects>`: Specify data objects (comma-separated)
- `-h, --help`: Show help message

**Examples:**
```bash
petaly init --workspace
petaly init -c postgres_source
petaly init -p my_pipeline -o table1,table2
```

### `petaly show`
Display workspace, pipeline, or connection information.

**Options:**
- `--workspace`: Show workspace structure
- `-p, --pipeline <name>`: Show pipeline details
- `-c, --connection <name>`: Show connection details
- `-h, --help`: Show help message

**Examples:**
```bash
petaly show
petaly show -p my_pipeline
petaly show -c my_connection
```

### `petaly run`
Execute a pipeline.

**Options:**
- `-p, --pipeline <name>`: Pipeline name to run (required)
- `-s, --source-only`: Run extraction only
- `-t, --target-only`: Run loading only
- `-h, --help`: Show help message

**Examples:**
```bash
petaly run -p my_pipeline
petaly run -p my_pipeline -s
petaly run -p my_pipeline -t
```

### `petaly cleanup`
Clean up pipeline output files.

**Options:**
- `-p, --pipeline <name>`: Pipeline name (required)
- `-o, --object <objects>`: Specific objects to clean (comma-separated)
- `-h, --help`: Show help message

**Examples:**
```bash
petaly cleanup -p my_pipeline
petaly cleanup -p my_pipeline -o table1,table2
```
