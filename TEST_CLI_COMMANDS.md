# CLI Test Commands

## Basic Setup

### 1. Initialize Workspace
```bash
# Initialize workspace (creates directories and endpoints.yaml)
petaly init --workspace
```

### 2. Check Configuration
```bash
# Show workspace structure
petaly show --workspace

# List all pipelines
petaly show

# List all endpoints (if endpoints.yaml exists)
petaly show -e <endpoint_name>  # Shows specific endpoint
```

## Endpoint Management

### 3. Create Endpoints
```bash
# Create a new endpoint (will prompt for type: source/target)
petaly init -e my_postgres_source

# Create another endpoint
petaly init -e my_bigquery_target

# Modify existing endpoint
petaly init -e my_postgres_source
```

### 4. View Endpoints
```bash
# Show specific endpoint details
petaly show -e my_postgres_source

# Show another endpoint
petaly show -e my_bigquery_target
```

## Pipeline Management

### 5. Create Pipeline
```bash
# Create a new pipeline (will prompt for endpoint usage)
petaly init -p my_test_pipeline

# Create pipeline with specific object
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
# pipeline_format=json
# endpoint_format=json

# Then test:
petaly init --workspace
petaly init -e my_json_endpoint
petaly init -p my_json_pipeline
```

## Testing Auto-Creation

### 10. Test Auto-Creation of Files
```bash
# If endpoints.yaml doesn't exist, it should be created during workspace init
# If pipeline.yaml doesn't exist, it should be created when accessing pipeline

# Test pipeline auto-creation
petaly show -p non_existent_pipeline
# Should create pipeline.yaml from skeleton

# Test endpoints auto-creation (if not created during workspace init)
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

# 2. Create source endpoint
petaly init -e postgres_prod

# 3. Create target endpoint  
petaly init -e bigquery_warehouse

# 4. Create pipeline (will use endpoints)
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

