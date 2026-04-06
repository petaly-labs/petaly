# Data Objects Specification Guide

This guide explains how to use the `data_objects_spec` main section in your pipeline configuration to handle specific data objects and their settings.

## Overview

The `data_objects_spec` is second main section in your pipeline YAML file that defines how to handle specific data objects (tables/files) in your pipeline, including source and destination mappings, file specifications, and object-specific settings.

## Connection Between Sections

The pipeline configuration uses data_objects_spec sections to handle data objects:

1. **Pipeline main section**:
   ```yaml
   pipeline:
     pipeline_name: my_pipeline
     load_attributes:
       all_from_schema: true          # Load all objects from schema (true) or only from data_objects_spec (false)
       use_data_objects_spec: true    # Apply specifications from data_objects_spec if they exist (true) or ignore them (false)
       csv_default_settings:        # Default settings for CSV/TSV/TXT files
         header: true
         columns_delimiter: ","
         columns_quote: double
   ```

2. **Data Objects Specification main section**:
   ```yaml
   # Second main section: Data objects specification
   data_objects_spec:
   - object_spec:
       object_name: source_table
       destination_object_name: target_table
   ```

### all_from_schema and use_data_objects_spec

These two boolean parameters in `load_attributes` control how objects are loaded and how `data_objects_spec` is used:

- **`all_from_schema`** (default: `true`):
  - `true`: Load all objects from the database_schema (or database_name if no schema exists) as defined in the source_attributes section.
  - `false`: Load only the objects explicitly specified in `data_objects_spec`. If `data_objects_spec` is empty, no objects will be loaded.

- **`use_data_objects_spec`** (default: `true`):
  - `true`: Apply object specifications from `data_objects_spec[]` if they exist (including incremental load settings). Objects not specified in `data_objects_spec[]` will load with default settings.
  - `false`: Ignore `data_objects_spec[]` and use default settings for all objects.

**Important:** The combination `all_from_schema: false` and `use_data_objects_spec: false` is **invalid** and will result in no objects being loaded. At least one parameter must be `true`.

### Parameter Combinations

| `all_from_schema` | `use_data_objects_spec` | Behavior |
|-------------------|------------------------|----------|
| `true` | `true` | Load all objects from schema. Apply specifications from `data_objects_spec[]` where they exist. (Default) |
| `true` | `false` | Load all objects from schema. Ignore `data_objects_spec[]` and use default settings for all objects. |
| `false` | `true` | Load only objects specified in `data_objects_spec[]`. Apply their specifications. |
| `false` | `false` | **INVALID** - No objects will be loaded. This combination is not allowed. |

**Example of Invalid Configuration:**
```yaml
load_attributes:
  all_from_schema: false
  use_data_objects_spec: false
  # This will result in an error - no objects will be loaded
```

## Basic Structure

```yaml
# Second main section: Data objects specification
data_objects_spec:
- object_spec:
    object_name: source_table
    destination_object_name: target_table  # Optional
    recreate_destination_object: true      # Optional
    cleanup_linebreak_in_fields: false     # Optional
    exclude_columns:                       # Optional
      - column1
      - column2
    object_source_dir: csv/files           # For file sources with source_base_dir
    object_target_dir: exports/files       # Optional for file targets with target_base_dir
    file_names:                            # For CSV sources
      - file1.csv
      - file2.csv
    # Incremental Load Parameters (values only relevant when extract_load_mode: incremental)
    extract_load_mode: incremental         # Options: "full" (default) or "incremental"
    column_primary_key: id                 # Required when extract_load_mode: incremental, can be empty for full
    column_last_modified: modified_at     # Required when extract_load_mode: incremental, can be empty for full
    batch_size: 10000                      # Optional when extract_load_mode: incremental, can be empty for full
```

## Configuration Options

### object_spec
Settings for each data object:
- `object_name`: Name of the source data object (required)
- `destination_object_name`: Optional name for the target object
- `recreate_destination_object`: Whether to recreate the target object (default: false)
- `cleanup_linebreak_in_fields`: Handle line breaks in fields (default: false)
- `exclude_columns`: List of columns to exclude
- `object_source_dir`: Source subdirectory relative to `source_base_dir`, or an absolute path if no base dir is configured
- `object_target_dir`: Target subdirectory relative to `target_base_dir`, or an absolute path if no base dir is configured
- `file_names`: List of files to process (for CSV sources)
- `extract_load_mode`: Extract/load mode - `"full"` (default) or `"incremental"` (see [Incremental Load](#incremental-load))
- `column_primary_key`: Primary key column name for incremental load (required only if `extract_load_mode: incremental`, can be omitted/empty for `extract_load_mode: full`)
- `column_last_modified`: Timestamp column name for incremental load (required only if `extract_load_mode: incremental`, can be omitted/empty for `extract_load_mode: full`)
- `batch_size`: Number of rows to load per batch in incremental mode (only relevant if `extract_load_mode: incremental`, can be omitted/empty for `extract_load_mode: full`)

## Examples

### CSV Files with Default Settings
```yaml
# First main section: Pipeline configuration
pipeline:
  pipeline_name: csv_to_db
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ","
      columns_quote: double

# Second main section: Data objects specification
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name: stocks_new
    recreate_destination_object: true
    object_source_dir: incoming/stocks
    file_names:
      - stocks.csv
```

### Database Tables with All Mode
```yaml
# First main section: Pipeline configuration
pipeline:
  pipeline_name: db_to_db
  load_attributes:
    all_from_schema: true   # Will load all tables from schema
    use_data_objects_spec: true  # Will apply specific settings to listed ones

# Second main section: Data objects specification
data_objects_spec:
- object_spec:
    object_name: customers
    destination_object_name: customers_new
    recreate_destination_object: true
    exclude_columns:
      - created_at
      - updated_at
```

### Incremental Load

Incremental load allows you to transfer only new or updated rows based on a timestamp column, making it efficient for regularly updating large datasets.

**Important:** 
- Incremental load is only supported for MySQL and PostgreSQL sources.
- When `extract_load_mode: full` (default), the incremental load parameters (`column_primary_key`, `column_last_modified`, `batch_size`) can be omitted or left empty.
- These parameters are only required/relevant when `extract_load_mode: incremental`.

**Configuration for Incremental Load:**
```yaml
data_objects_spec:
- object_spec:
    object_name: users
    extract_load_mode: incremental
    column_primary_key: id              # Required for incremental load
    column_last_modified: modified_at   # Required for incremental load
    batch_size: 10000                   # Optional: rows per batch (enables resumable loads)
```

**Configuration for Full Load (default):**
```yaml
data_objects_spec:
- object_spec:
    object_name: users
    extract_load_mode: full  # or omit extract_load_mode entirely (defaults to "full")
    # column_primary_key, column_last_modified, and batch_size can be omitted
```

**How it works:**
- On first run: If the target table doesn't exist, it performs a full load and creates a state file (`load_state.json`) in the object's metadata directory.
- On subsequent runs: Only rows with `column_last_modified` greater than the last loaded timestamp are extracted.
- State persistence: The last loaded timestamp is saved after each batch (if `batch_size` is set) or after the full load completes, allowing the process to resume if interrupted.
- State file location: `{output_dir_path}/{pipeline_name}/{object_name}/metadata/load_state.json`

**Example: MySQL to PostgreSQL with Incremental Load**
```yaml
pipeline:
  pipeline_name: mysql_to_postgres_incremental
  source_attributes:
    connector_type: mysql
    database_user: root
    database_password: dbpassword
    database_host: localhost
    database_port: 3306
    database_name: source_db
  target_attributes:
    connector_type: postgres
    database_user: postgres
    database_password: dbpassword
    database_host: localhost
    database_port: 5432
    database_name: target_db
    database_schema: public
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true

data_objects_spec:
- object_spec:
    object_name: users
    extract_load_mode: incremental
    column_primary_key: user_id
    column_last_modified: updated_at
    batch_size: 10000
- object_spec:
    object_name: orders
    extract_load_mode: full  # This table uses full load
```

## Best Practices

1. **Mode Selection**
   - Use `all_from_schema: false` when you need precise control over which objects to process
   - Use `all_from_schema: true` with `use_data_objects_spec: true` when you want to process all objects but customize some

2. **Object Specification**
   - Use meaningful names for source and destination objects
   - Specify file paths and names for CSV sources
   - Use `exclude_columns` to optimize data transfer

3. **Incremental Load**
   - Use incremental load for large tables that are regularly updated
   - Ensure your source table has a reliable timestamp column (`column_last_modified`)
   - Set `batch_size` for large datasets to enable resumable loads
   - Only use incremental load with MySQL or PostgreSQL sources
   - Monitor the `load_state.json` file to track load progress

4. **Performance Considerations**
   - Use `recreate_destination_object` for fresh loads
   - Exclude unnecessary columns
   - Handle line breaks appropriately
   - Use incremental load to reduce data transfer for frequently updated tables

## Common Issues

1. **Missing Objects**
   - Check object names match exactly
   - Verify file paths or table names
   - Ensure all required fields are present

2. **Invalid Settings**
   - Ensure settings are valid for the object type
   - Check file paths exist for CSV sources
   - Verify column names for exclusions

## Troubleshooting

1. **Object Not Found**
   ```yaml
   # Second main section: Data objects specification
   data_objects_spec:
   - object_spec:
       object_name: "table1"  # Check exact name
       destination_object_name: "table1_new"
   ```

2. **Invalid Configuration**
   ```yaml
   # Second main section: Data objects specification
   data_objects_spec:
   - object_spec:
       object_name: "table1"
       destination_object_name: "table1_new"  # Optional but recommended
       recreate_destination_object: true       # Optional
   ```

3. **CSV File Issues**
   ```yaml
   # Second main section: Data objects specification
   data_objects_spec:
   - object_spec:
       object_name: "data"
       object_source_dir: "/path/to/files"  # Verify path exists
       file_names:
         - "file1.csv"  # Verify file exists
   ```

## Related Topics

- [Pipeline Configuration](pipeline_examples.md)
- [Error Messages](error_messages.md)
- [Performance](performance.md)
- [Source and Target Attributes](source_target_attributes.md)
