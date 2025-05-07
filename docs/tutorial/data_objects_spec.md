# Data Objects Specification Guide

This guide explains how to use the `data_objects_spec` document in your pipeline configuration to handle specific data objects and their settings.

## Overview

The `data_objects_spec` is a separate document in your pipeline YAML file that defines how to handle specific data objects (tables/files) in your pipeline, including source and destination mappings, file specifications, and object-specific settings.

## Connection Between Sections

The pipeline configuration uses three connected sections to handle data objects:

1. **Pipeline Document**:
   ```yaml
   pipeline:
     data_attributes:
       data_objects_spec_mode: only  # Controls how data_objects_spec is used
       object_default_settings:      # Default settings for all objects
         header: true
         columns_delimiter: ","
         columns_quote: double
   ```

2. **Data Objects Specification Document**:
   ```yaml
   ---
   # Second document: Data objects specification
   data_objects_spec:
   - object_spec:
       object_name: source_table
       destination_object_name: target_table
   ```

### data_objects_spec_mode

The `data_objects_spec_mode` in `data_attributes` controls how the `data_objects_spec` document is used:

- `only`: Load only the objects explicitly specified in `data_objects_spec`. These objects will be configured in the next step.
- `ignore`: Load all objects from the database_schema (or database_name if no schema exists) as defined in the source_attributes section, completely disregarding `data_objects_spec`.
- `prefer`: Load all objects from the database_schema, but for objects specified in `data_objects_spec`, apply the refined configuration defined in that section.

## Basic Structure

```yaml
---
# Second document: Data objects specification
data_objects_spec:
- object_spec:
    object_name: source_table
    destination_object_name: target_table  # Optional
    recreate_destination_object: true      # Optional
    cleanup_linebreak_in_fields: false     # Optional
    exclude_columns:                       # Optional
      - column1
      - column2
    object_source_dir: /path/to/files      # For CSV sources
    file_names:                            # For CSV sources
      - file1.csv
      - file2.csv
```

## Configuration Options

### object_spec
Settings for each data object:
- `object_name`: Name of the source data object (required)
- `destination_object_name`: Optional name for the target object
- `recreate_destination_object`: Whether to recreate the target object (default: false)
- `cleanup_linebreak_in_fields`: Handle line breaks in fields (default: false)
- `exclude_columns`: List of columns to exclude
- `object_source_dir`: Directory containing source files (for CSV sources)
- `file_names`: List of files to process (for CSV sources)

## Examples

### CSV Files with Default Settings
```yaml
# First document: Pipeline configuration
pipeline:
  data_attributes:
    data_objects_spec_mode: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      columns_quote: double

---
# Second document: Data objects specification
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name: stocks_new
    recreate_destination_object: true
    object_source_dir: /path/to/csv/files
    file_names:
      - stocks.csv
```

### Database Tables with Prefer Mode
```yaml
# First document: Pipeline configuration
pipeline:
  data_attributes:
    data_objects_spec_mode: prefer  # Will load all tables but apply specific settings to listed ones

---
# Second document: Data objects specification
data_objects_spec:
- object_spec:
    object_name: customers
    destination_object_name: customers_new
    recreate_destination_object: true
    exclude_columns:
      - created_at
      - updated_at
```

## Best Practices

1. **Mode Selection**
   - Use `only` when you need precise control over which objects to process
   - Use `prefer` when you want to process all objects but customize some
   - Use `ignore` when you want to process all objects with default settings

2. **Object Specification**
   - Use meaningful names for source and destination objects
   - Specify file paths and names for CSV sources
   - Use `exclude_columns` to optimize data transfer

3. **Performance Considerations**
   - Use `recreate_destination_object` for fresh loads
   - Exclude unnecessary columns
   - Handle line breaks appropriately

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
   ---
   # Second document: Data objects specification
   data_objects_spec:
   - object_spec:
       object_name: "table1"  # Check exact name
       destination_object_name: "table1_new"
   ```

2. **Invalid Configuration**
   ```yaml
   ---
   # Second document: Data objects specification
   data_objects_spec:
   - object_spec:
       object_name: "table1"
       destination_object_name: "table1_new"  # Optional but recommended
       recreate_destination_object: true       # Optional
   ```

3. **CSV File Issues**
   ```yaml
   ---
   # Second document: Data objects specification
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