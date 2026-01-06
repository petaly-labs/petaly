# Commit Summary

## Overview
Major improvements: CSV/Parquet/JSON conversion fixes, parameter renaming, file connector enhancements, and direct export/load support for BigQuery/Redshift.

**Stats:** 33 files changed, +1957/-577 lines

## Key Changes

### 1. Parameter Renaming
- `include_data_objects` → `use_data_objects_spec` (`"all"`/`"spec"` → `"prefer"`/`"strict"`, default changed to `"prefer"`)
- `object_default_settings` → `csv_default_settings`
- Removed `pipeline_attributes` nesting (moved `pipeline_name` directly under `pipeline`)
- **Backward compatibility:** All old parameter names still work with deprecation warnings

### 2. CSV to Parquet/JSON Conversion Fix
- **Critical:** Fixed corruption when converting CSV with `columns_quote: none` to Parquet/JSON
- Implemented manual CSV parser to handle escaped commas (`\,`) in JSON fields
- Prevents pandas from incorrectly splitting fields with embedded commas

### 3. Direct Format Support
- BigQuery/Redshift: Direct export to Parquet/JSON (no CSV intermediate)
- BigQuery/Redshift: Direct load from Parquet/JSON files
- PostgreSQL/MySQL: Auto-convert Parquet/JSON to CSV before loading

### 4. File Connector Improvements
- Unified CSV/Parquet/JSON connector structure under `file/` directory
- Added `source_dir` and `object_source_dir` support for Parquet/JSON
- Implemented complementary path logic: `source_dir + / + object_source_dir` or `source_dir + / + object_name`

### 5. CLI & Documentation
- Removed redundant prompts, always use pipeline wizard
- Added Parquet/JSON to README data sources
- Created comprehensive CLI reference documentation
- Added unit tests for core classes (connections, pipeline, data_object, composer)

## Breaking Changes
⚠️ Parameter names changed (backward compatible):
- `include_data_objects` → `use_data_objects_spec` (`"all"`/`"spec"` → `"prefer"`/`"strict"`)
- `object_default_settings` → `csv_default_settings`
- `pipeline_attributes` section removed (use `pipeline_name` directly)

## Files Changed
- **Modified:** 28 files
- **New:** 6 files (file connectors, scripts, tests, docs)
- **Deleted:** 11 files (old CSV connector structure)
