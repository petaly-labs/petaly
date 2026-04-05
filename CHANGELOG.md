# Petaly: Change Log ![](https://raw.githubusercontent.com/petaly-labs/petaly/main/images/logo/petaly_favicon_small.png)

All notable changes to the Petaly project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.2.0]

### Added
- **Incremental Load Support**: New incremental load feature for MySQL and PostgreSQL sources allows loading only new or updated rows based on timestamp columns. Configure per object with `load_mode: "incremental"`, `column_last_modified`, `column_primary_key`, and `batch_size`
- `load_state.json` state tracking for resumable incremental loads
- Export support for BigQuery and Redshift to Parquet and JSON format
- Load support for BigQuery and Redshift from Parquet and JSON files
- Parquet and JSON to CSV conversion for PostgreSQL and MySQL loaders
- Support for Parquet and JSON file connectors as source and target
- Configurable `connections_file_path` parameter in `petaly.ini` (defaults to `pipeline_dir_path/connections.yaml`)
- Parallel processing support with `max_workers` parameter for concurrent object processing
- `exclude_objects` parameter in `load_attributes` to exclude specific objects from processing
- `flow_mode` parameter (`"object"` or `"dump"`) to control processing flow
- `null_string` and `force_null` parameters in `csv_default_settings`
- `full_pipeline_wizard` parameter in `petaly.ini` to control wizard mode
- Thread-safe parallel processing with thread-local database connections
- Utility scripts:
  - `scripts/add_pk_and_modified.sh`
  - `scripts/check_parallel.sh`
  - `scripts/check_postgres_connections.sh`
  - `scripts/check_mysql_connections.sh`
  - `scripts/inspect_parquet.py`

### Changed
- **BREAKING:** Renamed `data_attributes` to `load_attributes`
- **BREAKING:** Replaced `include_data_objects` with `all_from_schema` and `use_data_objects_spec`
- **BREAKING:** Renamed `object_default_settings` to `csv_default_settings`
- **BREAKING:** Removed `pipeline_attributes` nesting and moved `pipeline_name` directly under `pipeline`
- **BREAKING:** Removed `is_enabled` parameter, pipelines are always enabled
- Improved object-by-object execution flow in `MainCtl` with support for extract then load per object
- Updated file extractors and loaders to use clearer per-object processing methods
- Enhanced BigQuery and Redshift loaders to detect Parquet and JSON directly and improve file handling
- Improved CSV to Parquet and JSON conversion with better handling of complex data types
- Enhanced file connector structure (CSV, Parquet, JSON unified under `file/` directory)
- Added `source_dir` and `object_source_dir` support for Parquet and JSON connectors
- Streamlined CLI prompts and initialization flow
- Removed YAML document separators (`---`) from generated pipeline files and standardized on a single-document format
- Updated pipeline initialization to support `exclude_objects` and richer default load settings
- All connection file path resolutions now respect `connections_file_path` configuration

### Fixed
- Fixed CSV corruption issue when converting CSV with `columns_quote: none` to Parquet or JSON
- Improved loader row counting and per-object load summary behavior
- Improved handling of mixed compressed and uncompressed CSV inputs for cloud loaders

### Backward Compatibility
- Old pipelines with `pipeline_attributes` section continue to work with deprecation warning
- Old pipelines with `object_default_settings` continue to work with deprecation warning
- Old pipelines with `load_all_from_schema: true/false` are automatically converted
- Old pipelines with `include_data_objects` are converted to the new object-selection settings
- Old pipelines with `data_attributes` are converted to `load_attributes`
- Legacy multi-document YAML format with `---` separator is still supported

## [v0.1.0] - 2025-05-10 (BETA)

### Added
- Official Beta release of Petaly
- Support for JSON format in pipeline configuration
- Added unit tests for all connectors

### Changed
- For compatibility reasons, the YAML format has been updated:
  The structure using two documents separated by three dashes (---) is now deprecated.
  To ensure compatibility with the updated format, remove the three dashes from the YAML file or recreate the pipeline. petaly init -p pipeline-name
  
  Old Format:
  ```yaml
  pipeline:
    pipeline_attributes:
    source_attributes:
    target_attributes:
    load_attributes:
  ---
  data_objects_spec:
  - object_spec:   
  ```
  New Format:
  ```yaml
  pipeline:
    pipeline_attributes:
    source_attributes:
    target_attributes:
    load_attributes:
  data_objects_spec:
  - object_spec:   
  ```
- Enhanced documentation and README
- renamed templates_petaly.ini to petaly.ini-template

### Configuration
New configuration options in `petaly.ini`:
```ini
# Pipeline format selection (yaml or json, default: yaml)
pipeline_format=yaml

```

## [v0.0.10] - 2025-02-05

### Added
- New AWS Redshift Connectors:
  - Redshift Cluster (IAM and TCP connections)
  - Redshift Serverless (IAM and TCP connections)
- AWS S3-Bucket connection support

### Changed
- Renamed pipeline.yaml attributes:
  - `destination_file_dir` → `destination_dir`
  - `destination_blob_dir` → `bucket_pipeline_prefix`
- Updated bucket pipeline prefix behavior:
  - Default pattern: `{pipeline_name}`
  - Optional custom prefix with forward slash (/) for folder separation

### Fixed
- Various bug fixes and improvements

## [v0.0.9] - 2024-12-24

### Added
- GCP Connectors:
  - BigQuery
  - Google Storage (GS-Bucket)

### Changed
- Renamed pipeline.yaml attribute:
  - `data_attributes:object_default_settings:quote_char` → `columns_quote`
- Updated quote options to: `["double", "single", "none"]`
```yaml
pipeline:
  object_default_settings:
    header: true
    columns_delimiter: ','
    columns_quote: double
```

### Fixed
- Multiple bug fixes and stability improvements

## [v0.0.8] - 2024-12-12

### Added
- Enhanced logging system with new `petaly.ini` configuration:
```ini
[global_settings]
# Logging modes: INFO (default) or DEBUG
logging_mode=DEBUG
```

### Changed
- Restructured pipeline/data_objects_spec parameters:
  - `recreate_target_object` → `recreate_destination_object`
  - `files_source_dir` → `object_source_dir`
  - `object_attributes` → `object_spec`
  - Moved `object_name` under `object_spec`
- Consolidated documentation in README.md

### Structure Changes
Previous:
```yaml
data_objects_spec:
- object_name: stocks
  object_attributes:
    object_name: stocks
    destination_object_name:
    recreate_target_object: true
    cleanup_linebreak_in_fields: true
    exclude_columns: []
    files_source_dir:
    file_names: []
```

New:
```yaml
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: true
    exclude_columns: []
    object_source_dir:
    file_names: []
```

## [v0.0.7.1] - 2024-12-05

### Added
- New documentation: [run_pipeline.md](.docs/tutorial/run_pipeline.md)
- Test data: gzip CSV files in `.tests/data/csv/`
  - `stocks.csv.gz`
  - `options.csv.gz`

### Changed
- Renamed `csv_parse_options` to `object_default_settings` for broader configuration scope

## [v0.0.7] - 2024-12-04

### Added
- New tutorial files:
  - petaly_init_workspace.md
  - petaly_install.md
- CSV parsing configuration:
```yaml
csv_parse_options:
  header: true
  columns_delimiter: ','
  quote_char: double-quote
```

### Changed
- Pipeline parameter updates:
  - `endpoint_type` → `connector_type`
  - `use_data_objects_spec` → `data_objects_spec_mode`
    - New modes: `only`, `ignore`, `prefer`
- Simplified pipeline structure
- Removed unused parameters

### Fixed
- Updated MD links to absolute paths
- Various bug fixes

## [v0.0.6] - 2024-11-22
- Minor improvements and bug fixes

## [v0.0.5] - 2024-11-18

### Changed
- Renamed `petaly.ini` parameters:
  - `pipeline_base_dir_path` → `pipeline_dir_path`
  - `logs_base_dir_path` → `logs_dir_path`
  - `output_base_dir_path` → `output_dir_path`
- Renamed pipeline.yaml parameter:
  - `load_data_objects_spec_only` → `use_data_objects_spec`
- Moved parameters to `data_object_main_config` block:
```yaml
pipeline:
  data_object_main_config:   
    preferred_load_type: full
    data_transition_format: csv
    use_data_objects_spec: true
```

### Fixed
- Improved MD link formatting

## [v0.0.4-alpha] - 2024-10-18

### Changed
- Set default logging level to INFO

### Fixed
- Fixed `recreate_target_object` parameter control in pipeline definition

## [v0.0.3-alpha] - 2024-09-26

### Added
- Initial CHANGELOG.md

### Changed
- Renamed `PETALY_CONFIG_PATH` to `PETALY_CONFIG_DIR`
  - Supports multiple .ini files in directory
- Enhanced CLI messaging
