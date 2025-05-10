# Petaly: Change Log ![](https://raw.githubusercontent.com/petaly-labs/petaly/main/images/logo/petaly_favicon_small.png)

All notable changes to the Petaly project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.0] - 2025-05-10 (BETA)

### Added
- Official Beta release of Petaly
- AI agent integration with LLM providers [EXPERIMENTAL]:
  - OpenAI
  - Anthropic
- Support for JSON format in pipeline configuration
- Comprehensive unit tests for all connectors and AI agent
- Enhanced documentation and README
- renamed templates_petaly.ini to petaly.ini-template

### Configuration
New configuration options in `petaly.ini`:
```ini
# Pipeline format selection (yaml or json, default: yaml)
pipeline_format=yaml

[ai_settings]
# LLM provider selection (openai or anthropic, default: openai)
llm_provider=openai

# LLM model selection (tested with gpt-4, claude-3-opus-20240229)
llm_model=gpt-4

# Memory file path (default: ~/.petaly/agent_memory.json)
agent_memory_file=~/.petaly/agent_memory.json

# AI Agent API Key configuration
# Set via environment variable: export AI_AGENT_API_KEY="YOUR-KEY"
# Or directly in config (not recommended for production)
ai_agent_api_key=
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

### Fixed
- Fixed templates_petaly.ini file issues

## [v0.0.2-alpha] - 2024-09-26

### Changed
- Moved CSV file analysis to workspace folder

## [v0.0.1-alpha] - 2024-09-25
- Initial release