# Commit Summary

## Incremental Load Implementation (Under Development)
- Implemented incremental load feature for MySQL and PostgreSQL sources (work in progress, not yet tested)
- Added LoadState class and load_state.json files for state management
- Added validation to restrict incremental load to MySQL/PostgreSQL only

## Pipeline Wizard Improvements
- Added full_pipeline_wizard parameter in petaly.ini (default: true)
- Added wizard_required field for selective prompting in short form mode

## Configuration & UX Improvements
- Fixed connections_file_path to expand ~ (tilde) for home directory paths
- Reordered pipeline initialization: source questions first, then target questions
- Enhanced connection prompts with bold formatting for "source" and "target"
- Added separate prompts for pipeline config, data folders, and metadata folders deletion
- Extended help text for empty data_objects_spec with incremental load guidance

## Documentation
- Updated CHANGELOG.md with incremental load feature
- Added MySQL/PostgreSQL restriction notes in pipeline_meta_config.json
