# Change Log
## [v0.0.5] - 2024-11-18 

### Added

- New tutorial file pipeline_explained.md

### Changed

- renamed petaly.ini parameters:
  - **pipeline_base_dir_path** renamed to **pipeline_dir_path**
  - **logs_base_dir_path** renamed to **logs_dir_path**
  - **output_base_dir_path** renamed to **output_dir_path**
<br><br>
- renamed pipeline.yaml config parameter:
  - **load_data_objects_spec_only** renamed to **use_data_objects_spec**
<br><br>
- following parameters were moved into the block **data_object_main_config:** in pipeline:  
```
pipeline:
...
    data_object_main_config:   
      # Only full load is supported yet
      preferred_load_type: full
      # Only csv format is supported yet  
      data_transition_format: csv
      # provide fine definition 
      use_data_objects_spec: true
---      
```
### Fixed

- MD links were improved

## [v0.0.4-alpha] - 2024-10-18 
### Added
- None
### Changed
- Changed logging_config to INFO level for log files
### Fixed
Fixed the recreate_target_object parameter, now it can be controlled from the pipeline definition.

## [v0.0.3-alpha] - 2024-09-26 
### Added
- CHANGELOG file was added to repo   
### Changed
- changed PETALY_CONFIG_PATH to PETALY_CONFIG_DIR
  - switched environment variable to allow use of multiple .ini files configured in directory path
- cli message was improved 
### Fixed
- fixed issue with templates_petaly.ini file

## [v0.0.2-alpha] - 2024-09-26 
### Changed
- moved csv file analysis from source folder into in workspace folder
## [v0.0.1-alpha] - 2024-09-25 
- Initial version