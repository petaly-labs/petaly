# ![](https://raw.githubusercontent.com/petaly-labs/petaly/main/images/logo/petaly_favicon_small.png)Petaly: Change Log

## [v0.0.8] - 2024-12-12

### Added

Improved logging behaviour and added new section in petaly.ini config file:

[global_settings]
The logging mode has two settings: INFO and DEBUG.
By default, it is set to INFO, which generates minimal log output.
If an issue occurs, switch to DEBUG for more detailed output that can assist in troubleshooting.

logging_mode=DEBUG

### Changed

renamed pipeline/data_objects_spec parameters:
renamed `recreate_target_object` to `recreate_destination_object`
renamed `files_source_dir` `object_source_dir`
renamed `object_attributes` to `object_spec` 
relocate the parameter `object_name` into nested part under `object_spec:` 

previous structure of `data_objects_spec`:

```
data_objects_spec:
- object_name: stocks
  object_attributes:
    object_name: stocks
    destination_object_name:
    recreate_target_object: true
    cleanup_linebreak_in_fields: true
    exclude_columns: 
    -
    files_source_dir:
    file_names:
    -
```

new structure of `data_objects_spec`:

```
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: true
    exclude_columns: 
    -
    object_source_dir:
    file_names:
    -
```

The entire documentation is now consolidated in the README.md file

### Fixed

## [v0.0.7.1] - 2024-12-05 - Major Release - Post 1

### Added

New documentation is added under [run_pipeline.md](.docs/tutorial/run_pipeline.md)
Added gzip csv files as test data to the `.tests/data/csv/` folder: `stocks.csv.gz` and `options.csv.gz`

### Changed

The parameter `csv_parse_options` has been renamed to `object_default_settings` because the context of csv_parse_options was very limited. 
The new object_default_settings allows for the inclusion of a broader range of options.

## [v0.0.7] - 2024-12-04 - Major Release

This is a major release that affects all files and changes the logic flow of pipelines, as well as renaming several parameters and changing their behaviour.
For pipelines created with the previous version, it is recommended to rebuild all pipelines with the wizard and set all parameters again.

### Added

- New tutorial files: petaly_init_workspace.md, petaly_install.md
- Improved petaly_install.md
- Added main parsing definition for csv files for: header, delimiter, quote
  ```
  csv_parse_options:
    header: true
    columns_delimiter: ','
    quote_char: double-quote```
- 
### Changed
The following pipeline parameters have been modified:
- `endpoint_type` to `connector_type`
- `use_data_objects_spec` to `data_objects_spec_mode`; The behaviour has also changed. New modes have been added: ***only***, ***ignore***, ***prefer***
- Removed all unused parameters
- Simplified the entire pipeline to make it easier to use
      
### Fixed
- All md links have been changed to absolute paths. This should work on both github.com and pypi.com. 
- Fixed bugs

## [v0.0.6] - 2024-11-22
minor change

## [v0.0.5] - 2024-11-18

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