# Petaly Configuration Guide (`petaly.ini`)

## Overview

The `petaly.ini` file is the main configuration file for the Petaly ETL tool. It controls the workspace setup and global settings. This guide explains each parameter in detail and provides examples for different use cases.

## File Structure

The configuration file is divided into three main sections:

```ini
[workspace_config]
# Workspace directory configurations

[global_settings]
# Global tool settings

```

## Workspace Configuration

The `[workspace_config]` section defines the core directories for Petaly's operation.

### Parameters

| Parameter | Description | Required | Example |
|-----------|-------------|----------|---------|
| `pipeline_dir_path` | Absolute path where all pipeline configurations are stored. This is where Petaly will create and manage your pipeline files. | Yes | `/home/user/petaly/pipelines` |
| `logs_dir_path` | Absolute path for storing log files. Petaly will create detailed logs of all operations here. | Yes | `/home/user/petaly/logs` |
| `output_dir_path` | Absolute path for temporary data storage during pipeline execution. This directory is used as a transition space between source and destination. | Yes | `/home/user/petaly/output` |
| `connections_file_path` | Absolute path to the connections.yaml/json file. If not specified, defaults to `pipeline_dir_path/connections.yaml`. | No | `/home/user/petaly/connections.yaml` |

### Example
```ini
[workspace_config]
pipeline_dir_path=/home/user/petaly/pipelines
logs_dir_path=/home/user/petaly/logs
output_dir_path=/home/user/petaly/output
# Optional: Custom connections file path
# If not specified, defaults to: pipeline_dir_path/connections.yaml
connections_file_path=~/.petaly/connections.yaml
```

## Global Settings

The `[global_settings]` section controls the general behavior of Petaly.

### Parameters

| Parameter | Description | Default | Options | Example |
|-----------|-------------|---------|---------|---------|
| `logging_mode` | Controls the verbosity of logging output. Use DEBUG for troubleshooting and INFO for normal operation. | INFO | INFO, DEBUG | `logging_mode=DEBUG` |
| `pipeline_file_format` | Determines the format of pipeline configuration files. YAML is more readable, while JSON is better for integration with other tools. | yaml | yaml, json | `pipeline_file_format=json` |
| `connections_file_format` | Determines the format of connections configuration files. | yaml | yaml, json | `connections_file_format=json` |
| `csv_analysis_max_size_mb` | Maximum file size (in MB) to analyze for metadata extraction when processing CSV files. Files larger than this limit will only have the first portion analyzed. Set to 0 to analyze entire file. | 10 | Integer (0 or greater) | `csv_analysis_max_size_mb=20` |
| `full_pipeline_wizard` | Controls the pipeline initialization wizard mode. If `true`, asks all questions during pipeline initialization. If `false`, only asks questions marked as `wizard_required=true` in pipeline_meta_config.json. | true | true, false | `full_pipeline_wizard=false` |

### Example
```ini
[global_settings]
logging_mode=INFO
pipeline_file_format=yaml
connections_file_format=yaml
csv_analysis_max_size_mb=10
full_pipeline_wizard=true
```

## Complete Example

Here's a complete example of a `petaly.ini` file:

```ini
[workspace_config]
pipeline_dir_path=/home/user/petaly/pipelines
logs_dir_path=/home/user/petaly/logs
output_dir_path=/home/user/petaly/output
# Optional: Custom connections file path
connections_file_path=

[global_settings]
logging_mode=INFO
pipeline_file_format=yaml
connections_file_format=yaml
csv_analysis_max_size_mb=10
full_pipeline_wizard=true

```

## Best Practices

1. **Security**
   - Store sensitive information like API keys in environment variables
   - Use appropriate file permissions for the config file
   
2. **Directory Structure**
   - Use separate directories for different environments (dev, prod, etc.)
   - Ensure sufficient disk space for the output directory
   - Use absolute paths to avoid confusion

3. **Logging**
   - Start with INFO mode for normal operation
   - Switch to DEBUG mode only when troubleshooting
   - Regularly check log files for issues

4. **Pipeline Format**
   - Use YAML for better readability and manual editing
   - Use JSON if integration with other JSON-based tools is needed
   - Be consistent with the format across all pipelines

5. **Pipeline Wizard**
   - Use `full_pipeline_wizard=true` (default) for comprehensive configuration
   - Set `full_pipeline_wizard=false` for faster pipeline creation with minimal prompts
   - Only parameters marked as `wizard_required=true` will be prompted in short form mode

## Environment Variables

Petaly supports the following environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `PETALY_CONFIG_DIR` | Directory containing the `petaly.ini` file | `export PETALY_CONFIG_DIR=/home/user/.petaly` |

## Troubleshooting

1. **Configuration Not Found**
   - Ensure `PETALY_CONFIG_DIR` is set correctly
   - Verify the config file exists in the specified directory
   - Check file permissions

2. **Directory Access Issues**
   - Verify all paths are absolute
   - Check directory permissions
   - Ensure sufficient disk space


## Related Topics

- [Pipeline Configuration](pipeline_examples.md)
- [Data Objects Specification](data_objects_spec.md)
- [Error Messages](error_messages.md)
- [Source and Target Attributes](source_target_attributes.md)
- [Installation Guide](installation.md)

## Additional Resources

- [Pipeline Configuration Guide](../pipeline_examples.md)
- [Video Tutorials](recording/) 