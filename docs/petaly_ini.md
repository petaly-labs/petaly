# Petaly Configuration Guide (`petaly.ini`)

## Overview

The `petaly.ini` file is the main configuration file for the Petaly ETL tool. It controls the workspace setup, global settings, and AI agent functionality. This guide explains each parameter in detail and provides examples for different use cases.

## File Structure

The configuration file is divided into three main sections:

```ini
[workspace_config]
# Workspace directory configurations

[global_settings]
# Global tool settings

[ai_settings]
# AI agent mode settings
```

## Workspace Configuration

The `[workspace_config]` section defines the core directories for Petaly's operation.

### Parameters

| Parameter | Description | Required | Example |
|-----------|-------------|----------|---------|
| `pipeline_dir_path` | Absolute path where all pipeline configurations are stored. This is where Petaly will create and manage your pipeline files. | Yes | `/home/user/petaly/pipelines` |
| `logs_dir_path` | Absolute path for storing log files. Petaly will create detailed logs of all operations here. | Yes | `/home/user/petaly/logs` |
| `output_dir_path` | Absolute path for temporary data storage during pipeline execution. This directory is used as a transition space between source and destination. | Yes | `/home/user/petaly/output` |

### Example
```ini
[workspace_config]
pipeline_dir_path=/home/user/petaly/pipelines
logs_dir_path=/home/user/petaly/logs
output_dir_path=/home/user/petaly/output
```

## Global Settings

The `[global_settings]` section controls the general behavior of Petaly.

### Parameters

| Parameter | Description | Default | Options | Example |
|-----------|-------------|---------|---------|---------|
| `logging_mode` | Controls the verbosity of logging output. Use DEBUG for troubleshooting and INFO for normal operation. | INFO | INFO, DEBUG | `logging_mode=DEBUG` |
| `pipeline_format` | Determines the format of pipeline configuration files. YAML is more readable, while JSON is better for integration with other tools. | yaml | yaml, json | `pipeline_format=json` |

### Example
```ini
[global_settings]
logging_mode=INFO
pipeline_format=yaml
```

## AI Settings

The `[ai_settings]` section configures the AI Agent mode functionality. This section is only required if you plan to use Petaly's AI capabilities.

### Parameters

| Parameter | Description | Default | Required | Example |
|-----------|-------------|---------|----------|---------|
| `llm_provider` | Specifies the AI model provider to use. | openai | Yes | `llm_provider=anthropic` |
| `llm_model` | The specific AI model to use for processing. | gpt-4 | Yes | `llm_model=claude-3-opus-20240229` |
| `agent_memory_file` | Path to the file where the AI agent stores its memory. | ~/.petaly/agent_memory.json | No | `agent_memory_file=/home/user/.petaly/memory.json` |
| `ai_agent_api_key` | API key for the AI provider. Can be set via environment variable instead. | None | Yes* | `ai_agent_api_key=your-api-key-here` |

*Note: The API key can be provided either in the config file or as an environment variable `AI_AGENT_API_KEY`

### Example
```ini
[ai_settings]
llm_provider=openai
llm_model=gpt-4
agent_memory_file=~/.petaly/agent_memory.json
ai_agent_api_key=your-api-key-here
```

## Complete Example

Here's a complete example of a `petaly.ini` file:

```ini
[workspace_config]
pipeline_dir_path=/home/user/petaly/pipelines
logs_dir_path=/home/user/petaly/logs
output_dir_path=/home/user/petaly/output

[global_settings]
logging_mode=INFO
pipeline_format=yaml

[ai_settings]
llm_provider=openai
llm_model=gpt-4
agent_memory_file=~/.petaly/agent_memory.json
ai_agent_api_key=your-api-key-here
```

## Best Practices

1. **Security**
   - Store sensitive information like API keys in environment variables
   - Use appropriate file permissions for the config file
   - Example: `export AI_AGENT_API_KEY="your-key-here"`

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

## Environment Variables

Petaly supports the following environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `PETALY_CONFIG_DIR` | Directory containing the `petaly.ini` file | `export PETALY_CONFIG_DIR=/home/user/.petaly` |
| `AI_AGENT_API_KEY` | API key for AI provider | `export AI_AGENT_API_KEY="your-key-here"` |

## Troubleshooting

1. **Configuration Not Found**
   - Ensure `PETALY_CONFIG_DIR` is set correctly
   - Verify the config file exists in the specified directory
   - Check file permissions

2. **Directory Access Issues**
   - Verify all paths are absolute
   - Check directory permissions
   - Ensure sufficient disk space

3. **AI Agent Issues**
   - Verify API key is set correctly
   - Check internet connectivity
   - Ensure the specified model is available

## Related Topics

- [Pipeline Configuration](pipeline_examples.md)
- [Data Objects Specification](data_objects_spec.md)
- [Error Messages](error_messages.md)
- [Source and Target Attributes](source_target_attributes.md)
- [Installation Guide](installation.md)

## Additional Resources

- [Pipeline Configuration Guide](../pipeline_examples.md)
- [AI Agent Mode Guide](ai_agent_mode.md)
- [Video Tutorials](recording/) 