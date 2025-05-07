# Error Messages Guide

This guide explains common error messages in Petaly and how to resolve them.

## Pipeline Configuration Errors

### Missing Pipeline File
```
Pipeline file not found at {pipeline_fpath} or {alt_fpath}
```
**Cause**: The pipeline configuration file is missing or not in the expected location.
**Solution**: 
- Check if the pipeline file exists in the correct directory
- Verify the pipeline name is correct
- Ensure the file has the correct extension (.yaml or .json)

### Invalid Pipeline Configuration
```
The pipeline: {pipeline_name} does not exist under: {pipeline_fpath}
```
**Cause**: The pipeline configuration is invalid or missing required sections.
**Solution**:
- Verify the pipeline configuration structure
- Check for required sections: pipeline_attributes, source_attributes, target_attributes
- Ensure all required parameters are present

### Pipeline Disabled
```
The pipeline: {pipeline_name} is disabled. To enable pipeline {pipeline_dpath} set the parameter is_enabled: true
```
**Cause**: The pipeline is explicitly disabled in the configuration.
**Solution**:
- Set `is_enabled: true` in the pipeline_attributes section
- Review the pipeline configuration to ensure it's ready for execution

## Data Objects Specification Errors

### Missing Object Specification
```
For {connector_type} extract the parameters data_objects_spec_mode=only and specification in the data_objects_spec[] are required.
```
**Cause**: Required object specifications are missing when using `data_objects_spec_mode=only`.
**Solution**:
- Add object specifications to the data_objects_spec section
- Use the command: `python -m petaly init -p {pipeline_name} --object_name table1,table2 -c your_config_dir/petaly.ini`

### CSV Source Configuration Error
```
In case your source is csv, the parameters data_objects_spec_mode should be set to only and require the specification in the data_objects_spec[].
```
**Cause**: CSV sources require explicit object specifications.
**Solution**:
- Set `data_objects_spec_mode: only`
- Add required object specifications including:
  - object_name
  - object_source_dir
  - file_names

### Missing Source Directory
```
Incorrect object specification in file: {pipeline_fpath}
data_objects_spec:
- object_spec:
    object_name: {object_name}
    object_source_dir: IS EMPTY
```
**Cause**: The object_source_dir parameter is missing or empty for CSV sources.
**Solution**:
- Add the object_source_dir parameter
- Ensure the directory path is valid and accessible

### Cloud Storage Configuration Error
```
Incorrect source or object specification in file: {pipeline_fpath}
Either bucket_pipeline_prefix in source_attributes or object_source_dir in object_spec, or both, must be specified and cannot be empty.
```
**Cause**: Missing required cloud storage configuration parameters.
**Solution**:
- Specify either bucket_pipeline_prefix in source_attributes
- Or object_source_dir in object_spec
- Or both for more specific control

## Command Line Interface Errors

### Invalid Arguments
```
init requires either --workspace or --pipeline_name
```
**Cause**: Missing required command line arguments.
**Solution**:
- Provide either --workspace or --pipeline_name
- Check command syntax: `python -m petaly init --help`

### Conflicting Arguments
```
Cannot specify both --source_only and --target_only
```
**Cause**: Mutually exclusive arguments were provided.
**Solution**:
- Choose either --source_only or --target_only
- Not both

### Missing Object Name
```
--object_name requires --pipeline_name
```
**Cause**: Object name was specified without a pipeline name.
**Solution**:
- Provide both --pipeline_name and --object_name
- Or use the interactive mode to specify these values

## AI Agent Errors

### Pipeline Creation Error
```
I encountered an error while trying to create the pipeline: {error}
```
**Cause**: Error during pipeline creation by the AI agent.
**Solution**:
- Check the error message for specific details
- Verify all required parameters are provided
- Ensure the AI agent has necessary permissions

### Pipeline Modification Error
```
I encountered an error while trying to modify the pipeline: {error}
```
**Cause**: Error during pipeline modification by the AI agent.
**Solution**:
- Review the modification request
- Check if the pipeline exists
- Verify the changes are valid

### Pipeline Execution Error
```
I encountered an error while trying to run the pipeline: {error}
```
**Cause**: Error during pipeline execution.
**Solution**:
- Check pipeline configuration
- Verify source and target connections
- Review object specifications

## Best Practices for Error Handling

1. **Check Logs**
   - Review the log files in the logs directory
   - Look for detailed error messages and stack traces
   - Check for related warnings that might indicate the root cause

2. **Validate Configuration**
   - Use the `petaly init` command to validate pipeline configuration
   - Check all required parameters are present
   - Verify paths and connections are correct

3. **Test Incrementally**
   - Test source connection first
   - Verify object specifications
   - Test target connection
   - Run pipeline with a small subset of data

4. **Use Debug Mode**
   - Enable debug logging for more detailed information
   - Set `logging_mode: DEBUG` in petaly.ini
   - Review debug logs for additional context

## Related Topics

- [Pipeline Configuration](pipeline_examples.md)
- [Data Objects Specification](data_objects_spec.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Source and Target Attributes](source_target_attributes.md) 