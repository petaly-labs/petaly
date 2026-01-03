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
- Check for required sections: pipeline_name, source_attributes, target_attributes
- Ensure all required parameters are present

### Connection Resolution Errors

#### Connection Not Found
```
Connection 'connection_name' not found in connections
```
**Cause**: The pipeline references a connection that doesn't exist in `connections.yaml`.
**Solution**:
- Verify the connection name in `connection_name` matches exactly with a connection in `connections.yaml`
- Check that `connections.yaml` exists in the workspace directory
- Create the connection if it doesn't exist:
  ```bash
  python3 -m petaly init -e connection_name
  ```

#### Missing connections.yaml
```
Could not resolve source/target connection. The connection does not exist in connections.yaml.
```
**Cause**: Pipeline uses `connection_name` but `connections.yaml` file doesn't exist or is empty.
**Solution**:
- Create `connections.yaml` file in your workspace directory
- Or switch to inline attributes in the pipeline configuration
- Initialize connections file:
  ```bash
  python3 -m petaly init --workspace
  ```

### Pipeline Disabled
```
The pipeline: {pipeline_name} is disabled. To enable pipeline {pipeline_dpath} set the parameter is_enabled: true
```
**Cause**: This error is deprecated. Pipelines are now always enabled by default.
**Solution**:
- The `is_enabled` parameter has been removed. Pipelines are always enabled.
- If you see this error, update your pipeline configuration to remove the `is_enabled` parameter.

## Data Objects Specification Errors

### Missing Object Specification
```
For {connector_type} extract the parameters include_data_objects=spec and specification in the data_objects_spec[] are required.
```
**Cause**: Required object specifications are missing when using `include_data_objects: "spec"`.
**Solution**:
- Add object specifications to the data_objects_spec section
- Use the command: `python -m petaly init -p {pipeline_name} --object_name table1,table2 -c your_config_dir/petaly.ini`
- Or change `include_data_objects` to `"all"` to load all objects from the schema

### CSV Source Configuration Error
```
In case your source is csv, the parameters include_data_objects should be set to spec and require the specification in the data_objects_spec[].
```
**Cause**: CSV sources require explicit object specifications.
**Solution**:
- Set `include_data_objects: "spec"`
- Add required object specifications including:
  - object_name
  - object_source_dir (or source_dir in source_attributes)
  - file_names (optional, if not specified all files in directory will be processed)

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