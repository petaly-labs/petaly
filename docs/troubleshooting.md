# Troubleshooting Guide

This guide provides solutions for common issues you might encounter while using Petaly.

## Installation Issues

### Python Version Compatibility
**Issue**: Installation fails with Python version error
**Solution**: Ensure you're using Python 3.10 - 3.12
```bash
python3 --version
```

### Virtual Environment Issues
**Issue**: Package not found after installation
**Solution**: Ensure virtual environment is activated
```bash
source .venv/bin/activate
```

## Configuration Issues

### petaly.ini Problems

#### Missing Configuration File
**Issue**: "Configuration file not found"
**Solution**: Initialize configuration
```bash
python3 -m petaly -c /path/to/petaly.ini init
```

#### Invalid Paths
**Issue**: "Invalid path in configuration"
**Solution**: Ensure all paths in petaly.ini are absolute
```ini
pipeline_dir_path=/absolute/path/to/pipelines
logs_dir_path=/absolute/path/to/logs
output_dir_path=/absolute/path/to/output
```

## Pipeline Issues

### Connection Problems

#### Database Connection Failures
**Issue**: "Could not connect to database"
**Solutions**:
1. Verify credentials
2. Check network connectivity
3. Ensure database is running
4. Verify port accessibility

#### Cloud Service Connection Issues
**Issue**: "Failed to connect to cloud service"
**Solutions**:
1. Verify credentials/API keys
2. Check IAM roles and permissions
3. Ensure proper SDK installation
4. Verify network connectivity

### Data Transfer Issues

#### CSV File Problems
**Issue**: "Error reading CSV file"
**Solutions**:
1. Verify file exists and is readable
2. Check CSV format matches configuration
3. Ensure proper delimiter and quote settings
4. Verify file encoding

#### Database Transfer Issues
**Issue**: "Error during data transfer"
**Solutions**:
1. Check table schemas match
2. Verify column data types
3. Ensure sufficient permissions
4. Check for NULL value handling

## Performance Issues

### Slow Data Transfer
**Solutions**:
1. Adjust batch sizes
2. Optimize network connection
3. Check system resources
4. Consider using bulk operations

### Memory Issues
**Solutions**:
1. Reduce batch size
2. Monitor system memory
3. Optimize query performance
4. Use streaming for large datasets

## Logging and Debugging

### Enable Debug Mode
Set in petaly.ini:
```ini
[global_settings]
logging_mode = DEBUG
```

### Common Log Messages

#### Connection Errors
```
ERROR: Failed to connect to database
```
- Check credentials
- Verify network
- Ensure service is running

#### Data Transfer Errors
```
ERROR: Failed to transfer data
```
- Check data types
- Verify permissions
- Review error details

## Cloud Platform Specific Issues

### AWS Issues

#### S3 Access Problems
**Issue**: "Failed to access S3 bucket"
**Solutions**:
1. Verify IAM role permissions
2. Check bucket policy
3. Ensure proper AWS credentials
4. Verify bucket exists

#### Redshift Connection Issues
**Issue**: "Failed to connect to Redshift"
**Solutions**:
1. Check cluster status
2. Verify security group settings
3. Ensure proper IAM role
4. Check network access

### GCP Issues

#### BigQuery Access Problems
**Issue**: "Failed to access BigQuery"
**Solutions**:
1. Verify service account permissions
2. Check project access
3. Ensure proper authentication
4. Verify dataset exists

#### GCS Bucket Issues
**Issue**: "Failed to access GCS bucket"
**Solutions**:
1. Check bucket permissions
2. Verify service account access
3. Ensure proper authentication
4. Check bucket exists

## Getting Help

If you're still experiencing issues:

1. Check the [GitHub Issues](https://github.com/petaly-labs/petaly/issues)
2. Review the [Documentation](./index.md)
3. Join our [Community](https://github.com/petaly-labs/petaly/discussions)
4. Submit a new issue with:
   - Error message
   - Configuration details
   - Steps to reproduce
   - System information

## Related Topics

- [Error Messages](error_messages.md)
- [Pipeline Configuration](pipeline_examples.md)
- [Configuration Guide](petaly_ini.md) 