# Source and Target Attributes Guide

This guide provides detailed configuration options for each endpoint in Petaly, specifying their usage as source, target, or both.

## PostgreSQL

Can be used as both source and target. The configuration parameters are identical for both source and target.

```yaml
source_attributes:  # or target_attributes
  connector_type: postgres
  database_user: your_username
  database_password: your_password
  database_host: localhost
  database_port: 5432
  database_name: your_database
  database_schema: your_schema  # Optional, defaults to 'public'
```

## MySQL

Can be used as both source and target. The configuration parameters are identical for both source and target.

```yaml
source_attributes:  # or target_attributes
  connector_type: mysql
  database_user: your_username
  database_password: your_password
  database_host: localhost
  database_port: 3306
  database_name: your_database
```

## Local CSV

Can be used as both source and target, but with different parameters.

### Source Attributes
```yaml
source_attributes:
  connector_type: csv
  # Note: File paths are specified in data_objects_spec
```

### Target Attributes
```yaml
target_attributes:
  connector_type: csv
  destination_dir: /path/to/output/directory
```

## BigQuery

Can be used as both source and target. The configuration parameters are identical for both source and target.

```yaml
source_attributes:  # or target_attributes
  connector_type: bigquery
  platform_type: gcp
  gcp_project_id: your-project-id
  gcp_region: your-region
  database_schema: your_dataset
  # Optional: Service account authentication
  gcp_service_account_key: /path/to/key.json
```

## Redshift

Can be used as both source and target. The configuration parameters are identical for both source and target, with additional parameters for target.

### Common Parameters (Source and Target)
```yaml
source_attributes:  # or target_attributes
  connector_type: redshift

  # For IAM authentication
  connection_method: iam
  is_serverless: false  # or true for Redshift Serverless
  cluster_identifier: your-cluster
  database_user: awsuser
  database_name: your_database
  database_schema: your_schema
  platform_type: aws
  aws_region: your-region
  aws_iam_role: arn:aws:iam::xxxxxxxx:role/YourRole
  aws_profile_name: your-aws-profile

  # OR for TCP authentication
  connection_method: tcp
  database_user: awsuser
  database_password: your_password
  database_host: your-cluster.xxxxx.region.redshift.amazonaws.com
  database_port: 5439
  database_name: your_database
  database_schema: your_schema
```

### Additional Target Parameters
```yaml
target_attributes:
  # Add these to the common parameters above
  aws_bucket_name: your-bucket
  bucket_pipeline_prefix: petaly/{pipeline_name}
```

## Google Cloud Storage (GCS)

Can be used as both source and target. The configuration parameters are identical for both source and target.

```yaml
source_attributes:  # or target_attributes
  connector_type: gcs
  platform_type: gcp
  gcp_project_id: your-project-id
  gcp_region: your-region
  gcp_bucket_name: your-bucket-name
  bucket_pipeline_prefix: petaly/{pipeline_name}
  # Optional: Service account authentication
  gcp_service_account_key: /path/to/key.json
```

## AWS S3

Can be used as both source and target. The configuration parameters are identical for both source and target.

```yaml
source_attributes:  # or target_attributes
  connector_type: s3
  platform_type: aws
  aws_bucket_name: your-bucket-name
  aws_region: your-region
  bucket_pipeline_prefix: petaly/{pipeline_name}
  aws_iam_role: arn:aws:iam::xxxxxxxx:role/YourRole
  aws_profile_name: your-aws-profile
```

## Common Notes

1. **Authentication**:
   - For cloud services, prefer IAM roles/service accounts over access keys
   - Store sensitive credentials in environment variables when possible
   - Use appropriate security groups and network access rules

2. **Performance**:
   - Use appropriate batch sizes for large datasets
   - Consider network bandwidth and latency
   - Use compression when appropriate

3. **Error Handling**:
   - Implement proper retry mechanisms
   - Monitor connection timeouts
   - Handle authentication failures gracefully

## Related Topics

- [Pipeline Configuration](pipeline_examples.md)
- [Cloud Platform Guide](cloud_platforms.md)
- [Error Messages](error_messages.md) 