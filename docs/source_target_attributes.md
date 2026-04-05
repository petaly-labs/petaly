# Source and Target Attributes Guide

This guide provides detailed configuration options for each endpoint in Petaly. Reusable endpoint definitions should be marked as either source or target.

## Connection Configuration

Petaly uses reusable endpoint definitions for source and target connections.

### Using connections.yaml or connections.json

Define reusable connections in `connections.yaml` and reference them in pipelines.

**Template File:** See [connections.yaml-template](connections.yaml-template) for a complete example with all connector types.

**In connections.yaml:**
```yaml
connections:
  my_postgres_conn:
    connector_type: postgres
    endpoint_type: source
    database_user: user
    database_password: password
    database_host: localhost
    database_port: 5432
    database_name: mydb
```

**In pipeline.yaml:**
```yaml
source_attributes:
  connection_name: my_postgres_conn  # Reference to connection
  database_schema: public            # Pipeline-specific override
```

**Note:** When using `connection_name`, pipeline-specific attributes (like `database_schema`, `bucket_pipeline_prefix`) can override connection defaults. All other attributes from the connection are inherited.

### Endpoint Role Protection

Reusable endpoint definitions can declare an `endpoint_type`:

```yaml
connections:
  prod_source_db:
    connector_type: postgres
    endpoint_type: source

  analytics_target_db:
    endpoint_type: target
    connector_type: postgres

```

Rules:
- `source`: can only be used as a pipeline source
- `target`: can only be used as a pipeline target

If a connection marked as `source` is used as a target, Petaly rejects the pipeline to protect the source endpoint from write operations.
If `endpoint_type` is missing on an older connection, Petaly still accepts it for backward compatibility, but new endpoint definitions should always declare either `source` or `target`.

## Deprecated Inline Endpoint Definition

Older pipelines may still define source and target endpoints directly in `pipeline.yaml`, for example:

```yaml
pipeline:
  pipeline_name: mysql_to_postgres_incremental
  source_attributes:
    connector_type: mysql
    database_user: root
    database_password: dbpassword
    database_host: localhost
    database_port: 3306
    database_name: source_db
  target_attributes:
    connector_type: postgres
    database_user: postgres
    database_password: dbpassword
    database_host: localhost
    database_port: 5432
    database_name: target_db
    database_schema: public
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
```

This style is deprecated. Prefer reusable endpoint definitions in `connections.yaml` or `connections.json`.

## BigQuery

Can be used as both source and target. The configuration parameters are identical for both source and target.

```yaml
source_attributes:  # or target_attributes
  connector_type: bigquery
  platform_type: gcp
  gcp_project_id: your-project-id
  gcp_region: your-region
  database_schema: your_dataset
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
