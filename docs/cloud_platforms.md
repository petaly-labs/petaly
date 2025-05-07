# Cloud Platform Configuration Guide

This guide provides detailed instructions for configuring Petaly to work with cloud platforms like AWS and GCP.

## AWS Configuration

### Prerequisites

1. Install AWS CLI:
```bash
# macOS
brew install awscli

# Linux
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

2. Configure AWS credentials:
```bash
aws configure
```

### S3 Configuration

#### Basic S3 Setup
```yaml
target_attributes:
  platform_type: aws
  connector_type: s3
  aws_bucket_name: your-bucket-name
  aws_region: your-region
  bucket_pipeline_prefix: petaly/{pipeline_name}
```

#### IAM Role Configuration
```yaml
target_attributes:
  aws_iam_role: arn:aws:iam::xxxxxxxx:role/YourRole
  aws_profile_name: your-aws-profile
```

### Redshift Configuration

#### Redshift Cluster
```yaml
target_attributes:
  platform_type: aws
  connector_type: redshift
  database_host: your-cluster.xxxxx.region.redshift.amazonaws.com
  database_port: 5439
  database_name: your_database
  database_user: your_username
  database_password: your_password
  aws_region: your-region
```

#### Redshift Serverless
```yaml
target_attributes:
  platform_type: aws
  connector_type: redshift
  database_host: your-workgroup.xxxxx.region.redshift-serverless.amazonaws.com
  database_port: 5439
  database_name: your_database
  database_user: your_username
  database_password: your_password
  aws_region: your-region
```

## GCP Configuration

### Prerequisites

1. Install Google Cloud SDK:
```bash
# macOS
brew install google-cloud-sdk

# Linux
curl https://sdk.cloud.google.com | bash
```

2. Initialize and authenticate:
```bash
gcloud init
gcloud auth application-default login
```

### BigQuery Configuration

#### Basic Setup
```yaml
target_attributes:
  platform_type: gcp
  connector_type: bigquery
  gcp_project_id: your-project-id
  gcp_region: your-region
  database_schema: your_dataset
```

#### Service Account Authentication
1. Create service account key:
```bash
gcloud iam service-accounts keys create key.json --iam-account=your-service-account@your-project.iam.gserviceaccount.com
```

2. Set environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json
```

### Google Cloud Storage Configuration

#### Basic GCS Setup
```yaml
target_attributes:
  platform_type: gcp
  connector_type: gcs
  gcp_project_id: your-project-id
  gcp_region: your-region
  gcp_bucket_name: your-bucket-name
  bucket_pipeline_prefix: petaly/{pipeline_name}
```

## Security Best Practices

### AWS Security

1. **IAM Roles**:
   - Use IAM roles instead of access keys
   - Follow principle of least privilege
   - Regularly rotate credentials

2. **S3 Security**:
   - Enable bucket encryption
   - Use bucket policies
   - Enable versioning if needed

3. **Redshift Security**:
   - Use VPC security groups
   - Enable encryption
   - Use IAM authentication

### GCP Security

1. **Service Accounts**:
   - Use service accounts with minimal permissions
   - Regularly rotate keys
   - Use workload identity when possible

2. **Storage Security**:
   - Enable bucket encryption
   - Use IAM policies
   - Enable versioning if needed

3. **BigQuery Security**:
   - Use dataset-level permissions
   - Enable column-level security
   - Use service account authentication

## Performance Optimization

### AWS Optimization

1. **S3 Performance**:
   - Use multipart uploads
   - Enable transfer acceleration
   - Use appropriate storage class

2. **Redshift Performance**:
   - Use appropriate node type
   - Optimize distribution keys
   - Use compression

### GCP Optimization

1. **BigQuery Performance**:
   - Use appropriate table partitioning
   - Optimize query patterns
   - Use appropriate pricing model

2. **GCS Performance**:
   - Use appropriate storage class
   - Enable caching
   - Use parallel uploads

## Monitoring and Logging

### AWS Monitoring

1. **CloudWatch**:
   - Set up metrics
   - Configure alarms
   - Monitor costs

2. **Redshift Monitoring**:
   - Monitor query performance
   - Track storage usage
   - Monitor cluster health

### GCP Monitoring

1. **Cloud Monitoring**:
   - Set up metrics
   - Configure alerts
   - Monitor costs

2. **BigQuery Monitoring**:
   - Monitor query performance
   - Track storage usage
   - Monitor job status

## Troubleshooting

### Common AWS Issues

1. **S3 Access**:
   - Check IAM permissions
   - Verify bucket policy
   - Check network access

2. **Redshift Connection**:
   - Verify security groups
   - Check IAM roles
   - Verify network access

### Common GCP Issues

1. **BigQuery Access**:
   - Check service account permissions
   - Verify project access
   - Check API enablement

2. **GCS Access**:
   - Check bucket permissions
   - Verify service account
   - Check API enablement

For more detailed troubleshooting, see our [Troubleshooting Guide](troubleshooting.md).

## Related Topics

- [Pipeline Configuration](pipeline_examples.md)
- [Configuration Guide](petaly_ini.md)
- [Error Messages](error_messages.md)
- [Source and Target Attributes](source_target_attributes.md) 