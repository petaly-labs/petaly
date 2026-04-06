# Pipeline Configuration Guide

This guide provides detailed information about configuring Petaly pipelines, including examples for different data sources and targets.

## Pipeline Structure

A Petaly pipeline configuration consists of two main sections: pipeline and data_objects_spec. View a skeleton example below:

```yaml
pipeline:
  pipeline_name: my_pipeline
  source_attributes:
    ...
  target_attributes:
    ...
  load_attributes:
    ...
data_objects_spec: []
```

## Configuration Blocks

### Pipeline Name
```yaml
pipeline:
  # Unique pipeline name (directly under pipeline, no nested section)
  pipeline_name: my_pipeline
```

## Connection Configuration

Petaly uses separated reusable endpoint definitions for source and target connections.

### Using connections.yaml or connections.json

**Benefits:**
- Reusable connection configurations across multiple pipelines
- Centralized credential management
- Easier maintenance and updates
- Pipeline-specific attributes can override connection defaults
- Explicit separation between readable source endpoints and writable target endpoints

**connections.yaml Structure:**

For a complete template with examples of all connector types, see [connections.yaml-template](connections.yaml-template).

**Basic Example:**
```yaml
connections:
  postgres_prod:
    connector_type: postgres
    endpoint_type: target
    database_user: prod_user
    database_password: ${DB_PASSWORD}
    database_host: prod-db.example.com
    database_port: 5432
    database_name: production_db
  
  mysql_dev:
    connector_type: mysql
    endpoint_type: source
    database_user: dev_user
    database_password: dev_password
    database_host: localhost
    database_port: 3306
    database_name: dev_db
  
  bigquery_analytics:
    connector_type: bigquery
    endpoint_type: target
    platform_type: gcp
    gcp_project_id: my-project-id
    gcp_region: US
```

**Using Connections in Pipeline:**
```yaml
pipeline:
  pipeline_name: my_pipeline
  source_attributes:
    connection_name: mysql_dev  # Reference to connection in connections.yaml
    database_schema: my_schema   # Pipeline-specific override
  
  target_attributes:
    connection_name: postgres_prod
    database_schema: analytics    # Pipeline-specific override
```

**Key Points:**
- `connection_name` references a connection defined in `connections.yaml`
- Pipeline-specific attributes (like `database_schema`, `bucket_pipeline_prefix`) can be added to override connection defaults
- Connections are stored at the workspace level (`pipeline_base_dpath/connections.yaml`)
- The connections file format matches the pipeline format (YAML or JSON)
- Reusable endpoints should declare `endpoint_type: source` or `endpoint_type: target`
- If a source endpoint is referenced under `target_attributes`, Petaly rejects the pipeline to protect the source from write operations

**Recommended approach:**
- Define reusable source and target endpoints in `connections.yaml` or `connections.json`
- Reference them from the pipeline with `connection_name`
- Use `endpoint_type` to declare whether the endpoint is `source` or `target`

### Source Attributes
In active pipeline configurations, `source_attributes` should normally contain a `connection_name` plus optional pipeline-specific overrides.

#### Source Reference Example
```yaml
source_attributes:
  connection_name: mysql_dev
  database_schema: source_schema
```

### Target Attributes
In active pipeline configurations, `target_attributes` should normally contain a `connection_name` plus optional pipeline-specific overrides.

#### Target Reference Example
```yaml
target_attributes:
  connection_name: postgres_prod
  database_schema: analytics
```

### Load Attributes
```yaml
load_attributes:
  # Mode for handling data objects
  all_from_schema: false          # Load only objects from data_objects_spec (false) or all from schema (true)
  use_data_objects_spec: true    # Apply specifications from data_objects_spec if they exist (true) or ignore them (false)
  
  # Default settings for CSV/TSV/TXT file processing
  csv_default_settings:
    header: true
    columns_delimiter: ","
    columns_quote: double  # Options: double, single, none
    type_autodetection: true  # Automatically detect column types
```

## Data Objects Specification

The `data_objects_spec` section defines how to handle specific data objects (tables/files):

```yaml
data_objects_spec:
- object_spec:
    object_name: source_table
    destination_object_name: target_table  # Optional
    recreate_destination_object: true      # Optional
    cleanup_linebreak_in_fields: false     # Optional
    exclude_columns:                       # Optional
      - column1
      - column2
    object_source_dir: incoming/files      # For file sources with source_base_dir
    file_names:                            # For CSV sources
      - file1.csv
      - file2.csv
```

## Cloud Platform Configuration

### GCP Configuration
```yaml
target_attributes:
  platform_type: gcp
  connector_type: bigquery  # or gcs
  gcp_project_id: your-project-id
  gcp_region: your-region
  gcp_bucket_name: your-bucket-name
  bucket_pipeline_prefix: petaly/{pipeline_name}
```

### AWS Configuration
```yaml
target_attributes:
  platform_type: aws
  connector_type: redshift  # or s3
  aws_bucket_name: bucket-name
  aws_iam_role: arn:aws:iam::xxxxxxxx:role/YourRedshiftRole
  aws_profile_name: your-aws-profile
  aws_region: eu-north-1
```

## Complete Examples

### CSV to PostgreSQL

**Example 1: Using connections.yaml (Recommended)**
```yaml
# connections.yaml
connections:
  postgres_target:
    connector_type: postgres
    endpoint_type: target
    database_user: root
    database_password: db-password
    database_host: localhost
    database_port: 5432
    database_name: petalydb

# pipeline.yaml
pipeline:
  pipeline_name: csv_to_postgres
  source_attributes:
    connector_type: csv
  target_attributes:
    connection_name: postgres_target
    database_schema: petaly_tutorial  # Pipeline-specific override
```

**Example 2: Using inline attributes**
```yaml
pipeline:
  pipeline_name: csv_to_postgres
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: postgres
    database_user: root
    database_password: db-password
    database_host: localhost
    database_port: 5432
    database_name: petalydb
    database_schema: petaly_tutorial
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ","
      columns_quote: none

data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name: stocks_new
    recreate_destination_object: true
    object_source_dir: stocks
    file_names:
      - stocks.csv
- object_spec:
    object_name: options
    destination_object_name: options_new
    recreate_destination_object: true
    object_source_dir: options
    file_names:
      - options.csv
```

### Deprecated Inline Example: MySQL to PostgreSQL
```yaml
pipeline:
  pipeline_name: mysql_to_postgres
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

data_objects_spec:
- object_spec:
    object_name: customers
    destination_object_name: customers_new
    recreate_destination_object: true
    exclude_columns:
      - created_at
      - updated_at
```

### Deprecated Inline Example: MySQL to PostgreSQL with Incremental Load
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

data_objects_spec:
- object_spec:
    object_name: users
    extract_load_mode: incremental
    column_primary_key: user_id
    column_last_modified: updated_at
    batch_size: 10000
- object_spec:
    object_name: orders
    extract_load_mode: incremental
    column_primary_key: order_id
    column_last_modified: modified_at
    batch_size: 5000
```

**Note:** Incremental load is only supported for MySQL and PostgreSQL sources. The `load_state.json` file is automatically created in `{output_dir_path}/{pipeline_name}/{object_name}/metadata/` to track the last loaded timestamp and enable resumable loads.

These inline endpoint examples are deprecated. Prefer reusable endpoint definitions in `connections.yaml` or `connections.json`.

## Best Practices

1. **Security**:
   - Store sensitive credentials in environment variables
   - Use IAM roles for cloud services when possible
   - Avoid hardcoding passwords in configuration files

2. **Performance**:
   - Use appropriate batch sizes for large datasets
   - Consider using `recreate_destination_object` for fresh loads
   - Use `exclude_columns` to minimize data transfer

3. **Maintenance**:
   - Use meaningful pipeline names
   - Document pipeline purposes in comments
   - Keep configurations in version control

## Troubleshooting

Common issues and solutions:

1. **Connection Issues**:
   - Verify network connectivity
   - Check credentials and permissions
   - Ensure ports are open

2. **Data Type Mismatches**:
   - Review source and target schemas
   - Use appropriate data type mappings
   - Handle NULL values appropriately

3. **Performance Issues**:
   - Check batch sizes
   - Monitor system resources
   - Optimize query performance

For more detailed troubleshooting, see our [Troubleshooting Guide](troubleshooting.md).

## 7. More Deprecated Inline Pipeline Examples

#### MySQL to Postgres

The following example exports a table `stocks` from Mysql into PostgreSQL under the name `stocks_in_postgres`

```
pipeline:
  pipeline_name: mysql2psql
  source_attributes:
    connector_type: mysql
    database_user: root
    database_password: db-password
    database_host: localhost
    database_port: 3306
    database_name: petaly_tutorial
  target_attributes:
    connector_type: postgres
    database_user: postgres
    database_password: db-password
    database_host: localhost
    database_port: 5432
    database_name: petalydb
    database_schema: petaly_tutorial
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: double

data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name: stocks_in_postgres
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    
```

#### CSV to MySQL

The following example create a new table and load csv file stocks.csv into Mysql database.

```
pipeline:
  pipeline_name: csv2mysql
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: mysql
    database_user: root
    database_password: db-password
    database_host: localhost
    database_port: 3306
    database_name: petaly_tutorial
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ","
      columns_quote: none

data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: stocks
    file_names:
    - 
- object_spec:
    object_name: options
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: options
    file_names:
    - options.csv
    - options2.csv
       
```


#### Postgres to CSV

The following example exports tables **stocks*** and ***users** from Postgres into target base directory `target_base_dir: /your-path-to-destination-folder`
It also exclude columns ***likebroadway***, ***likemusicals*** of table **users** from export.

```
pipeline:
  pipeline_name: psql2csv
  source_attributes:
    connector_type: postgres
    database_user: postgres
    database_password: db-password
    database_host: localhost
    database_port: 5432
    database_name: petalydb
    database_schema: petaly_schema
  target_attributes:
    connector_type: csv
    target_base_dir: /your-path-to-destination-folder
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ","
      columns_quote: single

data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name: stocks_as_csv
    recreate_destination_object: true
    cleanup_linebreak_in_fields: true
    exclude_columns:
    - 
- object_spec:
    object_name: users
    destination_object_name: users_as_csv
    recreate_destination_object: true
    cleanup_linebreak_in_fields: true
    exclude_columns:
    - likebroadway
    - likemusicals
    
```

#### BigQuery to CSV

In this example, the pipeline bq2csv extracts the table osm_admin from BigQuery and downloads it as a CSV file to the local machine.

```
pipeline:
  pipeline_name: bq2csv
  source_attributes:
    connector_type: bigquery
    database_schema: petaly_tutorial
    platform_type: gcp
    gcp_project_id: 'my-project'
    gcp_region: EU
    gcp_bucket_name: 'bucket-name'
    bucket_pipeline_prefix: petaly/{pipeline_name}
  target_attributes:
    connector_type: csv
    target_base_dir: /opt/petaly_labs/data/dest_data/
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: single

data_objects_spec:
- object_spec:
    object_name: osm_admin
    destination_object_name:
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    
``` 

#### CSV to BigQuery

```
pipeline:
  pipeline_name: csv2bq
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: bigquery
    database_schema: petaly_tutorial
    platform_type: gcp
    gcp_project_id: 'my-project'
    gcp_region: EU
    gcp_bucket_name: 'bucket-name'
    bucket_pipeline_prefix: petaly/{pipeline_name}
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: none

data_objects_spec:
- object_spec:
    object_name: osm_admin
    destination_object_name: osm_admin_csv
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: test_data/osm_admin
    file_names:
    - osm_admin.csv.gz

```

#### CSV to GCS

```
pipeline:
  pipeline_name: csv2gcs
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: gcs
    platform_type: gcp
    gcp_project_id: 'my-project
    gcp_region: EU
    gcp_bucket_name: 'bucket-name'
    bucket_pipeline_prefix: petaly/{pipeline_name}
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: none

data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: test_data/stocks
    file_names:
      - 2013-01-08stocks.csv
      - 2013-01-09stocks.csv

```

### AWS Redshift Cluster and Redshift Serverless

#### CSV to Redshift Cluster over IAM
```
pipeline:
  pipeline_name: csv2rs_cluster_iam
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: redshift
    connection_method: iam
    is_serverless: 'false'
    cluster_identifier: rs-cluster
    database_user: awsuser
    database_name: dev
    database_schema: schema-name
    platform_type: aws
    aws_bucket_name: 'bucket-name'
    bucket_pipeline_prefix: petaly/{pipeline_name}
    aws_iam_role: 'arn:aws:iam::xxxxxxxxxxxx:role/YourRedshiftRole'
    aws_profile_name: 'your-aws-profile'
    aws_access_key_id:
    aws_secret_access_key:
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: double

data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: test_data/stocks
    file_names:
    -

```
#### CSV to Redshift Serverless over IAM
Target-Attribute
```
  target_attributes:
    connector_type: redshift
    connection_method: 'iam'
    is_serverless: true
    cluster_identifier: 'default-workgroup'
    database_user: awsuser
    database_name: dev
    database_schema: your-schema
    workgroup_name: 'default-workgroup'
    platform_type: aws
    aws_region: 'eu-north-1'
    aws_bucket_name: 'your-bucket'
    bucket_pipeline_prefix: petaly/{pipeline_name}
    aws_iam_role: 'arn:aws:iam::xxxxxxxxxxxx:role/YourRedshiftRole'
    aws_profile_name: 'your-aws-profile'
    aws_access_key_id:
    aws_secret_access_key:

```
#### CSV to Redshift Cluster over tcp
Target-Attribute
```
  target_attributes:
    connector_type: redshift
    connection_method: tcp
    database_user: awsuser
    database_password: 'db-password'
    database_host: redshift-host
    database_port: 5439
    database_name: dev
    database_schema: public
    platform_type: aws
    aws_bucket_name: 'bucket-name'
    bucket_pipeline_prefix: petaly/{pipeline_name}
    aws_iam_role: 'arn:aws:iam::xxxxxxxx:role/YourRedshiftRole'
    aws_profile_name: 'your-aws-profile'
    aws_access_key_id:
    aws_secret_access_key:
    aws_region: 'eu-north-1'

```

#### CSV to Redshift Serverless over IAM 
Target-Attribute

``` 
  target_attributes:
    connector_type: redshift
    connection_method: 'iam'
    is_serverless: true
    cluster_identifier: 'default-workgroup'
    database_user: 'awsuser'
    database_name: dev
    database_schema: your-schema
    workgroup_name: 'default-workgroup'
    platform_type: aws
    aws_region: 'eu-north-1'
    aws_bucket_name: 'your-bucket'
    bucket_pipeline_prefix: petaly/{pipeline_name}
    aws_iam_role: 'arn:aws:iam::xxxxxxxxxxxx:role/YourRedshiftRole'
    aws_profile_name: 'your-aws-profile'
    aws_access_key_id:
    aws_secret_access_key:

```
#### Redshift Serverless over iam to CSV

```
pipeline:
  pipeline_name: rs2csv
  source_attributes:
    connector_type: redshift
    connection_method: 'iam'
    is_serverless: true
    cluster_identifier: 'default-workgroup'
    database_user: 'awsuser'
    database_name: dev
    database_schema: your-schema
    workgroup_name: 'default-workgroup'
    platform_type: aws
    aws_region: 'eu-north-1'
    aws_bucket_name: 'your-bucket'
    bucket_pipeline_prefix: petaly/{pipeline_name}
    aws_iam_role: 'arn:aws:iam::xxxxxxxxxxxx:role/YourRedshiftRole'
    aws_profile_name: 'your-aws-profile'
    aws_access_key_id:
    aws_secret_access_key:
  target_attributes:
    connector_type: csv
    target_base_dir: /opt/petaly_labs/data/dest_data
  load_attributes:
    all_from_schema: false
    use_data_objects_spec: true
    csv_default_settings:
      header: true
      columns_delimiter: '\t'
      columns_quote: none

data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -

```

## Related Topics

- [Data Objects Specification](data_objects_spec.md)
- [Configuration Guide](petaly_ini.md)
- [Error Messages](error_messages.md)
- [Source and Target Attributes](source_target_attributes.md)
- [Installation Guide](installation.md)
