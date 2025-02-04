## 7. More Pipeline Examples

#### MySQL to Postgres

The following example exports a table `stocks` from Mysql into PostgreSQL under the name `stocks_in_postgres`

```
pipeline:
  pipeline_attributes:
    pipeline_name: mysql2psql
    is_enabled: true
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
  data_attributes:
    data_objects_spec_mode: only
    object_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: double
---
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
  pipeline_attributes:
    pipeline_name: csv2mysql
    is_enabled: true
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: mysql
    database_user: root
    database_password: db-password
    database_host: localhost
    database_port: 3306
    database_name: petaly_tutorial
  data_attributes:
    use_data_objects_spec: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      columns_quote: none
---
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /your-path-to-csv-folder/stocks
    file_names:
    - 
- object_spec:
    object_name: options
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /your-directory-path-to-csv-files
    file_names:
    - options.csv
    - options2.csv
       
```


#### Postgres to CSV

The following example exports tables **stocks*** and ***users** from Postgres into destination folder `destination_dir: /your-path-to-destination-folder`
It also exclude columns ***likebroadway***, ***likemusicals*** of table **users** from export.

```
pipeline:
  pipeline_attributes:
    pipeline_name: psql2csv
    is_enabled: true
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
    destination_dir: /your-path-to-destination-folder
  data_attributes:
    data_objects_spec_mode: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      columns_quote: single
---
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
  pipeline_attributes:
    pipeline_name: bq2csv
    is_enabled: true
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
    destination_dir: /opt/petaly_labs/data/dest_data/
  data_attributes:
    data_objects_spec_mode: only
    object_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: single
---
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
  pipeline_attributes:
    pipeline_name: csv2bq
    is_enabled: true
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
  data_attributes:
    data_objects_spec_mode: only
    object_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: none
---
data_objects_spec:
- object_spec:
    object_name: osm_admin
    destination_object_name: osm_admin_csv
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /opt/petaly_labs/data/source_data/csv/test_data/osm_admin
    file_names:
    - osm_admin.csv.gz

```

#### CSV to GCS

```
pipeline:
  pipeline_attributes:
    pipeline_name: csv2gcs
    is_enabled: true
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: gcs
    platform_type: gcp
    gcp_project_id: 'my-project
    gcp_region: EU
    gcp_bucket_name: 'bucket-name'
    bucket_pipeline_prefix: petaly/{pipeline_name}
  data_attributes:
    data_objects_spec_mode: only
    object_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: none
---
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /opt/petaly_labs/data/source_data/csv/test_data/stocks
    file_names:
      - 2013-01-08stocks.csv
      - 2013-01-09stocks.csv

```

### AWS Redshift Cluster and Redshift Serverless

#### CSV to Redshift Cluster over IAM
```
pipeline:
  pipeline_attributes:
    pipeline_name: csv2rs_cluster_iam
    is_enabled: true
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
    aws_region:
  data_attributes:
    data_objects_spec_mode: only
    object_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: double
---
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /opt/petaly_labs/data/source_data/csv/test_data/stocks
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
  pipeline_attributes:
    pipeline_name: rs2csv
    is_enabled: true
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
    destination_dir: /opt/petaly_labs/data/dest_data
  data_attributes:
    data_objects_spec_mode: only
    object_default_settings:
      header: true
      columns_delimiter: '\t'
      columns_quote: none
---
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -

```