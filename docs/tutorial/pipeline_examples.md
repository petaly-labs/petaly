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

The following example exports tables **stocks*** and ***users** from Postgres into destination folder `destination_file_dir: /your-path-to-destination-folder`
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
    destination_file_dir: /your-path-to-destination-folder
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
  target_attributes:
    connector_type: csv
    destination_file_dir: /opt/petaly_labs/data/dest_data/
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
    destination_prefix_path: abcd/test
    compress_format: gz
    platform_type: gcp
    gcp_project_id: 'my-project
    gcp_region: EU
    gcp_bucket_name: 'bucket-name'
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

#### CSV to Redshift

```
pipeline:
  pipeline_attributes:
    pipeline_name: csv2rs
    is_enabled: true
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: redshift
    database_user: redshift-user
    database_password: 'db-password'
    database_host: redshift-host
    database_port: 5439
    database_name: dev
    database_schema: public
    platform_type: aws
    aws_account: null
    aws_region: 'eu-north-1'
    aws_bucket_name: 'bucket-name'
    aws_iam_role: 'arn:aws:iam::xxxxxxxx:role/YourRedshiftRole'
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
    destination_object_name: stocks_part
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /opt/petaly_labs/data/source_data/csv/test_data/stocks
    file_names:
    - 2013-01-02stocks.csv
    - 2013-01-04stocks.csv
    - 2013-01-08stocks.csv
- object_spec:
    object_name: options
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /opt/petaly_labs/data/source_data/csv/test_data/options
    file_names:
    - 2013-01-11options.csv
    - 2013-01-14options.csv

```

#### Redshift to CSV

```
pipeline:
  pipeline_attributes:
    pipeline_name: rs2csv
    is_enabled: true
  source_attributes:
    connector_type: redshift
    database_user: redshift-user
    database_password: 'db-password'
    database_host: redshift-host
    database_port: 5437
    database_name: dev
    database_schema: public
    platform_type: aws
    aws_account: null
    aws_region: 'eu-north-1'
    aws_bucket_name: 'sigmaql-bucket-01'
    aws_iam_role: 'arn:aws:iam::xxxxxxxxx:role/YourRedshiftRole'
  target_attributes:
    connector_type: csv
    destination_file_dir: /opt/petaly_labs/data/dest_data
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
- object_spec:
    object_name: options
    destination_object_name:
    recreate_destination_object: false
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -

```