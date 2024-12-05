# Step 4. Run Pipeline
## Load CSV file to Postgres or MySQL

In this tutorial, we’ll show you how to run a new pipeline and load a test CSV file into PostgreSQL or MySQL.

### Before You Start

- Ensure that Petaly is installed properly and your workspace is initialized. 
- Refer to our [installation](./petaly_install.md) and [workspace setup guides](./petaly_init_workspace.md) if needed.

- Install a PostgreSQL or MySQL server locally, or use a Docker image for setup.

### Steps to Run the Pipeline

#### 1. Configure the Pipeline:

- Run the following command to configure your pipeline. <br><br>For detailed instructions, check  [How to set up a pipeline with Petaly](./pipeline_explained.md)

- `$ python3 -m petaly init -p csv_to_postgres -c /path_to_config_dir/petaly.ini`

#### 2. Download the test files:

- Download the stocks and options CSV files from the repository. [petaly/tests/data/csv/*](../../tests/data/csv/) and store it under `/your-directory-path-to-csv-files`
- unzip it under linux or mac with: `gunzip options.csv.gz`, `gunzip stocks.csv.gz` 

#### 3. Set Up the Pipeline:

- Use `csv` as the source and either `postgres` or `mysql` as the target. Follow the configuration wizard.
- Specify objects for `stocks` and `options`, and provide the appropriate paths in files_source_dir and file_names.

#### 4. Run the Pipeline:

- Execute the pipeline using the configured settings:

- `$ python3 -m petaly run -p csv_to_postgres -c /path_to_config_dir/petaly.ini`


### Pipeline Full Examples

Use the wizard to create the appropriate pipeline skeleton, as different connector types require specific parameters.

Once the pipeline is created, you can modify it using editors like `vi`, `nano`, or any other text editor.

The following examples are provided for reference only.

#### csv to postgres
```
pipeline:
  pipeline_attributes:
    pipeline_name: csv_to_psql
    is_enabled: true
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: postgres
    database_user: root
    database_password: db-password
    database_host: localhost
    database_port: 5432
    database_name: petalydb
    database_schema: petaly_schema
  data_attributes:
    use_data_objects_spec: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      quote_char: none-quote
---
data_objects_spec:
- object_name: stocks
  object_attributes:
    destination_object_name:
    recreate_target_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    files_source_dir: /your-directory-path-to-csv-files
    file_names:
    - stocks.csv
- object_name: options
  object_attributes:
    destination_object_name:
    recreate_target_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    files_source_dir: /your-directory-path-to-csv-files
    file_names:
    - options.csv
```

#### csv to mysql
```
pipeline:
  pipeline_attributes:
    pipeline_name: csv_to_mysql
    is_enabled: true
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: mysql
    database_user: root
    database_password: db-password
    database_host: localhost
    database_port: 3306
    database_name: petalydb
  data_attributes:
    use_data_objects_spec: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      quote_char: none-quote
---
data_objects_spec:
- object_name: stocks
  object_attributes:
    destination_object_name:
    recreate_target_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    files_source_dir: /your-directory-path-to-csv-files
    file_names:
    - stocks.csv
- object_name: options
  object_attributes:
    destination_object_name:
    recreate_target_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    files_source_dir: /your-directory-path-to-csv-files
    file_names:
    - options.csv    
```
