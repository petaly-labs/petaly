![](https://raw.githubusercontent.com/petaly-labs/petaly/main/images/logo/petaly_logo_transparent.png)

![](https://raw.githubusercontent.com/petaly-labs/petaly/main/docs/tutorial/recording/petaly_run_pipe.gif)

## Welcome to Petaly!

Petaly is an open-source ETL (Extract, Transform, Load) tool created by and for data professionals! 
Our mission is to simplify data movement and transformation across different platforms with a tool that truly understands the needs of the data community.

Designed for seamless data exchange, Petaly supports PostgreSQL, MySQL and CSV formats, with plans to expand and integrate additional open-source technologies. 
It makes connecting and transferring data across various systems effortless. Petaly is user-friendly and requires no programming knowledge. 
Data pipelines can be easily configured using the YAML format, making the tool ready to use immediately after installation.

## Important
This is an Alpha version of the Petaly project!

## Getting Started

- **Explore the Documentation:** Check out our documentation to get started with: 
  - **[Installation](#petaly-install)**
  - **[Configuration](#petaly-init-config-workspace)**
  - **[How to set up your first pipeline](https://github.com/petaly-labs/petaly/blob/main/docs/tutorial/pipeline_explained.md)**
- **Contribute:** We’re continuously improving Petaly, and your feedback and contributions are invaluable. Check out our Contributing Guide to see how you can get involved.
- **Join the Community:** Connect with fellow contributors, share your experiences, and get support in our community channels.

## Tool Features
In the current version Petaly provides extract and load data between following endpoints:

- CSV
- MySQL (tested version 8.0+)
- PostgresQL (tested version 16+)

## Requirements:
- Python 3.10 - 3.12

## Tested on
Petaly was tested on: 

OS: 
- MacOS 14.6
- Ubuntu 22.04.3 LTS

It's possible that the tool will work with other operating systems and other databases and python versions. It just hasn't been tested yet.


## 1. Installation
<a id="petaly-install"></a>
Petaly can be installed using `pip` or downloaded directly from this repository.

### Install with pip
```
$ mkdir petaly
$ cd petaly
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip3 install petaly

```
### Alternatively, download and install from GitHub

```
$ git clone https://github.com/petaly-labs/petaly.git
$ cd petaly
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip3 install -r requirements.txt
$ cd src/

# Installation step completed
```

Check petaly installation.

If petaly was installed with: 

`$ pip install petaly`

run:

`$ python3 -m petaly --help` 

If the repo was cloned from the GitHub  [petaly-labs](https://github.com/petaly-labs/petaly) repository navigate to the src folder first:

`$ cd petaly/scr` and execute the command above

## 2. Initialize config file and workspace
<a id="petaly-init-config-workspace"></a>

This tutorial explains the `petaly.ini` configuration file and how to create a workspace.

### Init config file

To create petaly.ini file run following step once:

`$ python3 -m petaly init -c /absolute-path-to-your-config-dir/petaly.ini`

After the configuration file has been created, either use it always with the [-c] argument

`$ python -m petaly init -c /absolute-path-to-your-config-dir/petaly.ini`

or to skip the [-c] argument, set the environment variable `PETALY_CONFIG_DIR`

`$ export PETALY_CONFIG_DIR=/absolute-path-to-your-config-dir`

### Init workspace

This step must be performed only once after petaly installation. 

If the workspace is already initialised, you can skip the following step and start configuring pipelines. If the workspace hasn't been initialised yet, first define following three parameters in petaly.ini

```
pipeline_dir_path=/absolute-path-to-pipelines-dir
logs_dir_path=/absolute-path-to-logs-dir
output_dir_path=/absolute-path-to-output-dir
```
And then initialize the workspace with following command. This command will simply create all these 3 paths defined above in the petaly.ini file.

`$ python3 -m petaly init --workspace -c /path_to_config_dir/petaly.ini` 

## 3. Init a pipeline
<a id="petaly-init-pipeline"></a>

Run the following command and follow the wizard steps to initialize a pipeline my_pipeline.
No changes will be made to the target endpoint at this point.

`$ python3 -m petaly init -p my_pipeline -c /path_to_config_dir/petaly.ini`

Run the following command to configure your pipeline. Once the pipeline is created, you can modify it manually.

For detailed instructions, check tutorial: [How to set up a pipeline with Petaly](./docs/tutorial/pipeline_explained.md)

## 4. Run Pipeline
<a id="petaly-run-pipeline"></a>
Now you can run the pipeline my_pipeline and load data from the specified source. 
Note that it will make changes, re/create tables in the target endpoint (database or folders)

`$ python3 -m petaly run -p my_pipeline -c /path_to_config_dir/petaly.ini`


## 5. Load CSV file to Postgres or MySQL with Examples
<a id="petaly-run-examples"></a>
In this tutorial, we’ll show you how to run a new pipeline and load a test CSV file into PostgreSQL or MySQL.

### 5.1. Before You Start

- Ensure that Petaly is installed properly and your workspace is initialized. 
- Refer to our [installation](#petaly-install) and [workspace setup guides](#petaly-init-config-workspace) if needed.

- Install a PostgreSQL or MySQL server locally, or use a Docker image for setup.

### 5.2. Configure the Pipeline:

- Run the following command to configure your pipeline. For detailed instructions, check  [How to set up a pipeline with Petaly](./docs/tutorial/pipeline_explained.md)

- `$ python3 -m petaly init -p csv_to_postgres -c /path_to_config_dir/petaly.ini`

### 5.3. Download the test files:

- Download the stocks and options CSV files from the repository. [./tests/data/csv/*](./tests/data/csv/) and store it under `/your-directory-path-to-csv-files`
- unzip it under linux or mac with: `gunzip options.csv.gz`, `gunzip stocks.csv.gz` 

### 5.4. Set Up the Pipeline:

- Use `csv` as the source and either `postgres` or `mysql` as the target. Follow the configuration wizard.

- Specify objects for `stocks` and `options`, and provide the appropriate paths in object_source_dir and file_names inside.

### 5.5. Run the Pipeline:

- Execute the pipeline using the configured settings:

- `$ python3 -m petaly run -p csv_to_postgres -c /path_to_config_dir/petaly.ini`

## Full Examples

Use the wizard to create the appropriate pipeline skeleton, as different connector types require specific parameters.

Once the pipeline is created, you can modify it using editors like `vi`, `nano`, or any other text editor.

The following examples are provided for reference only. For detailed instructions, check [How to set up a pipeline with Petaly](./docs/tutorial/pipeline_explained.md)

### CSV to Postgres
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
    database_schema: petaly_tutorial
  data_attributes:
    use_data_objects_spec: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      quote_char: none-quote
---
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /your-directory-path-to-csv-files
    file_names:
    - stocks.csv
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
```

### CSV to MySQL
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
    database_name: petaly_tutorial
  data_attributes:
    use_data_objects_spec: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      quote_char: none-quote
---
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name:
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    -
    object_source_dir: /your-directory-path-to-csv-files
    file_names:
    - stocks.csv
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
```

#### MySQL to Postgres

The following example exports a table `stocks` from Mysql into PostgresQL under the name `stocks_in_postgres`

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
      quote_char: double-quote
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


