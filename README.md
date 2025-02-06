![](https://raw.githubusercontent.com/petaly-labs/petaly/main/images/logo/petaly_logo_transparent.png)

![](https://raw.githubusercontent.com/petaly-labs/petaly/main/docs/tutorial/recording/petaly_run_pipe.gif)

## Welcome to Petaly!

Petaly is an open-source ETL/ELT (Extract, Load, "Transform") tool, created by and for data professionals! 
Our mission is to simplify data movement across different platforms with a tool that truly understands the needs of the data community.

Petaly is designed for seamless data exchange, currently supporting the following endpoints:

- PostgreSQL
- MySQL
- BigQuery
- Redshift
- Google Cloud Storage (GCS Bucket)
- S3 Bucket
- local CSV files

It makes connecting and transferring data across various systems effortless. Petaly is user-friendly and requires no programming knowledge. 
Data pipelines can be easily configured using the YAML format, making the tool ready to use immediately after installation.

## Getting Started

Explore the documentation below:
  
<br>**[1. Installation](#petaly-install)**
<br>**[2. Init config file and workspace](#petaly-init-config-workspace)**
<br>**[3. Init a pipeline](#petaly-init-pipeline)**
<br>**[4. Run the pipeline](#petaly-run-pipeline)**
<br>**[5. Load CSV to Postgres](#petaly-load-csv-postgres-examples)** (Step-by-Step Tutorial)
<br>**[6. Pipeline better explained](#petaly-pipeline-explained)**


## Requirements:
- Python 3.10 - 3.12

OS:
- Linux
- MacOS

It's possible that Petaly will work with other operating systems and python versions. It just hasn't been tested yet.

<a id="petaly-install"></a>

## 1. Installation
Petaly can be installed using `pip` or downloaded directly from this repository.

### Install with pip
To install petaly with support for open-source endpoints: such as PostgreSQL, MySQL, and CSV, use: `pip install petaly`
```
$ mkdir petaly
$ cd petaly
$ python3 -m venv .venv
$ source .venv/bin/activate
$ python3 -m pip install petaly
```

To install all packages including GCP and AWS libraries use:

```
$ python3 -m pip install petaly[all]
```

#### Install with GCP
In case GCP BigQuery or Google Cloud Storage support is required, install it using: `pip install petaly[gcp]`

```
$  python3 -m pip install petaly[gcp]
```
or use `pip install petaly[all]`

To use your GCP resources, the first step is to install the Google Cloud SDK (gcloud) from the official webpage: [Google Cloud SDK Installation](https://cloud.google.com/sdk/docs/install-sdk)
Follow the instructions to configure access to your Google Project, BigQuery, and GCS (bucket).
Petaly supports access via a service account authentication key in JSON format, saved locally and configured with gcloud.

#### Install with AWS
In case AWS Redshift or AWS S3 Storage support is required, install it using: `pip install petaly[aws]`
```
$  python3 -m pip install petaly[aws]
```
or use `pip install petaly[all]`

To access AWS resources, install the AWS CLI by following the official tutorial: [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-prereqs.html)

After installation, configure access to your AWS Account, Redshift and S3 (bucket). [AWS Documentation](https://docs.aws.amazon.com/)

Petaly supports two authentication methods:

**IAM** - Authentication via AWS IAM using a profile saved in `.aws/config` locally (recommended). 
Alternatively, direct authentication can be used by specifying `aws_access_key_id` and `aws_secret_access_key` in `pipeline.yaml`.

**TCP** - Authentication using host, port, database user and password.

Petaly provides authentication options for:
- **Redshift Serverless** access via IAM or TCP
- **Redshift Cluster** access via IAM or TCP
- **S3 Bucket** access via IAM

To use TCP, the Redshift Cluster or Serverless instance can be accessed through an SSH jump host configured in AWS VPC. The cluster or serverless instance does not need to be publicly available. 
The following AWS tutorial provides guidance on setting this up. [Access private Redshift Cluster via TCP](https://repost.aws/knowledge-center/private-redshift-cluster-local-machine)

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

<a id="petaly-init-config-workspace"></a>

## 2. Initialize config file and workspace


This tutorial explains the `petaly.ini` configuration file and how to create a workspace.

### Init config file

To create petaly.ini file run following step once:

`$ python3 -m petaly -c /absolute-path-to-your-config-dir/petaly.ini init`

After the configuration file has been created, either use it always with the [-c] argument

`$ python -m petaly -c /absolute-path-to-your-config-dir/petaly.ini init`

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

`$ python3 -m petaly -c /path_to_config_dir/petaly.ini init --workspace` 

<a id="petaly-init-pipeline"></a>

## 3. Init a pipeline


Run the following command and follow the wizard steps to initialize a pipeline my_pipeline.
No changes will be made to the target endpoint at this point.

`$ python3 -m petaly -c /path_to_config_dir/petaly.ini init -p my_pipeline `

Run the following command to configure your pipeline. Once the pipeline is created, you can modify it manually.

For detailed instructions, check in the section: **[Pipeline explained](#petaly-pipeline-explained)**

<a id="petaly-run-pipeline"></a>

## 4. Run Pipeline

Now you can run the pipeline my_pipeline and load data from the specified source. 
Note that it will make changes, re/create tables in the target endpoint (database or folders)

`$ python3 -m petaly -c /path_to_config_dir/petaly.ini run -p my_pipeline`

<a id="petaly-load-csv-postgres-examples"></a>

## 5. Load CSV file to Postgres

In this tutorial, we’ll show you how to run a new pipeline and load a test CSV file into PostgreSQL.

### 5.1. Before You Start

- Ensure that Petaly is installed properly and your workspace is initialized. 
- Refer to our [installation](#petaly-install) and [workspace setup guides](#petaly-init-config-workspace) if needed.

- Install a PostgreSQL server locally, or use a Docker image for setup.

### 5.2. Configure the Pipeline:

- Run the following command to configure your pipeline. For detailed instructions, check the section below **[Pipeline explained](#petaly-pipeline-explained)**

- `$ python3 -m petaly -c /path_to_config_dir/petaly.ini init -p csv_to_postgres`

### 5.3. Download the test files:

- Download the stocks and options CSV files from the repository. [./tests/data/csv/*](./tests/data/csv/) and store it under `/your-directory-path-to-csv-files`
- unzip it under linux or mac with: `gunzip options.csv.gz`, `gunzip stocks.csv.gz` 

### 5.4. Set Up the Pipeline:

- Use `csv` as the source and `postgres` as the target. Follow the configuration wizard.

- Specify objects for `stocks` and `options`, and provide the appropriate paths in object_source_dir and file_names inside.

### 5.5. Run the Pipeline:

- Execute the pipeline using the configured settings:

- `$ python3 -m petaly -c /path_to_config_dir/petaly.ini run -p csv_to_psql`


### 5.6 CSV to Postgres Full Examples

Use the wizard to create the appropriate pipeline skeleton, as different connector types require specific parameters.

Once the pipeline is created, you can modify it using editors like `vi`, `nano`, or any other text editor.

The following examples are provided for reference only. For detailed instructions, and other examples check the section below **[Pipeline explained](#petaly-pipeline-explained)**

#### CSV to Postgres
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

<a id="petaly-pipeline-explained"></a>

## 6. Pipeline explained

This tutorial provides a step-by-step guide for getting started with Petaly after installation. It begins with an explanation of the petaly.ini configuration file and then dives into the details of the pipeline.yaml file.

### Pipeline configuration

To avoid mistakes during configuration, it is recommended to use the pipeline wizard. The configuration differs depending on the endpoints.

To use pipeline wizard run following command:

`$ python3 -m petaly -c /path_to_config_dir/petaly.ini init -p my_pipeline`

### Pipeline structure
After creating the pipeline, let's review the structure and each parameter.

Here is an example of an export table from mysql to postgres

The pipeline.yaml file contains two documents `pipeline:` and `data_objects_spec:`.

The `pipeline:` document defines `pipeline_attributes:`, `source_attributes:`, `target_attributes:` and `data_attributes:`

The second document in the pipeline.yaml file, separated by three dashes, is the `data_objects_spec:` document which contains the definition of each object/table.

Here is the skeleton of the whole pipeline.yaml document:

```
pipeline:
  pipeline_attributes:
    ....
  source_attributes:
    ....
  target_attributes:
    ....
  data_attributes:
    ....
--- 
data_objects_spec:[] 
```

### Pipeline Main Blocks:

The structure of a pipeline definition is explained in the following sections.

#### pipeline_attributes

```
  pipeline_attributes:
    # The name of the pipeline must be unique
    pipeline_name: mysql_to_psql
    
    # True indicates that the pipeline is enabled. The default is true
    is_enabled: true
    
```

#### source_attributes

The source attributes specify the source connections. The connection parameters may differ depending on the endpoint type.

```
  source_attributes:
    # Specify endpoint type: mysql, postgres, csv
    connector_type: mysql 
    
    # Specify database user name
    database_user: root
    
    # Specify database user password in plain text
    database_password: dbpassword
    
    # Specify the database hostname or IP address. It can also be remote, if access is given to the machine running petaly, or if an ssh tunnel is provided.
    database_host: localhost
    
    # Specify database port
    database_port: 3306
    
    # Specify database name
    database_name: tutorial_db
    
```    
#### target_attributes

The target attributes specify the target connections. The connection parameters may differ depending on the endpoint type.

```
  target_attributes:    
    
    # Specify endpoint type: mysql, postgres, csv
    connector_type: postgres
    
    # Specify database user name
    database_user: postgres
     
    # Specify database user password in plain text
    database_password: dbpassword
    
    # Specify the database hostname or IP address. It can also be remote, if access is given to the machine running petaly, or if an ssh tunnel is provided.
    database_host: localhost
    
    # Specify database port
    database_port: 5432
    
    # Specify database name
    database_name: petaly_db
     
    # Specify database schema name
    database_schema: petaly
```
#### data_attributes
Following parameters configuring default behavior for data-objects/tables 
```
    data_attributes:
      
      # provide fine definition: only, prefer, ignore 
      data_objects_spec_mode: only
```
In this step, you will define the main behaviour of the object definition, as follows:<br>
If **only** [default]: Load only the objects explicitly specified in data_objects_spec[] section. These objects will be configured in the next step.<br>
If **ignore**: Load all objects from the database_schema (or database_name if no schema exists) as defined in the source_attributes section, completely disregarding data_objects_spec[] section.<br>
If **prefer**: Load all objects from the database_schema, but for objects specified in data_objects_spec[], apply the refined configuration defined in that section.<br>

**object_default_settings**

The object_default_settings parameter defines the default configuration options applied to objects during processing.
These settings serve as a baseline and can be overridden by more specific configurations if needed.

This section is used for loading data from CSV files, as well as for extracting and loading data from databases.

It provides a standardized configuration to facilitate seamless data exchange between different engines, such as MySQL and PostgreSQL.


```
      # define csv parse options. It also valid for mysql, postgres extract or load
      object_default_settings:
        
        # Specifies whether the file should contains or contains a header line with the names of each column in the file.
        header: true
        
        # The character delimiting individual cells in the CSV data.
        columns_delimiter: ","
        
        # Choose between double, single or none. The default is double.
        columns_quote:  double
        
```

### data_objects_spec

The second document `data_objects_spec:` in the pipeline.yaml file, separated by three dashes, contains none, one or more object (table). 

```
---
data_objects_spec:[]
```

As explained above data_objects_spec is only used when it set to **only** or **prefer**. The default mode is **only**
`data_objects_spec_mode: only`

Take a look at the `data_objects_spec:` bellow:

```
---
data_objects_spec:
- object_spec:
    object_name: orders
    destination_object_name:   
    ...
- object_spec:
    object_name: customers
    destination_object_name: customers_reloaded
    ...     
```

The `object_name` has to be unique.
This can be a source/destination table name.
Multiple objects can be specified starting with a dash `- object_spec:` and followed by `object_name: table_name` and few others parameters.

Optionally, the destination object/table name can be different. To achieve this specify the parameter `destination_object_name`.

```
---
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name: stocks_new
```

If the `recreate_destination_object` parameter is true, the target object (table) will be recreated. 
Otherwise, the object/table will only be created if it does not exist. The default is false.

```
    recreate_destination_object: false
```

Use the `exclude_columns` parameter to exclude specific columns, or leave it blank to include all columns for specific table/object. 
If you want to exclude columns: either specify a comma-separated list of columns to exclude in parentheses [] or use dashes '-', one per line. As shown below:   

To include all
```    
    exclude_columns:
      - null
```

To exclude column1, column2

```
    exclude_columns:
      - column1
      - column2
```

Alternative approach to exclude column1, column2

```
    exclude_columns: [column1, column2]
```

### csv files as source

In `object_source_dir:`, specify the path to the directory where the csv files are stored. This is only relevant for file uploads.

`object_source_dir: /absolute-path-to-file-dir`

Specify the filenames to load specific file or leave blank/null to include all files from `object_source_dir:`.
At least one dash or empty brackets [] should be present.
If you leave it empty/null, remember that all files in `object_source_dir:` should have the same metadata structure as they will be loaded into the same table. 
In case you want to load only a specific file/s from the given `object_source_dir:` use dash character one per line and filename.

Use one of the following options: 
1. To load all files from the `object_source_dir` set dash to null `- null` or keep dash `-` empty:    
```
    file_names:
    - null
```
Alternatively, to load all files from the `object_source_dir` set:    
```
    file_names: []
```
2. To load specific files from the `object_source_dir` set: 
```

    file_names:
    - file_name.csv
    - file_name_2.csv
    - file_name_3.csv
```
Alternatively, to load specific files from `object_source_dir` set:

```
    file_names: [file_name.csv, file_name_2.csv, file_name_3.csv]
```

### csv as target

The target file format is specified through connector_type: csv.

The initial data is loaded into the output folder defined in the petaly.ini file. 
The destination_dir defines the final directory after download the data in the output folder first.


```
pipeline:
  ....
  target_attributes:
    connector_type: csv
    destination_dir: /your-path-to-destination-folder
```

### On GCP platform
if you use Bigquery or Google Cloud Storage(Bucket) following parameters to configure.

```
    platform_type: gcp
    
    # use bigquery or gcs
    connector_type: gcs
    
    # Specify the GCP project ID.
    gcp_project_id: your-project-id
    
    # Specify the GCP region or location.
    gcp_region: your-region
    
    # Specify the GCP bucket name without the gs:// prefix. Leave it empty if loading from a local folder is preferred.
    gcp_bucket_name: your-bucket-name
    
    # [Optional] It defines the path prefix to your objects in the bucket. Don't use here the bucket name. Use a forward slash (/) to separate folders. 
    # By default, the string petaly/{pipeline_name} will be added as the prefix. The pattern {pipeline_name} will be automatically replaced with the pipeline name during runtime.
    # If not needed, you can remove it manually after the pipeline is created.
    bucket_pipeline_prefix: petaly/{pipeline_name}

```

For BigQuery you can specify dataset name under:
``` 
    database_schema: petaly_tutorial
```

### On AWS platform

For Redshift or S3-Bucket following parameters to configure.

```
    platform_type: aws
    
    # use s3 or redshift
    connector_type: s3
    
    # Specify the AWS Bucket Name, without prefix s3://
    aws_bucket_name: 'bucket-name'
    
    # [Optional] It defines the path prefix to your objects in the bucket. Don't use here the bucket name. Use a forward slash (/) to separate folders. 
    # By default, the string petaly/{pipeline_name} will be added as the prefix. The pattern {pipeline_name} will be automatically replaced with the pipeline name during runtime.
    # If not needed, you can remove it manually after the pipeline is created.
    bucket_pipeline_prefix: petaly/{pipeline_name}
    
    # This role should have the ability to access the S3 bucket defined in aws_bucket_name from the Redshift site.
    aws_iam_role: 'arn:aws:iam::xxxxxxxx:role/YourRedshiftRole'
    
    # Specify the AWS profile_name if it is defined in the .aws/config file after installing the AWS SDK. If you use this method, leave the following options aws_access_key_id, aws_secret_access_key and aws_region empty.
    aws_profile_name: 'your-aws-profile'
    
    # [Optional] If aws_profile_name is empty specify the aws-access-key-id. You can ignore this if the aws-access-key-id is already defined in .aws/config and the aws_profile_name parameter is properly specified.
    aws_access_key_id:
    
    # [Optional] If aws_profile_name is empty specify the aws-secret-access-key. You can ignore this if the aws-secret-access-key is already defined in .aws/config and the aws_profile_name parameter is properly specified.
    aws_secret_access_key:
    
    # [Optional] If aws_profile_name is empty specify the AWS region here
    aws_region: 'eu-north-1'

```
For Redshift, additional parameters must be configured. The required parameter list will dynamically adjust based on the Redshift type (Cluster or Serverless) and the connection method (TCP or IAM). 
<br>Follow the pipeline wizard instructions `init -p pipeline-name` for detailed guidance on Redshift.


[More Pipeline Examples](./docs/tutorial/pipeline_examples.md)

## Let's Build Together  🌱

Join us in building something meaningful together.

The foundation of any open-source project is its community, a group of individuals collaborating, sharing knowledge and contributing to a shared vision. At Petaly, every contribution, no matter the size, plays an important role in shaping the project.
We’re continuously improving Petaly, and your feedback and contributions are invaluable. 
Check out our [Contributing Guide](./CONTRIBUTING.md) to see how you can get involved. Connect with fellow contributors, share your experiences and get support in our community channels. 

Together, we can make Petaly even better!