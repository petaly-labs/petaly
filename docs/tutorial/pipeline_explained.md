# Tutorial 

This tutorial explains the first steps after installation.

## Init config file

If the petaly.ini file has already been created, ignore this step. 

`$ python3 -m petaly init -c /path_to_config_dir/petaly.ini`

## Init workspace

This step must be performed once during the installation. 
If the workspace is already initialised, you can skip the following step and start configuring pipelines.

If the workspace hasn't been initialised yet, first define three predefined parameters in petaly.ini

```
pipeline_dir_path=/absolute-path-to-pipelines-dir
logs_dir_path=/absolute-path-to-logs-dir
output_dir_path=/absolute-path-to-output-dir
```
And execute:

`$ python3 -m petaly init --workspace -c /path_to_config_dir/petaly.ini` 

## Pipeline configuration

To avoid mistakes during configuration, it is recommended to use the pipeline wizard. The configuration differs depending on the endpoints.

To use pipeline wizard run following command:

`$ python3 -m petaly init -p my_pipeline -c /path_to_config_dir/petaly.ini`

### Pipeline structure
After creating the pipeline, let's review the structure and each parameter.

Here is an example of an export table from mysql to postgres

The pipeline.yaml file contains two documents `pipeline:` and `data_objects_spec:`.

The `pipeline:` document defines `pipeline_attributes:`, `source_attributes:` and `target_attributes:`.

The second document in the pipeline.yaml file, separated by three dashes, is the `data_objects_spec:` document which contains the definition of each object.

Here is the skeleton of the whole pipeline.yaml document:

```
pipeline:
  pipeline_attributes:
    ....
  source_attributes:
    ....
  target_attributes:
    ....
  data_object_main_config:
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

Following parameters configuring default behavior for data-objects/tables 
```
    data_object_main_config:
      
      # Only full load is supported yet
      preferred_load_type: full
      
      # Only csv format is supported yet  
      data_transition_format: csv
      
      # provide fine definition 
      use_data_objects_spec: true
      
```

The `use_data_objects_spec` parameter determines whether the `data_objects_spec` document is used.
If true, only the objects specified in `data_objects_spec` are loaded, otherwise all objects from the specified database_schema are loaded.

`use_data_objects_spec: true` [default is true]

#### source_attributes

The source attributes specify the source connections. The connection parameters may differ depending on the endpoint type.

```
  source_attributes:
    # Specify endpoint type: mysql, postgres, csv
    endpoint_type: mysql 
    
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
### target_attributes

The target attributes specify the target connections. The connection parameters may differ depending on the endpoint type.

```
  target_attributes:    
    
    # Specify endpoint type: mysql, postgres, csv
    endpoint_type: postgres
    
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

### data_objects_spec

The second document `data_objects_spec:` in the pipeline.yaml file, separated by three dashes, contains none, one or more object (table). 

```
---
data_objects_spec:[]
```

As explained above it is only used when it set to true, which is the default.
`use_data_objects_spec: true`

If it set to false, all tables from the specified schema in Postgres `database_schema:` or in MySQL `database_name` will be loaded. 

`use_data_objects_spec: false`

If case the `use_data_objects_spec:` was set to true, `data_objects_spec:` will be used.

`use_data_objects_spec: true`

Take a look at the `data_objects_spec:` bellow:

```
---
data_objects_spec:
- object_name: object_or_table_name_1
  object_attributes:
    target_object_name:   
    ...
- object_name: object_or_table_name_2
  object_attributes:
    target_object_name:
    ...     
```

The `object_name` has to be unique.
This can be a source/destination table name.
Multiple objects can be specified starting with a dash `- object_name:` and followed by `object_attributes:` parameters.

Optionally, the target object/table name can be different. To achieve this specify the parameter `target_object_name`.

```
---
data_objects_spec:
- object_name: stocks 
  object_attributes:
    target_object_name: stocksnew
```

If the `recreate_target_object` parameter is true, the target object (table) will be recreated. 
Otherwise, the object/table will only be created if it does not exist. The default is false.

```
    recreate_target_object: true
```

Use the `excluded_columns` parameter to exclude specific columns, or leave it blank to include all columns for specific table/object. 
If you want to exclude columns: either specify a comma-separated list of columns to exclude in parentheses [] or use dashes '-', one per line. As shown below:   

To include all
```    
    excluded_columns:
      - null
```

To exclude column1, column2

```
    excluded_columns:
      - column1
      - column2
```

Alternative approach to exclude column1, column2

```
    excluded_columns: [column1, column2]
```

The following parameters are not yet implemented and can be ignored.

```
    load_type: full
    load_batch_size: 10000
    incremental_load_column: null
```

#### Source file

To skip leading rows, for example a header line in csv file, use 1. This is only relevant for file uploads.

`skip_leading_rows: 1`

Currently only the CSV format is supported. This is only relevant for file uploads.

`file_format: csv`

In `file_dir:`, specify the path to the directory where the csv files are stored. This is only relevant for file uploads.

`file_dir: /absolute-path-to-file-dir`

Specify the filenames to load specific file or leave blank/null to include all files from `file_dir:`.
At least one dash or empty brackets [] should be present.
If you leave it empty/null, remember that all files in `file_dir:` should have the same metadata structure as they will be loaded into the same table. 
In case you want to load only a specific file/s from the given `file_dir:` use dash character one per line and filename.

Use one of the following options: 
1. To load all files from the `file_dir:`    
```
    file_name_list:
    - null
```
2. Alternatively, to load all the files from the `file_dir:`    
```
    file_name_list: []
```
3. To load specific files from the `file_dir:` 
```

    file_name_list:
    - file_name.csv
    - file_name_2.csv
    - file_name_3.csv
```
4. Alternatively, to load specific files from `file_dir:`

```
    file_name_list: [file_name.csv, file_name_2.csv, file_name_3.csv]
```

#### Target file

The target_file_format specifies the format of the extract file from the database to a file. [Only the csv format is supported.]

The initial data is loaded into the output folder defined in the petaly.ini file. 
The target_file_dir defines the target directory after processing the data in the output folder first.

```
    target_file_format: csv 
    target_file_dir: null
```

### Full Example: MySQL to Postgres

The following example exports a table stocks and options from Mysql and loads it into Postgres under the name `stocks` and `optionsnew`

```
pipeline:
  pipeline_attributes:
    pipeline_name: mysql_to_psql
    is_enabled: true
  source_attributes:
    platform_type: local
    endpoint_type: mysql
    database_user: root
    database_password: dbpassword
    database_host: localhost
    database_port: 3306
    database_name: tutorial_db
  target_attributes:
    platform_type: local
    endpoint_type: postgres
    database_user: postgres
    database_password: dbpassword
    database_host: localhost
    database_port: 5432
    database_name: pg_db
    database_schema: tutorial_petaly
  data_object_main_config:
    preferred_load_type: full
    data_transition_format: csv
    use_data_objects_spec: true  
---
data_objects_spec:
- object_name: stocks
  object_attributes:
    target_object_name:
    recreate_target_object: false
    excluded_columns:
      - adjust_close
    load_type: full
    load_batch_size: 10000
    incremental_load_column: null
    skip_leading_rows: 1
    file_format: csv
    file_dir: null
    file_name_list:
    - null
    target_file_format: csv
    target_file_dir: null
- object_name: options
  object_attributes:
    target_object_name: optionsnew
    recreate_target_object: true
    excluded_columns:
      - adjust_close
    load_type: full
    load_batch_size: 10000
    incremental_load_column: null
    skip_leading_rows: 1
    file_format: csv
    file_dir: null
    file_name_list:
    - null
    target_file_format: csv
    target_file_dir: null
```
