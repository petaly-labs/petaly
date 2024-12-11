# Step 3: How to set up a pipeline with Petaly

This tutorial provides a step-by-step guide for getting started with Petaly after installation. It begins with an explanation of the petaly.ini configuration file and then dives into the details of the pipeline.yaml file.


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
        
        # Choose between double-quote, single-quote or none-quote. The default is double-quote.
        quote_char:  double-quote
        
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
    object_name: table_name_1
    destination_object_name:   
    ...
- object_spec:
    object_name: table_name_2
    destination_object_name: table_name_2_new
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
    destination_object_name: stocksnew
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

#### csv files as source

In `object_source_dir:`, specify the path to the directory where the csv files are stored. This is only relevant for file uploads.

`object_source_dir: /absolute-path-to-file-dir`

Specify the filenames to load specific file or leave blank/null to include all files from `object_source_dir:`.
At least one dash or empty brackets [] should be present.
If you leave it empty/null, remember that all files in `file_dir:` should have the same metadata structure as they will be loaded into the same table. 
In case you want to load only a specific file/s from the given `file_dir:` use dash character one per line and filename.

Use one of the following options: 
1. To load all files from the `object_source_dir` set:    
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

#### csv as target

The target file format is specified through connector_type: csv.

The initial data is loaded into the output folder defined in the petaly.ini file. 
The destination_file_dir defines the final directory after download the data in the output folder first.


```
pipeline:
  ....
  target_attributes:
    connector_type: csv
    destination_file_dir: /your-path-to-destination-folder
```


### Full Pipeline Examples
#### CSV file to MySQL

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
    database_name: petalydb
  data_attributes:
    use_data_objects_spec: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      quote_char: none
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
    - stocks.csv
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
    database_name: petalydb
  target_attributes:
    connector_type: postgres
    database_user: postgres
    database_password: db-password
    database_host: localhost
    database_port: 5432
    database_name: petalydb
    database_schema: petaly_schema
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

#### Postgres to CSV

The following example exports tables **stocks*** and ***users** from Postgres into destination folder `destination_file_dir: /opt/petaly_labs/data/dest_data`
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
      quote_char: double-quote
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
    - 
```



