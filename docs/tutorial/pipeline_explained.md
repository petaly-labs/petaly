# Tutorial 

After creation of the petaly.ini file<br> 
`$ python3 -m petaly init -c /path_to_config_dir/petaly.ini`<br> 
and init the workspace defined in the petaly.ini you can start configuring pipelines<br> 
`$ python3 -m petaly init workspace -c /path_to_config_dir/petaly.ini`<br> 

To initialise pipelines with new endpoints it is recommended to use the pipeline wizard.

```
$ python3 -m petaly init -p my_pipeline -c /path_to_config_dir/petaly.ini
```

It can have different configurations depending on the endpoints.<br>  
Let's take an example of exporting from mysql and importing into postgres<br> 

The first document in the pipeline.yaml file is called pipeline and contains 3 main blocks<br> 
```
pipeline:
  pipeline_attributes:
  ....
  source_attributes:
  ....
  target_attributes:
  ...
---  
```



## Key Blocks: 
### pipeline_attributes

```
pipeline:
  pipeline_attributes:
    pipeline_name: mysql_to_psql
    # The name of the pipeline must be unique
    
    is_enabled: true
    # True indicates that the pipeline is enabled. The default is true
    
    preferred_load_type: full
    # Only full load is supported yet
    
    data_transition_format: csv
    # Only csv format is supported yet
    
    load_data_objects_spec_only: true
    # If true, load only the objects specified in data_objects_spec.
    # If false, load all objects from the specified database_schema

```
### source_attributes
```
pipeline:

  source_attributes:
    platform_type: local
    # only local is supported yet
    
    endpoint_type: mysql 
    # Specify endpoint type: mysql, postgres, csv
    
    database_user: root
    # Specify database user name
    
    database_password: dbpassword
    # Specify database user password in plain text
    
    database_host: localhost
    # Specify database hostname or IP address
    
    database_port: 3306
    # Specify database port
    
    database_name: tutorial_db
    # Specify database name
    
```    
### target_attributes
```
pipeline:

  target_attributes:
    platform_type: local
    # only local is supported yet
    
    endpoint_type: postgres
    # Specify endpoint type: mysql, postgres, csv
    
    database_user: postgres
     # Specify database user name
     
    database_password: dbpassword
    # Specify database user password in plain text
    
    database_host: localhost
    # Specify database hostname or IP address
    
    database_port: 5432
    # Specify database port
    
    database_name: petaly_db
    # Specify database name
     
    database_schema: petaly
    # Specify database schema name
```
### data_objects_spec
The second document in the pipeline.yaml file is separated by three hyphen.<br>  
It can contain multiple objects(tables) or it can be empty if key<br>
`load_data_objects_spec_only: false`<br>  
In that case all objects from given schema will be loaded<br> 

```
---
data_objects_spec:[]
```
In case the following key `load_data_objects_spec_only: true` has true value, specify each object one by one in pipeline.yaml file
```
---
data_objects_spec:
- object_name: object-name
  object_attributes:
    target_object_name:   
    ...
```
Use wizard to simplify the configuration

```
# Three dashes separate the pipeline document from the data_objects_spec.
---
data_objects_spec:


- object_name: stocks
# Specify a unique data_object name.
# This can be a table name for a database or a file/folder name for file export/import.
# Multiple objects can be specified starting with a dash - object_name: and followed by object_attributes: parameters.
  
  object_attributes:
    target_object_name: stocksnew
    # Optional! Specify target_object_name only if you want it to be different from source_object_name
  
    recreate_target_object: true
    # If this parameter is true, the target object (table) will be rebuilt. If false, the object/table will be created if it does not exists. 
    # The default is false
    
    excluded_columns:
      - null
    # Leave empty or specify null to include all columns, at least one hyphen should exists
    # Specify the column name to exclude specific file or leave blank (put null) to include all columns from the table/object.
    # At least one hyphen should be present.
    excluded_columns: [column1, column2]
    # In case you plan to exclude columns: specify either a comma-separated list of columns in brackets [] to exclude.
    # or use hyphen charachter one per column as shown below.
    excluded_columns:
      - column1
      - column2
      
    load_type: full
    # Only full load is supported yet
    
    load_batch_size: 10000
    # Specify batch size for incremental load. It's not supported yet
    
    incremental_load_column: null
    # Incremental load column. It's not supported yet
    
    skip_leading_rows: 1
    # Skip leading rows, numeric value, usually 1
    
    file_format: csv
    # Relevant to file upload only. Currently only CSV format is supported
    
    file_dir: null
    # Relevant to file upload only. Specify path to file directory
    
    # Relevant to file upload only.
    file_name_list:
    - null
    # Specify the filenames to load specific file or leave blank (or put null) to include all files from file_dir.
    # At least one hyphen should be present.
    # If you leave empty/null, remember that all files in file_dir should have the same metadata structure as they will be loaded into the same table. 
    # If you want to load a specific file from the given file_dir use hyphen charachter one per filename as shown below.
    file_name_list:
    - file_name.csv
    - file_name_2.csv
    - file_name_3.csv
    
    target_file_format: csv
    # Relevant to file extract only. Supports csv format only
    
    target_file_dir: null
    # Relevant to file extract only. Destination directory after processing data in output folder first
```
# Full Example: MySQL to Postgres
```
pipeline:
  pipeline_attributes:
    pipeline_name: mysql_to_psql
    is_enabled: true
    preferred_load_type: full
    data_transition_format: csv
    load_data_objects_spec_only: false
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
---
data_objects_spec:
- object_name: stocks
  object_attributes:
    target_object_name: stocksnew
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
