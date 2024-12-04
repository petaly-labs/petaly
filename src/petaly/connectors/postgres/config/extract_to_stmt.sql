COPY (SELECT {column_list}
FROM {schema_name}.{table_name}) TO STDOUT
WITH (FORMAT CSV {copy_to_options});