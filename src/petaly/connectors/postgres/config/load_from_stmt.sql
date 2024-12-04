COPY {schema_table_name}
{column_list}
FROM STDIN
WITH (FORMAT CSV {copy_from_options})
;
