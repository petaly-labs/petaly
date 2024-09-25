COPY (SELECT {column_list}
FROM {schema_name}.{table_name}) TO STDOUT
WITH (FORMAT CSV, DELIMITER ',', HEADER true, FORCE_QUOTE *, ENCODING 'UTF-8');