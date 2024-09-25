COPY {schema_table_name}
{column_list}
FROM STDIN
 WITH (
 FORMAT CSV
,DELIMITER ','
,HEADER {has_header}
,ENCODING 'UTF-8')
;
