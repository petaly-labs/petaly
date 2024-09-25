LOAD DATA LOCAL INFILE '{path_to_data_file}'
INTO TABLE {table_name}
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE {skip_leading_rows} ROWS
{column_list}
;