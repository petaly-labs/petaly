-- It's deprecated!
-- The SELECT ... INTO OUTFILE 'file_name' form of SELECT writes the selected rows to a file.
-- The file is created on the server host, so you must have the FILE privilege to use this syntax.
SELECT {column_list}
INTO OUTFILE '/tmp/result.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM {table_name};