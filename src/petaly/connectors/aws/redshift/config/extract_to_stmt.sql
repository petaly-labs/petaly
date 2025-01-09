UNLOAD ('SELECT {column_list} FROM {schema_name}.{table_name}')
TO '{extract_to_fpath}'
IAM_ROLE '{iam_role}'
FORMAT AS CSV
GZIP
PARALLEL OFF
ALLOWOVERWRITE
MAXFILESIZE 100 MB
{extract_to_options}
;

