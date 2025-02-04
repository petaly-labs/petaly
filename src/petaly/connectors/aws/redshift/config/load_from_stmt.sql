COPY {schema_table_name} {column_list}
FROM '{path_to_data_file}'
IAM_ROLE '{iam_role}'
{load_from_options}
MAXERROR 0
TIMEFORMAT 'auto';
;