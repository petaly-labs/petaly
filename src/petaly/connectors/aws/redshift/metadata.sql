SELECT tb.table_schema as source_schema_name,
       tb.table_name as source_object_name,
       cl.ordinal_position as ordinal_position,
       cl.column_name as column_name,
       cl.is_nullable as is_nullable,
       cl.data_type as data_type,
       cl.character_maximum_length as character_maximum_length,
       cl.numeric_precision as numeric_precision,
       cl.numeric_scale as numeric_scale,
       cnst.column_name as primary_key
FROM information_schema.tables tb
JOIN svv_columns cl ON tb.table_schema = cl.table_schema AND tb.table_name = cl.table_name
LEFT JOIN
    (SELECT tc.table_schema, tc.table_name, cu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage cu ON tc.table_schema = cu.table_schema AND tc.table_name=cu.table_name AND tc.constraint_name=cu.constraint_name
        WHERE tc.constraint_type ='PRIMARY KEY') cnst ON tb.table_schema= cnst.table_schema AND tb.table_name=cnst.table_name AND cl.column_name=cnst.column_name
WHERE tb.table_type in ('BASE TABLE', 'VIEW')
AND tb.table_schema in ('{schema}')
{table_statement_list}
ORDER BY tb.table_schema,
       tb.table_name,
       cl.ordinal_position
       ;
