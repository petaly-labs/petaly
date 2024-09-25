SELECT r.table_schema as source_schema_name,
       r.table_name as source_object_name,
       r.ordinal_position as ordinal_position,
       r.column_name as column_name,
       r.is_nullable as is_nullable,
       r.data_type as data_type,
       r.character_maximum_length as character_maximum_length,
       r.numeric_precision as numeric_precision,
       r.numeric_scale as numeric_scale,
       r.primary_key as primary_key
FROM (
SELECT tb.table_schema,
       tb.table_name,
       cl.ordinal_position,
       cl.column_name,
       cl.is_nullable,
       CASE WHEN cl.data_type = 'USER-DEFINED' THEN cl.udt_name ELSE cl.data_type END,
       cl.character_maximum_length,
       cl.numeric_precision,
       cl.numeric_scale,
       tc.column_name as primary_key
FROM information_schema.tables tb
INNER JOIN information_schema.columns cl ON (cl.table_schema = tb.table_schema AND cl.table_name = tb.table_name )
LEFT JOIN
    (SELECT nc.nspname AS table_schema, r.relname AS table_name, a.attname AS column_name
        FROM pg_class r,
            pg_namespace nc,
            pg_attribute a,
            pg_constraint c
        WHERE r.oid = a.attrelid
            AND  nc.oid = r.relnamespace
            AND nc.oid = c.connamespace
            AND c.conrelid=r.oid
            AND c.contype='p'
            AND a.attnum = any (c.conkey)
            ) tc
    ON (tc.table_schema = cl.table_schema
        AND tc.table_name = cl.table_name
        AND tc.column_name = cl.column_name
        AND tc.table_schema = cl.table_schema)
WHERE tb.table_type in ('BASE TABLE', 'VIEW')
    AND tb.table_schema in ('{schema}')
{table_statement_list}
ORDER BY tb.table_schema,
       tb.table_name,
       cl.ordinal_position
) AS r


