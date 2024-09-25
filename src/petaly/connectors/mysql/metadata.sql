SELECT tb.table_schema as source_schema_name,
       tb.table_name as source_object_name,
       c.ordinal_position as ordinal_position,
       c.column_name as column_name,
       c.is_nullable as is_nullable,
       c.column_type as data_type,
       c.character_maximum_length as character_maximum_length,
       c.numeric_precision as numeric_precision,
       c.numeric_scale as numeric_scale,
       tc.column_name as primary_key
    FROM information_schema.tables tb
    INNER JOIN information_schema.columns c ON (c.table_schema = tb.table_schema
                                                AND tb.table_name = c.table_name )
    LEFT JOIN ( SELECT kcu.table_schema, kcu.table_name, kcu.column_name
                FROM information_schema.key_column_usage kcu
                INNER JOIN information_schema.table_constraints tc ON ( tc.constraint_schema = kcu.constraint_schema
                                                                        AND tc.constraint_name = kcu.constraint_name
                                                                        AND tc.constraint_type = 'PRIMARY KEY'
                                                                        AND tc.table_name = kcu.table_name)
    ) tc ON (   tc.table_schema = tb.table_schema
                AND tc.table_name = c.table_name
                 AND c.column_name = tc.column_name)
    WHERE table_type IN ('BASE TABLE', 'VIEW')
        AND tb.table_schema in ('{schema}')
        {table_statement_list}
     ORDER BY tb.table_schema,
       tb.table_name,
       c.ordinal_position