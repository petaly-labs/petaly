SELECT tb.table_schema as source_schema_name,
       tb.table_name as source_object_name,
       tb.ordinal_position as ordinal_position,
       tb.column_name as column_name,
       tb.is_nullable as is_nullable,
       tb.data_type as data_type,
       NULL as character_maximum_length,
       NULL as numeric_precision,
       NULL as numeric_scale,
       cn.constraint_type as primary_key
FROM myetl-412113.{schema}.INFORMATION_SCHEMA.COLUMNS  as tb
LEFT JOIN (SELECT kcu.table_schema, kcu.table_name, kcu.column_name, tc.constraint_type
          FROM myetl-412113.{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
          JOIN  myetl-412113.{schema}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc
              ON  kcu.table_schema = tc.table_schema
                  AND kcu.table_name = tc.table_name
                  AND kcu.constraint_name = tc.constraint_name
                  AND tc.constraint_type = 'PRIMARY KEY'
          ) as cn ON  cn.table_schema = tb.table_schema
                  AND cn.table_name = tb.table_name
                  AND cn.column_name = tb.column_name
WHERE tb.table_schema in ('{schema}')
{table_statement_list}
ORDER BY tb.table_schema,
       tb.table_name,
       tb.ordinal_position