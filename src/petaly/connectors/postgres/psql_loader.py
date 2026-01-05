# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

from petaly.connectors.postgres.psql_connector import PsqlConnector
from petaly.core.db_loader import DBLoader
from petaly.utils.utils import FormatDict


class PsqlLoader(DBLoader):

    def __init__(self, pipeline):
        self.db_connector = PsqlConnector(pipeline.target_attr)
        super().__init__(pipeline)

    def load_data(self):
        super().load_data()

    #def execute_sql(self, create_table_stmt):
    #    self.db_connector.execute_sql(create_table_stmt)

    def load_from(self, loader_obj_conf):
        #object_name = loader_obj_conf.get('object_name')
        load_from_stmt = loader_obj_conf.get('load_from_stmt')

        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')

        # Unzip any compressed files that may be present.
        self.f_handler.gunzip_csv_files(output_data_object_dir)

        # Convert Parquet/JSON files to CSV for database loading
        # Database loaders (PostgreSQL, MySQL) require CSV format
        self._convert_parquet_json_to_csv(output_data_object_dir, loader_obj_conf)

        # 2. drop and recreate table
        if loader_obj_conf.get('recreate_destination_object') == True:
            self.drop_table(loader_obj_conf)

        self.create_table(loader_obj_conf)

        # For loading: collect CSV files (Parquet/JSON files have been converted to CSV)
        import glob
        import os
        all_files = []
        for pattern in ['*.csv', '*.tsv', '*.txt']:
            all_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
        file_list = list(set([f for f in all_files if os.path.isfile(f)]))

        for path_to_data_file in file_list:
            logger.debug(f"Source file: {path_to_data_file}")
            logger.debug(f"Statement to execute:\n{load_from_stmt}")

            self.db_connector.load_from(load_from_stmt, path_to_data_file)

    def compose_create_table_stmt(self, loader_obj_conf):

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        create_table_stmt = table_ddl_dict.get('create_table_stmt')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        column_datatype_list = table_ddl_dict.get('column_datatype_list')

        create_table_stmt = create_table_stmt.format_map(FormatDict(schema_table_name=schema_table_name,
                                                                    column_datatype_list=column_datatype_list,
                                                                    partition_by='',
                                                                    cluster_by='',
                                                                    table_options='',
                                                                    alter_table_primary_or_unique_key=''))

        loader_obj_conf.get('table_ddl_dict').update({'create_table_stmt': create_table_stmt})

        return loader_obj_conf


    def compose_from_options(self, loader_obj_conf):
        """
        """
        load_options = ""
        object_settings = loader_obj_conf.get('object_settings')
        load_options += f", DELIMITER '{object_settings.get('columns_delimiter')}'"
        load_options += f", HEADER {object_settings.get('header')}"

        columns_quote = object_settings.get('columns_quote')
        if columns_quote == 'double':
            load_options += f", QUOTE '\"'"
        elif columns_quote == 'single':
            load_options += f", QUOTE \"'\""

        # Add NULL handling for \N (common null representation in TSV/CSV files)
        # This allows PostgreSQL to recognize \N as NULL for all column types (including numeric)
        load_options += f", NULL '\\N'"

        return load_options

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        """ Its compose a copy from statement """

        copy_from_options = self.compose_from_options(loader_obj_conf)

        load_from_stmt = self.f_handler.load_file(self.connector_load_from_stmt_fpath)
        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        column_list = '' if table_ddl_dict.get('column_list') == None else '(' + table_ddl_dict.get('column_list') + ')'

        load_from_stmt = load_from_stmt.format_map(FormatDict(schema_table_name=schema_table_name,
                                                              column_list=column_list,
                                                              copy_from_options=copy_from_options))
        load_from_file_fpath = loader_obj_conf.get('load_from_stmt_fpath')

        self.f_handler.save_file(load_from_file_fpath, load_from_stmt)
        return load_from_stmt

    def drop_table(self, loader_obj_conf: dict):

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        self.db_connector.drop_table(schema_table_name)

    def create_table(self, loader_obj_conf: dict):
        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        loader_obj_conf = self.compose_create_table_stmt(loader_obj_conf)
        self.f_handler.save_file(table_ddl_dict.get('create_table_stmt_fpath'),
                                 table_ddl_dict.get('create_table_stmt'))

        self.db_connector.execute_sql(loader_obj_conf.get('table_ddl_dict').get('create_table_stmt'))
    
    def _convert_parquet_json_to_csv(self, output_data_object_dir, loader_obj_conf):
        """Converts Parquet/JSON files to CSV format for database loading.
        
        Database loaders (PostgreSQL, MySQL) require CSV format. This method:
        1. Detects Parquet (.parquet, .parq) and JSON (.json) files
        2. Converts them to CSV using the object settings (delimiter, quote, etc.)
        3. Keeps the original files in the workspace
        """
        import os
        import glob
        
        # Find all Parquet and JSON files
        parquet_files = []
        for pattern in ['*.parquet', '*.parq']:
            parquet_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
        
        json_files = glob.glob(os.path.join(output_data_object_dir, '*.json'))
        
        object_settings = loader_obj_conf.get('object_settings', {})
        delimiter = object_settings.get('columns_delimiter', ',')
        columns_quote = object_settings.get('columns_quote', 'double')
        has_header = object_settings.get('header', True)
        
        # Convert Parquet files to CSV
        for parquet_file in parquet_files:
            csv_file = os.path.splitext(parquet_file)[0] + '.csv'
            logger.info(f"Converting Parquet file to CSV for database loading: {parquet_file} -> {csv_file}")
            self._convert_parquet_to_csv(parquet_file, csv_file, object_settings)
        
        # Convert JSON files to CSV
        for json_file in json_files:
            csv_file = os.path.splitext(json_file)[0] + '.csv'
            logger.info(f"Converting JSON file to CSV for database loading: {json_file} -> {csv_file}")
            self._convert_json_to_csv(json_file, csv_file, object_settings)
    
    def _convert_parquet_to_csv(self, parquet_file, csv_file, object_settings):
        """Converts a Parquet file to CSV format."""
        try:
            import pandas as pd
            import pyarrow.parquet as pq
            import csv as csv_module
            import json
            
            # Read Parquet file
            # Read as Arrow table first, then convert to pandas
            # Use use_pandas_metadata=False to avoid issues with pandas-specific metadata
            # and ensure we get the raw data as stored in Parquet
            table = pq.read_table(parquet_file, use_pandas_metadata=False)
            
            # Convert to pandas, but preserve string columns as object type
            # This is critical for JSON strings and other complex string data
            df = table.to_pandas(strings_to_categorical=False)
            
            logger.debug(f"Parquet file read: shape={df.shape}, columns={list(df.columns)}")
            logger.debug(f"Column dtypes after read: {df.dtypes.to_dict()}")
            
            # Verify the DataFrame structure before processing
            if len(df) == 0:
                logger.warning(f"Parquet file {parquet_file} contains no data rows")
                return
            
            # Check first row to verify data integrity
            if len(df) > 0:
                logger.debug("Verifying first row data integrity:")
                for col in df.columns:
                    val = df[col].iloc[0]
                    val_type = type(val)
                    val_str = str(val) if val is not None else 'None'
                    logger.debug(f"  {col}: type={val_type}, length={len(val_str) if val_str != 'None' else 0}, preview={val_str[:100] if len(val_str) > 100 else val_str}")
            
            # Remove index columns if present (Parquet files may include index columns like __index_level_0__)
            # These are internal pandas/Parquet metadata and should not be included in CSV
            index_columns = [col for col in df.columns if col.startswith('__index')]
            if index_columns:
                logger.debug(f"Removing index columns from Parquet file: {index_columns}")
                df = df.drop(columns=index_columns)
                
            # Verify expected columns are present
            expected_cols = ['id', 'osm_id', 'name', 'type', 'admin_level', 'geometry']
            missing_cols = [col for col in expected_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Expected columns missing from Parquet file: {missing_cols}")
            else:
                logger.debug("All expected columns present in Parquet file")
            
            # Convert all columns to string to ensure proper CSV formatting
            # This handles complex types (arrays, nested structures) that pandas might serialize incorrectly
            # Also ensures consistent formatting for PostgreSQL COPY command
            for col in df.columns:
                # Check data type before conversion
                col_dtype = df[col].dtype
                sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                
                logger.debug(f"Column '{col}': dtype={col_dtype}, sample_type={type(sample_val) if sample_val is not None else None}")
                
                # CRITICAL: Convert to object type first to preserve exact data structure
                # This prevents pandas from doing any automatic type conversions that could corrupt data
                df[col] = df[col].astype('object')
                
                # Check if column contains complex types (lists, dicts, etc.)
                # Note: If the column is already a string (like JSON), we don't need to convert it
                if sample_val is not None and isinstance(sample_val, (list, dict)):
                    # Convert complex types to JSON string representation
                    logger.debug(f"Converting complex type in column '{col}' to JSON string")
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
                
                # Now convert to string, preserving the exact value
                # Use a lambda function to handle NaN/None properly without corrupting the data
                def safe_str_convert(x):
                    if pd.isna(x) or x is None:
                        return ''
                    # If it's already a string, return as-is
                    if isinstance(x, str):
                        return x
                    # Otherwise convert to string
                    return str(x)
                
                df[col] = df[col].apply(safe_str_convert)
                
                # Replace pandas NaN/None representations with empty string (PostgreSQL will interpret as NULL)
                # Only replace actual string representations, not valid data
                df[col] = df[col].replace(['nan', 'None', '<NA>', 'NaT', '[]', '{}'], '')
                
                # Replace embedded newlines and carriage returns with spaces to prevent CSV row breaks
                # PostgreSQL COPY will handle quoted fields with newlines, but it's safer to normalize
                # Only replace actual newline characters, not escaped sequences
                # Use apply to handle string operations safely
                df[col] = df[col].apply(lambda x: x.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ') if isinstance(x, str) else x)
            
            # Verify DataFrame integrity before writing
            logger.debug(f"DataFrame after conversion: shape={df.shape}")
            if len(df) > 0:
                first_row_sample = {}
                for col in df.columns:
                    # Get value directly from DataFrame to avoid any corruption
                    val = df.at[df.index[0], col]
                    val_str = str(val) if val is not None else 'None'
                    first_row_sample[col] = {
                        'type': type(val).__name__,
                        'length': len(val_str) if val_str != 'None' else 0,
                        'preview': val_str[:200] + ('...' if len(val_str) > 200 else ''),
                        'ends_with': '...' + val_str[-100:] if len(val_str) > 100 else val_str
                    }
                logger.debug(f"First row sample: {first_row_sample}")
                
                # CRITICAL: Verify geometry column is intact
                if 'geometry' in df.columns:
                    geom_val = df.at[df.index[0], 'geometry']
                    geom_str = str(geom_val) if geom_val is not None else ''
                    logger.debug(f"Geometry column verification:")
                    logger.debug(f"  Type: {type(geom_val)}")
                    logger.debug(f"  Length: {len(geom_str)}")
                    logger.debug(f"  Starts with: {geom_str[:100] if len(geom_str) > 100 else geom_str}")
                    logger.debug(f"  Ends with: {geom_str[-100:] if len(geom_str) > 100 else geom_str}")
                    
                    # Check if it looks like valid JSON
                    if geom_str.startswith('{') or geom_str.startswith('['):
                        logger.debug(f"  Looks like JSON: Yes")
                    else:
                        logger.warning(f"  Looks like JSON: No - geometry may be corrupted!")
                        logger.warning(f"  Full value: {geom_str[:500]}")
            
            # Write to CSV with object settings
            delimiter = object_settings.get('columns_delimiter', ',')
            columns_quote = object_settings.get('columns_quote', 'double')
            has_header = object_settings.get('header', True)
            
            quotechar = '"' if columns_quote == 'double' else ("'" if columns_quote == 'single' else None)
            
            # Write CSV using Python's csv module directly for precise control
            # This ensures each field is properly quoted as a single unit
            import csv as csv_writer
            
            logger.debug(f"Writing CSV with delimiter='{delimiter}', quotechar='{quotechar}', has_header={has_header}")
            
            with open(csv_file, 'w', encoding='utf-8', newline='') as f:
                # Use QUOTE_ALL to ensure every field is quoted, preventing comma splitting
                writer = csv_writer.writer(f, 
                                          delimiter=delimiter,
                                          quotechar=quotechar if quotechar else '"',
                                          quoting=csv_writer.QUOTE_ALL,  # Always quote all fields
                                          doublequote=True,  # Escape quotes by doubling them
                                          lineterminator='\n')
                
                # Write header if needed
                if has_header:
                    writer.writerow(df.columns.tolist())
                    logger.debug(f"Wrote header: {df.columns.tolist()}")
                
                # Write data rows - ensure each value is a proper string
                rows_written = 0
                for idx, row in df.iterrows():
                    row_values = []
                    for col in df.columns:
                        val = row[col]
                        
                        # Handle NaN/None values
                        if pd.isna(val) or val is None:
                            row_values.append('')
                        else:
                            # CRITICAL: Get the value directly from the DataFrame to avoid corruption
                            # Using row[col] might cause issues, so get it directly from df
                            val = df.at[idx, col]
                            
                            # Convert to string - this should preserve the exact value
                            if isinstance(val, str):
                                str_val = val
                            else:
                                str_val = str(val)
                            
                            # Check for pandas NaN string representations
                            if str_val in ['nan', 'None', '<NA>', 'NaT', '[]', '{}']:
                                row_values.append('')
                            else:
                                # Use the string value as-is - ensure it's a single string, not split
                                row_values.append(str_val)
                    
                    # Verify row has correct number of columns before writing
                    if len(row_values) != len(df.columns):
                        logger.error(f"Row {rows_written + 1} has {len(row_values)} values but DataFrame has {len(df.columns)} columns!")
                        logger.error(f"  Expected columns: {list(df.columns)}")
                        logger.error(f"  Row values count: {len(row_values)}")
                        raise ValueError(f"Column count mismatch: expected {len(df.columns)}, got {len(row_values)}")
                    
                    # Write the row
                    writer.writerow(row_values)
                    rows_written += 1
                    
                    # Log first few rows for debugging
                    if rows_written <= 3:
                        logger.debug(f"Row {rows_written} written with {len(row_values)} columns")
                        if 'geometry' in df.columns:
                            geom_idx = df.columns.get_loc('geometry')
                            geom_val = row_values[geom_idx]
                            logger.debug(f"  Geometry column ({geom_idx}): length={len(geom_val)}, preview={geom_val[:150]}...")
                            if len(geom_val) > 0:
                                logger.debug(f"  Geometry column ends with: ...{geom_val[-50:]}")
                
                logger.debug(f"Total rows written: {rows_written}")
            
            # Verify the CSV file was written correctly
            import os
            csv_size = os.path.getsize(csv_file)
            logger.debug(f"Converted Parquet to CSV: {parquet_file} -> {csv_file} (size: {csv_size} bytes, rows: {len(df)})")
            
        except Exception as e:
            logger.error(f"Error converting Parquet file {parquet_file} to CSV: {e}")
            raise
    
    def _convert_json_to_csv(self, json_file, csv_file, object_settings):
        """Converts a JSON file to CSV format."""
        try:
            import pandas as pd
            import csv as csv_module
            
            # Read JSON file (supports both array of objects and newline-delimited JSON)
            try:
                # Try reading as array of objects first
                df = pd.read_json(json_file, orient='records')
            except ValueError:
                # If that fails, try newline-delimited JSON
                df = pd.read_json(json_file, lines=True)
            
            # Write to CSV with object settings
            delimiter = object_settings.get('columns_delimiter', ',')
            columns_quote = object_settings.get('columns_quote', 'double')
            has_header = object_settings.get('header', True)
            
            if columns_quote == 'double':
                quotechar = '"'
                # Use QUOTE_ALL to ensure ALL fields are quoted, preventing parsing issues with
                # fields that contain numbers, commas, or special characters (like JSON strings)
                quoting = csv_module.QUOTE_ALL
            elif columns_quote == 'single':
                quotechar = "'"
                # Use QUOTE_ALL to ensure ALL fields are quoted, preventing parsing issues with
                # fields that contain numbers, commas, or special characters (like JSON strings)
                quoting = csv_module.QUOTE_ALL
            else:
                quotechar = None
                quoting = csv_module.QUOTE_NONE
            
            df.to_csv(
                csv_file,
                sep=delimiter,
                quotechar=quotechar,
                quoting=quoting,
                index=False,
                header=has_header,
                escapechar='\\' if quotechar else None
            )
            
            logger.debug(f"Converted JSON to CSV: {json_file} -> {csv_file}")
            
        except Exception as e:
            logger.error(f"Error converting JSON file {json_file} to CSV: {e}")
            raise
