# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

import sys

from petaly.utils.utils import FormatDict
from petaly.core.db_loader import DBLoader

from petaly.connectors.redshift.rs_connector import RSConnectorIAM, RSConnectorTCP
from petaly.connectors.s3.s3_connector import S3Connector


class RSLoader(DBLoader):

    def __init__(self, pipeline):
        #connection_params = self.get_connection_params(pipeline.target_attr)

        if pipeline.target_attr.get('connection_method') == 'iam':
            self.db_connector = RSConnectorIAM(pipeline.target_attr)
            self.s3_connector = S3Connector(pipeline.target_attr, aws_session=self.db_connector.aws_session)
        elif pipeline.target_attr.get('connection_method') == 'tcp':
            self.db_connector = RSConnectorTCP(pipeline.target_attr)
            self.s3_connector = S3Connector(pipeline.target_attr, aws_session=None)
        else:
            logger.error(f"The connection_method: {pipeline.source_attr.get('connection_method')} is not supported for AWS load.")
            sys.exit()

        super().__init__(pipeline)

        self.cloud_bucket_name = self.pipeline.target_attr.get('bucket_name')
        # bucket_name is required for Redshift (used for staging)
        if self.cloud_bucket_name:
            self.cloud_bucket_path = self.s3_connector.bucket_prefix + self.cloud_bucket_name
        else:
            logger.error(f"bucket_name is required for Redshift target but was not found in target_attributes")
            raise ValueError("bucket_name is required in target_attributes for Redshift connector")
        self.aws_iam_role = self.pipeline.target_attr.get('aws_iam_role')

    def load_data(self):
        super().load_data()

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

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')
        blob_prefix = loader_obj_conf.get('blob_prefix')
        # 1. cleanup object from bucket
        self.s3_connector.delete_object_in_bucket(self.cloud_bucket_name, blob_prefix)

        # 2. drop and recreate table
        if loader_obj_conf.get('recreate_destination_object') == True:
            self.drop_table(loader_obj_conf)

        self.create_table(loader_obj_conf)
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')

        # Detect file format in output directory
        # Redshift can load Parquet and JSON directly without conversion
        import glob
        import os
        
        # Check for Parquet files first (Redshift supports direct Parquet loading)
        parquet_files = glob.glob(os.path.join(output_data_object_dir, '*.parquet'))
        parquet_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.parquet.gz')))
        
        # Check for JSON files (Redshift supports direct JSON loading)
        json_files = glob.glob(os.path.join(output_data_object_dir, '*.json'))
        json_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.json.gz')))
        
        # If Parquet or JSON files exist, use them directly (no CSV conversion needed)
        if parquet_files:
            file_list = list(set([f for f in parquet_files if os.path.isfile(f)]))
            logger.info(f"Redshift will load Parquet files directly (no CSV conversion needed): {len(file_list)} files")
        elif json_files:
            file_list = list(set([f for f in json_files if os.path.isfile(f)]))
            logger.info(f"Redshift will load JSON files directly (no CSV conversion needed): {len(file_list)} files")
            # Convert JSON arrays to newline-delimited JSON format for Redshift
            self._convert_json_to_newline_delimited(file_list)
        else:
            # No Parquet/JSON files found, fall back to CSV
            # Gzip all CSV files (they will be parsed based on delimiter, not extension)
            self.f_handler.gzip_csv_files(output_data_object_dir, cleanup_file=True)
            
            # Collect all files (regardless of extension) - delimiter will determine how to parse them
            all_files = []
            for pattern in ['*', '*.gz', '*.csv', '*.tsv', '*.txt']:
                all_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
            file_list = list(set([f for f in all_files if os.path.isfile(f)]))

        self.s3_connector.upload_files_to_bucket(self.cloud_bucket_name, blob_prefix, file_list)

        s3_file_list = self.s3_connector.get_bucket_file_list(self.cloud_bucket_name, blob_prefix)
        load_from_stmt = loader_obj_conf.get('load_from_stmt')

        for data_fpath in s3_file_list:
            path_to_data_file = self.cloud_bucket_path + '/' + data_fpath
            load_from_stmt = load_from_stmt.format_map(
                FormatDict(path_to_data_file=path_to_data_file))

            self.db_connector.load_from(load_from_stmt)

    def _convert_json_to_newline_delimited(self, json_files):
        """
        Converts JSON array files to newline-delimited JSON format for Redshift.
        Redshift with FORMAT AS JSON 'auto' requires one JSON object per line,
        not JSON arrays. Also flattens nested objects to JSON strings since
        Redshift can't handle nested objects with 'auto' mode.
        """
        import json
        
        def flatten_nested_objects(obj):
            """
            Flattens nested objects/arrays to JSON strings.
            Redshift FORMAT AS JSON 'auto' only works with flat JSON.
            Nested objects (like geometry) must be converted to strings.
            """
            if not isinstance(obj, dict):
                return obj
            
            flattened = {}
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    # Convert nested object/array to JSON string
                    flattened[key] = json.dumps(value, ensure_ascii=False)
                else:
                    flattened[key] = value
            return flattened
        
        for json_file in json_files:
            try:
                # Skip if already gzipped (we'll handle those separately if needed)
                if json_file.endswith('.gz'):
                    continue
                
                # Read the JSON file
                with open(json_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if not content:
                        continue
                    # Try to parse as JSON
                    try:
                        data = json.loads(content)
                    except json.JSONDecodeError:
                        # Might already be newline-delimited, check first line
                        f.seek(0)
                        first_line = f.readline().strip()
                        if first_line.startswith('{') and first_line.endswith('}'):
                            # Already newline-delimited, but might need flattening
                            # Re-read and process line by line
                            f.seek(0)
                            lines = f.readlines()
                            needs_flattening = False
                            processed_lines = []
                            for line in lines:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    obj = json.loads(line)
                                    flat_obj = flatten_nested_objects(obj)
                                    if flat_obj != obj:
                                        needs_flattening = True
                                    processed_lines.append(flat_obj)
                                except json.JSONDecodeError:
                                    continue
                            
                            if needs_flattening and processed_lines:
                                logger.debug(f"Flattening nested objects in newline-delimited JSON: {json_file}")
                                with open(json_file, 'w', encoding='utf-8') as fw:
                                    for obj in processed_lines:
                                        json.dump(obj, fw, ensure_ascii=False)
                                        fw.write('\n')
                                logger.debug(f"Flattened {len(processed_lines)} objects in {json_file}")
                            continue
                        else:
                            # Unknown format, skip conversion
                            logger.warning(f"File {json_file} is not in a recognized JSON format, skipping conversion")
                            continue
                
                # Check if it's a JSON array (needs conversion)
                if isinstance(data, list):
                    logger.debug(f"Converting JSON array to newline-delimited format: {json_file}")
                    # Write as newline-delimited JSON (one object per line), flattening nested objects
                    with open(json_file, 'w', encoding='utf-8') as f:
                        for obj in data:
                            flat_obj = flatten_nested_objects(obj)
                            json.dump(flat_obj, f, ensure_ascii=False)
                            f.write('\n')
                    logger.debug(f"Converted {json_file} to newline-delimited JSON format ({len(data)} objects)")
                elif isinstance(data, dict):
                    # Single object - convert to newline-delimited format and flatten
                    logger.debug(f"Converting single JSON object to newline-delimited format: {json_file}")
                    flat_obj = flatten_nested_objects(data)
                    with open(json_file, 'w', encoding='utf-8') as f:
                        json.dump(flat_obj, f, ensure_ascii=False)
                        f.write('\n')
                    logger.debug(f"Converted {json_file} to newline-delimited JSON format")
                # If it's already newline-delimited, leave it as is
                
            except Exception as e:
                logger.warning(f"Error converting JSON file {json_file} to newline-delimited format: {e}")
                # Continue with other files
                continue

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
        object_settings = loader_obj_conf.get("object_settings")
        
        # Detect source file format to determine if we can load Parquet/JSON directly
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')
        import glob
        import os
        
        # Check for Parquet or JSON files
        parquet_files = glob.glob(os.path.join(output_data_object_dir, '*.parquet'))
        parquet_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.parquet.gz')))
        json_files = glob.glob(os.path.join(output_data_object_dir, '*.json'))
        json_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.json.gz')))
        
        # Determine format
        using_json_format = False
        using_parquet_format = False
        using_csv_format = False
        
        if parquet_files:
            # Parquet format - Redshift COPY supports FORMAT AS PARQUET
            # Note: Parquet does NOT support GZIP option - it has internal compression
            load_options += "FORMAT AS PARQUET "
            using_parquet_format = True
            logger.info(f"Redshift will load Parquet format directly (no CSV conversion needed)")
        elif json_files:
            # JSON format - Redshift COPY supports FORMAT AS JSON 'auto'
            # Note: Redshift does NOT support GZIP, MAXERROR, or TIMEFORMAT with JSON format
            # The 'auto' argument tells Redshift to automatically map JSON keys to column names
            load_options += "FORMAT AS JSON 'auto' "
            using_json_format = True
            logger.info(f"Redshift will load JSON format directly (no CSV conversion needed)")
        else:
            # CSV format - include delimiter, header, and quote options
            load_options += "FORMAT AS CSV "
            using_csv_format = True
            columns_delimiter = object_settings.get("columns_delimiter")

            if columns_delimiter == '\t':
                load_options += "DELIMITER '\\t' "
            else:
                load_options += f"DELIMITER '{columns_delimiter}' "

            skip_leading_rows = 1 if object_settings.get("header") is None or True else 0
            load_options += f"IGNOREHEADER {skip_leading_rows} "

            columns_quote = object_settings.get("columns_quote")
            if columns_quote not in ('double','single'):
                load_options += "REMOVEQUOTES "

            # Add NULL handling for \N (common null representation in TSV/CSV files)
            # Redshift COPY recognizes \N as NULL by default, but EMPTYASNULL ensures empty strings are also treated as NULL
            # This provides consistent NULL handling across all column types
            load_options += "EMPTYASNULL "

        # GZIP compression is ONLY supported for CSV format
        # Parquet has internal compression, JSON does not support GZIP option
        if using_csv_format:
            load_options += "GZIP "

        # Store format flags in loader_obj_conf so compose_load_from_stmt can use them
        loader_obj_conf['using_json_format'] = using_json_format
        loader_obj_conf['using_parquet_format'] = using_parquet_format

        return load_options

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        """ Its compose a copy from statement """
        load_data_options = self.compose_from_options(loader_obj_conf)
        load_from_stmt = self.f_handler.load_file(self.connector_load_from_stmt_fpath)

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        column_list = '' if table_ddl_dict.get('column_list') == None else '(' + table_ddl_dict.get('column_list') + ')'

        # Check format type - JSON and Parquet don't support MAXERROR or TIMEFORMAT
        using_json_format = loader_obj_conf.get('using_json_format', False)
        using_parquet_format = loader_obj_conf.get('using_parquet_format', False)
        
        # Remove trailing whitespace from load_data_options to prevent syntax errors
        load_data_options = load_data_options.rstrip()
        
        # Format the statement first
        load_from_stmt = load_from_stmt.format_map(FormatDict(schema_table_name=schema_table_name,
                                                               column_list=column_list,
                                                              iam_role=self.aws_iam_role,
                                                              load_from_options=load_data_options))
        
        # For JSON and Parquet formats, remove MAXERROR and TIMEFORMAT from the formatted statement
        # Redshift COPY with FORMAT AS JSON / FORMAT AS PARQUET doesn't support these options
        if using_json_format or using_parquet_format:
            # Remove MAXERROR and TIMEFORMAT lines
            lines = load_from_stmt.split('\n')
            filtered_lines = []
            for line in lines:
                stripped_line = line.strip()
                # Skip MAXERROR and TIMEFORMAT lines
                if stripped_line.startswith('MAXERROR') or stripped_line.startswith("TIMEFORMAT"):
                    continue
                filtered_lines.append(line)
            
            # Rejoin and clean up
            load_from_stmt = '\n'.join(filtered_lines)
            # Remove all trailing whitespace including newlines
            load_from_stmt = load_from_stmt.rstrip()
            
            # Handle semicolon placement - remove standalone semicolons and ensure proper ending
            lines = load_from_stmt.split('\n')
            final_lines = []
            for line in lines:
                stripped = line.strip()
                if stripped == ';':
                    # Skip standalone semicolon lines
                    continue
                elif "FORMAT AS JSON" in line or "FORMAT AS PARQUET" in line:
                    # FORMAT line - ensure it ends properly
                    final_lines.append(line.rstrip())
                else:
                    final_lines.append(line)
            
            load_from_stmt = '\n'.join(final_lines).rstrip()
            
            # Ensure we have a semicolon at the end
            if not load_from_stmt.rstrip().endswith(';'):
                load_from_stmt = load_from_stmt.rstrip() + ';'
        
        load_from_file_fpath = loader_obj_conf.get('load_from_stmt_fpath')
        self.f_handler.save_file(load_from_file_fpath, load_from_stmt)
        
        # Log the final statement for debugging
        logger.debug(f"Final COPY statement for {schema_table_name}:\n{load_from_stmt}")

        return load_from_stmt

