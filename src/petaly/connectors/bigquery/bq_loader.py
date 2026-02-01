# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

from petaly.utils.file_handler import FileHandler
from petaly.core.db_loader import DBLoader
from petaly.utils.utils import FormatDict
from petaly.connectors.bigquery.bq_connector import BQConnector
from petaly.connectors.gs.gs_connector import GSConnector


class BQLoader(DBLoader):
    def __init__(self, pipeline):
        self.db_connector = BQConnector()
        self.gs_connector = GSConnector()
        self.f_handler = FileHandler()
        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.target_attr.get('bucket_name')
        self.cloud_project_id = self.pipeline.target_attr.get('gcp_project_id')
        self.cloud_region = self.pipeline.target_attr.get('gcp_region')
        
        # Handle bucket_name being None (optional for BigQuery - can load from local folder)
        if self.cloud_bucket_name:
            self.cloud_bucket_path = self.gs_connector.bucket_prefix + self.cloud_bucket_name + '/'
            self.load_from_bucket = True
        else:
            self.cloud_bucket_path = None
            self.load_from_bucket = False


    #def execute_sql(self, create_table_stmt):
    #    self.db_connector.execute_sql(create_table_stmt)

    def load_data(self):
        super().load_data()

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')

        table_id = self.get_table_id(loader_obj_conf.get('table_ddl_dict'))
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')

        if loader_obj_conf.get('recreate_destination_object') == True:
            self.drop_table(loader_obj_conf)
        self.create_table(loader_obj_conf)

        # Detect file format in output directory
        # BigQuery can load Parquet and JSON directly without conversion
        import glob
        import os
        
        # Check for Parquet files first (BigQuery supports direct Parquet loading)
        parquet_files = glob.glob(os.path.join(output_data_object_dir, '*.parquet'))
        parquet_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.parquet.gz')))
        
        # Check for JSON files (BigQuery supports direct JSON loading)
        json_files = glob.glob(os.path.join(output_data_object_dir, '*.json'))
        json_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.json.gz')))
        
        # If Parquet or JSON files exist, use them directly (no CSV conversion needed)
        if parquet_files:
            file_list = list(set([f for f in parquet_files if os.path.isfile(f)]))
            logger.info(f"BigQuery will load Parquet files directly (no CSV conversion needed): {len(file_list)} files")
        elif json_files:
            file_list = list(set([f for f in json_files if os.path.isfile(f)]))
            logger.info(f"BigQuery will load JSON files directly (no CSV conversion needed): {len(file_list)} files")
            # Convert JSON arrays to newline-delimited JSON format for BigQuery
            self._convert_json_to_newline_delimited(file_list, loader_obj_conf)
            # Convert JSON arrays to newline-delimited JSON format for BigQuery
            self._convert_json_to_newline_delimited(json_files, loader_obj_conf)
        else:
            # No Parquet/JSON files found, fall back to CSV
            # Check if CSV files are already gzipped
            csv_gz_files = []
            for pattern in ['*.csv.gz', '*.tsv.gz', '*.txt.gz']:
                csv_gz_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
            csv_gz_files = [f for f in csv_gz_files if os.path.isfile(f)]
            
            # Check for uncompressed CSV files (exclude if .gz version already exists)
            csv_files = []
            for pattern in ['*.csv', '*.tsv', '*.txt']:
                csv_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
            # Filter: must exist, not end with .gz, and not have a .gz version already
            uncompressed_csv_files = []
            for f in csv_files:
                if os.path.isfile(f) and not f.endswith('.gz'):
                    # Check if .gz version already exists
                    if not os.path.isfile(f + '.gz'):
                        uncompressed_csv_files.append(f)
            
            # Only gzip if there are uncompressed CSV files that need compression
            if uncompressed_csv_files:
                logger.debug(f"Found {len(uncompressed_csv_files)} uncompressed CSV files, compressing them...")
                self.f_handler.gzip_csv_files(output_data_object_dir, cleanup_file=True)
            elif csv_gz_files:
                logger.debug(f"Found {len(csv_gz_files)} already compressed CSV files, using them directly")
            
            # Collect all files (including already compressed ones)
            all_files = []
            for pattern in ['*.csv.gz', '*.tsv.gz', '*.txt.gz', '*.csv', '*.tsv', '*.txt']:
                all_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
            # Filter to only files (not directories) and remove duplicates
            file_list = list(set([f for f in all_files if os.path.isfile(f)]))
            
            if not file_list:
                logger.warning(f"No CSV files found in {output_data_object_dir}")
        if self.load_from_bucket == True:
            blob_prefix = loader_obj_conf.get('blob_prefix')
            self.gs_connector.delete_object_in_bucket(self.cloud_bucket_name, blob_prefix)
            bucket_file_list = self.gs_connector.upload_files_to_bucket(self.cloud_bucket_name, blob_prefix, file_list)

            if len(bucket_file_list) > 0:
                file_list = bucket_file_list
            else:
                logger.error(f"Files upload to bucket failed. Try upload from local path: ")

        bq_job_config_dict = loader_obj_conf.get('load_from_stmt')

        total_rows_loaded = 0
        for path_to_data_file in file_list:
            try:
                rows_loaded = self.db_connector.load_from(bq_job_config_dict, path_to_data_file, table_id, self.load_from_bucket, self.cloud_region)
                if rows_loaded is not None:
                    total_rows_loaded += rows_loaded
            except Exception as e:
                # Re-raise the exception so db_loader can catch it and mark as failed
                logger.error(f"Failed to load file {path_to_data_file} into BigQuery table {table_id}: {e}")
                raise
        
        # Store total rows loaded in loader_obj_conf so db_loader can use it
        loader_obj_conf['rows_loaded_from_bigquery'] = total_rows_loaded


    def compose_create_table_stmt(self, loader_obj_conf):

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        create_table_stmt = table_ddl_dict.get('create_table_stmt')
        create_table_stmt = create_table_stmt.format_map(FormatDict(schema_name=table_ddl_dict.get('schema_name'),
                                                                    table_name=table_ddl_dict.get('table_name'),
                                                                    column_datatype_list=table_ddl_dict.get('column_datatype_list'),
                                                                    partition_by='',
                                                                    cluster_by='',
                                                                    table_options=''))
        loader_obj_conf.get('table_ddl_dict').update({'create_table_stmt': create_table_stmt})

        return loader_obj_conf
    def get_table_id(self, table_ddl_dict):

        return (f"{self.cloud_project_id}."
                f"{table_ddl_dict.get('schema_name')}."
                f"{table_ddl_dict.get('table_name')}")

    def drop_table(self, loader_obj_conf: dict):

        table_id = self.get_table_id(loader_obj_conf.get('table_ddl_dict'))
        self.db_connector.drop_table(table_id)

    def create_table(self, loader_obj_conf: dict):
        loader_obj_conf = self.compose_create_table_stmt(loader_obj_conf)
        self.f_handler.save_file(loader_obj_conf.get('table_ddl_dict').get('create_table_stmt_fpath'),
                                 loader_obj_conf.get('table_ddl_dict').get('create_table_stmt'))

        self.db_connector.execute_sql(loader_obj_conf.get('table_ddl_dict').get('create_table_stmt'))

    def compose_from_options(self, loader_obj_conf):
        """
        """
        load_options = {}

        object_settings = loader_obj_conf.get("object_settings")
        columns_delimiter = object_settings.get("columns_delimiter")

        load_options.update({'delimiter': columns_delimiter})
        # Remove the problematic conversion that was causing PyArrow to fail
        # The delimiter should remain as a single character for PyArrow compatibility

        header = True if object_settings.get("header") is None or True else False
        load_options.update({'header': header})

        # 3. OPTIONALLY ENCLOSED BY
        columns_quote = object_settings.get("columns_quote")
        quote_char = None
        if columns_quote == 'double':
            quote_char = '"'
        elif columns_quote == 'single':
            quote_char = "'"
        load_options.update({'quote_char': quote_char})

        return load_options

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        """ Its compose a copy from statement """
        # Detect source file format to determine if we can load Parquet/JSON directly
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')
        import glob
        import os
        
        # Check for Parquet or JSON files
        parquet_files = glob.glob(os.path.join(output_data_object_dir, '*.parquet'))
        parquet_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.parquet.gz')))
        json_files = glob.glob(os.path.join(output_data_object_dir, '*.json'))
        json_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.json.gz')))
        
        # Determine source format
        # Import bigquery here to avoid circular imports
        from google.cloud import bigquery
        
        if parquet_files:
            source_format = bigquery.SourceFormat.PARQUET
            logger.info(f"BigQuery will load Parquet format directly (no CSV conversion needed)")
        elif json_files:
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            logger.info(f"BigQuery will load JSON format directly (no CSV conversion needed)")
        else:
            # Default to CSV
            source_format = bigquery.SourceFormat.CSV
        
        load_data_options = self.compose_from_options(loader_obj_conf)
        bq_load_from_stmt_fpath = self.f_handler.replace_file_extension(self.connector_load_from_stmt_fpath,'.json')
        load_from_stmt = self.f_handler.load_json(bq_load_from_stmt_fpath)

        #column_list = loader_obj_conf.get('table_ddl_dict').get('column_list')
        max_bad_records = 0
        
        # For Parquet and JSON formats, skip CSV-specific options
        if source_format == bigquery.SourceFormat.PARQUET:
            # Parquet format - no delimiter, header, or quote options needed
            # Remove CSV-specific fields that might be in the template
            load_from_stmt.update({"source_format": source_format})
            load_from_stmt.update({'autodetect': False})
            load_from_stmt.update({'max_bad_records': max_bad_records})
            # Remove CSV-specific fields that are not allowed for Parquet
            load_from_stmt.pop('field_delimiter', None)
            load_from_stmt.pop('quote_character', None)
            load_from_stmt.pop('skip_leading_rows', None)
        elif source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON:
            # JSON format - no delimiter, header, or quote options needed
            # Remove CSV-specific fields that might be in the template
            load_from_stmt.update({"source_format": source_format})
            load_from_stmt.update({'autodetect': False})
            load_from_stmt.update({'max_bad_records': max_bad_records})
            # Remove CSV-specific fields that are not allowed for JSON
            load_from_stmt.pop('field_delimiter', None)
            load_from_stmt.pop('quote_character', None)
            load_from_stmt.pop('skip_leading_rows', None)
        else:
            # CSV format - include delimiter, header, and quote options
            skip_leading_rows = 1 if load_data_options.get("header") is True else 0
            field_delimiter = load_data_options.get("delimiter")
            quote_char = load_data_options.get("quote_char")
            load_from_stmt.update({"source_format": source_format})
            load_from_stmt.update({"skip_leading_rows": skip_leading_rows})
            load_from_stmt.update({'autodetect': False})
            load_from_stmt.update({'max_bad_records': max_bad_records})
            load_from_stmt.update({'field_delimiter': field_delimiter})
            load_from_stmt.update({'quote_character': quote_char})

        load_from_file_fpath = loader_obj_conf.get('load_from_stmt_fpath')
        load_from_file_fpath = self.f_handler.replace_file_extension(load_from_file_fpath, '.json')
        loader_obj_conf.update({'load_from_stmt_fpath':load_from_file_fpath})
        self.f_handler.save_dict_to_json(load_from_file_fpath, load_from_stmt)

        return load_from_stmt
    
    def _convert_json_to_newline_delimited(self, json_files, loader_obj_conf):
        """
        Converts JSON array files to newline-delimited JSON format for BigQuery.
        BigQuery requires NEWLINE_DELIMITED_JSON format (one JSON object per line),
        not JSON arrays.
        """
        import json
        
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
                            # Already newline-delimited, skip
                            logger.debug(f"File {json_file} appears to be newline-delimited JSON, skipping conversion")
                            continue
                        else:
                            # Unknown format, skip conversion
                            logger.warning(f"File {json_file} is not in a recognized JSON format, skipping conversion")
                            continue
                
                # Check if it's a JSON array (needs conversion)
                if isinstance(data, list):
                    logger.debug(f"Converting JSON array to newline-delimited format: {json_file}")
                    # Write as newline-delimited JSON (one object per line)
                    with open(json_file, 'w', encoding='utf-8') as f:
                        for obj in data:
                            json.dump(obj, f, ensure_ascii=False)
                            f.write('\n')
                    logger.debug(f"Converted {json_file} to newline-delimited JSON format ({len(data)} objects)")
                elif isinstance(data, dict):
                    # Single object - convert to newline-delimited format
                    logger.debug(f"Converting single JSON object to newline-delimited format: {json_file}")
                    with open(json_file, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False)
                        f.write('\n')
                    logger.debug(f"Converted {json_file} to newline-delimited JSON format")
                # If it's already newline-delimited, leave it as is
                
            except Exception as e:
                logger.warning(f"Error converting JSON file {json_file} to newline-delimited format: {e}")
                # Continue with other files
                continue
