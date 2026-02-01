# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

import time
from abc import ABC, abstractmethod
from petaly.core.composer import Composer
from petaly.utils.utils import measure_time
from petaly.utils.file_handler import FileHandler
from petaly.core.type_mapping import TypeMapping
from petaly.core.object_metadata import ObjectMetadata
from petaly.core.data_object import DataObject
from petaly.core.load_summary import LoadSummary


class DBLoader(ABC):
    """Abstract base class for database loaders.
    
    This class provides the core functionality for loading data into database targets.
    It handles table creation, data loading, and metadata management.
    
    Key responsibilities:
    - Creates and manages database tables
    - Loads data from CSV files into database tables
    - Handles type mapping and data transformation
    - Manages table DDL and loading statements

    """

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.f_handler = FileHandler()
        self.composer = Composer()
        self.m_conf = pipeline.m_conf
        self.object_metadata = ObjectMetadata(pipeline)
        self.type_mapping = TypeMapping(self.pipeline)
        if self.m_conf.set_loader_paths(self.pipeline.target_connector_id):
            self.connector_load_from_stmt_fpath = self.m_conf.connector_load_from_stmt_fpath
            self.connector_create_table_stmt_fpath = self.m_conf.connector_create_table_stmt_fpath

    @abstractmethod
    def load_from(self, object_load_conf):
        pass

    @abstractmethod
    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        pass

    @abstractmethod
    def create_table(self, loader_obj_conf: dict):
        pass

    @measure_time
    def load_data(self):
        """Loads data into the database target.
        
        This method orchestrates the entire loading process:
        1. Gets list of objects to load
        2. For each object:
           - Composes loader configuration
           - Loads data into table
        3. Handles timing and logging
        4. Displays summary of loaded tables
        
        The method supports table recreation if specified in configuration.
        """

        logger.info(f"[--- Load into {self.pipeline.target_connector_id} ---]")
        start_total_time = time.time()
        # 1. get and run all objects
        object_list = self.composer.get_object_list_from_output_dir(self.pipeline)
        
        # Initialize load summary tracker
        load_summary = LoadSummary()

        for object_name in object_list:
            self.load_per_object(object_name, load_summary)

        end_total_time = time.time()
        logger.info(f"Load completed, duration: {round(end_total_time - start_total_time, 2)}s")
        
        # Display summary
        load_summary.display()

    def load_per_object(self, object_name, load_summary=None):
        """Load a single object into the target.
        
        Args:
            object_name: Name of the object to load
            load_summary: Optional LoadSummary instance to track loading progress
        
        Returns:
            tuple: (load_status, rows_loaded, schema_table_name, destination_object_recreated)
        """
        logger.info(f"Load object: {object_name} started...")
        start_time = time.time()
        load_status = 'success'
        rows_loaded = None
        schema_table_name = object_name
        destination_object_recreated = False

        try:
            # 1. compose loader_obj_conf
            loader_obj_conf = self.get_loader_obj_conf(object_name)
            
            # Get schema_table_name for summary
            table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
            schema_name = table_ddl_dict.get('schema_name')
            table_name = table_ddl_dict.get('table_name')
            if schema_name:
                schema_table_name = f"{schema_name}.{table_name}"
            else:
                schema_table_name = table_name
            
            # Track if destination object was recreated
            destination_object_recreated = loader_obj_conf.get('recreate_destination_object', False)

            # 2. load data into table (this may convert Parquet/JSON to CSV first)
            self.load_from(loader_obj_conf)
            
            # 3. Get row count - prefer BigQuery's actual loaded count if available
            # Otherwise count rows from files in output directory
            if loader_obj_conf.get('rows_loaded_from_bigquery') is not None:
                rows_loaded = loader_obj_conf.get('rows_loaded_from_bigquery')
                logger.debug(f"Using BigQuery actual row count: {rows_loaded} rows")
            else:
                # Count rows from CSV files in output directory (after conversion, before summary)
                # This gives us the exact number of rows that were loaded
                output_data_object_dir = loader_obj_conf.get('output_data_object_dir')
                rows_loaded = self.count_rows_in_csv_files(output_data_object_dir, loader_obj_conf)

            end_time = time.time()
            duration_sec = round(end_time - start_time, 2)
            logger.info(f"Load object: {object_name} completed | time: {duration_sec}s")
            
        except Exception as e:
            # Load failed - capture error details
            end_time = time.time()
            duration_sec = round(end_time - start_time, 2)
            load_status = 'failed'
            logger.error(f"Load object: {object_name} failed | time: {duration_sec}s | error: {str(e)}", exc_info=True)
            
            # Try to get schema_table_name even if load failed (for summary display)
            try:
                if schema_table_name == object_name:
                    loader_obj_conf = self.get_loader_obj_conf(object_name)
                    table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
                    schema_name = table_ddl_dict.get('schema_name')
                    table_name = table_ddl_dict.get('table_name')
                    if schema_name:
                        schema_table_name = f"{schema_name}.{table_name}"
                    else:
                        schema_table_name = table_name
                    destination_object_recreated = loader_obj_conf.get('recreate_destination_object', False)
            except Exception:
                # If we can't even get the schema_table_name, use object_name as fallback
                schema_table_name = object_name
                destination_object_recreated = False
        
        # Add to summary if provided
        if load_summary is not None:
            # Get source and target connection names
            source_connection_name = self.pipeline.source_attr.get('connection_name')
            if not source_connection_name:
                source_connection_name = 'inline'
            
            target_connection_name = self.pipeline.target_attr.get('connection_name')
            if not target_connection_name:
                target_connection_name = 'inline'
            
            # Source object name is the object_name (extracted from source)
            source_object_name = object_name
            
            # Target object name is the schema_table_name
            target_object_name = schema_table_name
            
            load_summary.add_entry(
                source_connection=source_connection_name,
                source_object=source_object_name,
                target_connection=target_connection_name,
                target_object=target_object_name,
                recreated=destination_object_recreated,
                rows_loaded=rows_loaded,
                duration_sec=duration_sec if 'duration_sec' in locals() else round(end_time - start_time, 2),
                status=load_status,
                start_time=start_time,
                end_time=end_time
            )
        
        return load_status, rows_loaded, schema_table_name, destination_object_recreated

    def get_loader_obj_conf(self, object_name) ->dict:
        """Gets the configuration for loading a specific object.
        
        The method composes a complete configuration containing:
        - Object name and paths
        - Metadata directory
        - Table DDL components
        - Object settings
        - Load statements
        - Blob prefix for cloud storage
        """
        loader_obj_conf = {}
        loader_obj_conf.update({'object_name': object_name})

        output_metadata_object_dir = self.pipeline.output_object_metadata_dpath.format(object_name=object_name)
        loader_obj_conf.update({'output_metadata_object_dir': output_metadata_object_dir})

        output_data_object_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
        loader_obj_conf.update({'output_data_object_dir': output_data_object_dir})

        # 2. search for metadata file and load table metadata
        metadata_file = self.pipeline.output_object_metadata_fpath.format(object_name=object_name)
        table_metadata = self.f_handler.load_file_as_dict(metadata_file, 'json')

        # 3. compose table DDL components
        data_object = DataObject(self.pipeline, object_name)
        table_ddl_dict = self.compose_table_ddl(data_object, table_metadata)
        loader_obj_conf.update({'table_ddl_dict': table_ddl_dict})

        # 4. object_spec and default_settings
        logger.debug(f"The object settings combined with default settings: {data_object.object_settings}")
        loader_obj_conf.update({'object_settings': data_object.object_settings})

        # Always set recreate_destination_object in loader_obj_conf (True or False)
        loader_obj_conf.update({'recreate_destination_object': bool(data_object.recreate_destination_object)})

        # 5. compose statement load_from
        output_load_from_stmt_fpath = self.pipeline.output_load_from_stmt_fpath.format(object_name=object_name)
        loader_obj_conf.update({'load_from_stmt_fpath': output_load_from_stmt_fpath})

        load_from_stmt = self.compose_load_from_stmt(data_object, loader_obj_conf)
        loader_obj_conf.update({'load_from_stmt': load_from_stmt})

        blob_prefix = self.composer.compose_bucket_object_path(self.pipeline.target_attr.get('bucket_pipeline_prefix'),
                                                                self.pipeline.pipeline_name,
                                                                object_name)
        loader_obj_conf.update({'blob_prefix': blob_prefix})

        return loader_obj_conf

    def get_data_object(self, object_name):
        """Gets a DataObject instance for the specified object.
        
        Creates and returns a DataObject instance containing the object's
        configuration and settings.
        """
        return DataObject(self.pipeline, object_name)

    def compose_table_ddl(self, data_object, table_metadata: dict) -> (dict):
        """Composes the DDL statement for creating a table.
        
        The method creates table DDL components including:
        - Table name
        - Schema name
        - Column definitions
        - Primary key
        """

        object_name = data_object.object_name
        table_name = data_object.destination_object_name

        if table_name is None:
            # table_name = table_metadata.get('source_object_name')
            table_name = object_name

        schema_name = self.pipeline.target_attr.get('database_schema')

        if schema_name is not None:
            schema_table_name = f"{schema_name}.{table_name}"
        else:
            schema_table_name = table_name

        logger.debug(f"Compose DDL table: {schema_table_name}")

        table_ddl_dict = {}
        table_ddl_dict.update({'table_name': table_name})
        table_ddl_dict.update({'schema_name': schema_name})
        columns_meta_arr = table_metadata.get('columns')
        connector_create_table_stmt_fpath = self.f_handler.load_file(self.connector_create_table_stmt_fpath)

        type_mapping = self.type_mapping.get_type_mapping()
        column_list = ""
        column_datatype_list = ""
        primary_key = ''

        # loop each line
        for i, column_meta in enumerate(columns_meta_arr):

            column_name = self.db_connector.metaquery_quote + self.composer.normalise_column_name(column_meta.get('column_name')) + self.db_connector.metaquery_quote
            column_list += column_name

            column_datatype_list += column_name
            column_type = type_mapping.get(column_meta.get('data_type'))

            if column_type is None:
                logger.error(f"Type mapping doesn't exists for source-connector-id: {self.pipeline.source_connector_id}, table: {table_name}, "
                              f"column: {column_name}, data-type: {column_meta.get('data_type')}, "
                              f"target_connector_id {self.pipeline.target_connector_id}."
                              )
                # Skip this column if type mapping is not found to avoid concatenation error
                continue

            column_datatype_list += " " + column_type
            mode = ' NOT NULL' if column_meta.get('is_nullable') == 'NO' else ''
            column_datatype_list += mode

            if column_meta.get('primary_key') != None:
                primary_key += column_meta.get('primary_key') + ','

            column_datatype_list += ",\n"
            column_list  += ",\n"

        if primary_key != '':
            primary_key = primary_key.rstrip(',')
            # ToDo: recreate generic primary key by adding ALTER TABLE
            primary_key = f"PRIMARY KEY ({primary_key})"
            #column_datatype_list += primary_key

        column_datatype_list = column_datatype_list.rstrip(',\n')
        column_list = column_list.rstrip(',\n')

        create_table_stmt_fpath = self.pipeline.output_create_table_stmt_fpath.format(object_name=object_name)
        table_ddl_dict.update({'column_datatype_list':column_datatype_list, 'primary_key':primary_key, 'column_list':column_list})
        table_ddl_dict.update({'create_table_stmt_fpath': create_table_stmt_fpath, 'create_table_stmt': connector_create_table_stmt_fpath})

        logger.debug(f"The DDL for table: {schema_table_name} was composed")
        return table_ddl_dict

    def get_column_type_with_precision(self, column_meta, type_mapping):
        """ This functions currently is not in use. """
        column_type = type_mapping.get(column_meta.get('data_type'))
        column_type_with_precision = column_type

        if column_type in ('character varying', 'varchar'):
            max_value = '21845'

            if column_meta.get('character_maximum_length') is not None:
                column_type_with_precision = column_type + '(' + column_meta.get('character_maximum_length') + ')'
            else:
                # ToDo to find a way to get max values for each database type
                column_type_with_precision = column_type + '(' + max_value + ')'

        return column_type_with_precision

    def compose_column_list(self, table_meta):
        """ It's a sub function to compose column list """
        columns_meta_arr = table_meta.get('columns')
        columns_list = ''
        for i, column_meta in enumerate(columns_meta_arr):
            column_name = self.composer.normalise_column_name(column_meta.get('column_name'))
            columns_list += self.db_connector.metaquery_quote + column_name + self.db_connector.metaquery_quote
            columns_list += ","

        columns_list = columns_list.rstrip(',')
        return columns_list
    
    def count_rows_in_csv_files(self, output_data_object_dir, loader_obj_conf):
        """
        Counts the total number of rows in files in the output directory.
        Supports CSV/TSV files and Parquet/JSON files (which will be converted to CSV).
        This gives the exact number of rows that were extracted and will be loaded.
        
        Args:
            output_data_object_dir: Directory containing files
            loader_obj_conf: Loader configuration containing object settings
            
        Returns:
            Total number of data rows (excluding headers if header=true)
        """
        try:
            import gzip
            import glob
            import os
            
            # Get object settings to check if header is present
            object_settings = loader_obj_conf.get('object_settings', {})
            has_header = object_settings.get('header', True)
            
            # Get all files in the directory
            all_files = []
            # Check for CSV/TSV files first (already converted)
            for pattern in ['*.csv', '*.tsv', '*.txt', '*.csv.gz', '*.tsv.gz', '*.txt.gz']:
                all_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
            
            # If no CSV files found, check for Parquet/JSON files (before conversion)
            if not all_files:
                for pattern in ['*.parquet', '*.parq', '*.json']:
                    all_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
            
            # Remove duplicates and filter to only files (not directories), exclude hidden files
            files_to_count = list(set([f for f in all_files if os.path.isfile(f) and not os.path.basename(f).startswith('.')]))
            
            if not files_to_count:
                logger.debug(f"No files found in {output_data_object_dir}")
                return 0
            
            total_rows = 0
            
            for file_path in files_to_count:
                try:
                    # Handle Parquet files
                    if file_path.endswith(('.parquet', '.parq')):
                        import pyarrow.parquet as pq
                        parquet_file = pq.ParquetFile(file_path)
                        row_count = parquet_file.metadata.num_rows
                        total_rows += row_count
                        logger.debug(f"File {file_path}: {row_count} rows (Parquet)")
                        continue
                    
                    # Handle JSON files
                    if file_path.endswith('.json'):
                        import json
                        with open(file_path, 'r', encoding='utf-8') as f:
                            # Try reading as array of objects first
                            try:
                                data = json.load(f)
                                if isinstance(data, list):
                                    row_count = len(data)
                                else:
                                    # Single object, count as 1
                                    row_count = 1
                            except json.JSONDecodeError:
                                # Try newline-delimited JSON - reset file pointer BEFORE counting
                                f.seek(0)
                                row_count = sum(1 for line in f if line.strip())
                        total_rows += row_count
                        logger.debug(f"File {file_path}: {row_count} rows (JSON)")
                        continue
                    
                    # Handle CSV/TSV files (text-based)
                    is_gzipped = file_path.endswith('.gz')
                    
                    if is_gzipped:
                        # Count lines in gzipped file
                        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                            line_count = sum(1 for _ in f)
                    else:
                        # Count lines in regular delimited file
                        with open(file_path, 'r', encoding='utf-8') as f:
                            line_count = sum(1 for _ in f)
                    
                    # Subtract header if present
                    if has_header and line_count > 0:
                        line_count -= 1
                    
                    total_rows += line_count
                    logger.debug(f"File {file_path}: {line_count} rows")
                    
                except Exception as e:
                    logger.debug(f"Error counting rows in {file_path}: {e}")
                    continue
            
            return total_rows
            
        except Exception as e:
            logger.debug(f"Error counting rows in files from {output_data_object_dir}: {e}")
            return None
    
    def table_exists(self, schema_table_name: str) -> bool:
        """
        Check if a table exists in the target database.
        
        Args:
            schema_table_name: Full table name including schema (e.g., 'schema.table')
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            # Try to query the table - if it exists, query succeeds
            # Use a simple SELECT 1 query that's fast and doesn't return data
            if self.pipeline.target_connector_id == 'bigquery':
                check_query = f"SELECT 1 FROM `{schema_table_name}` LIMIT 1"
                try:
                    result = self.db_connector.get_metadata_result(check_query)
                    return result is not None and len(result) >= 0  # Even empty result means table exists
                except Exception:
                    return False
            elif self.pipeline.target_connector_id == 'redshift' and hasattr(self.db_connector, 'execute_sql') and hasattr(self.db_connector, 'is_serverless'):
                # Redshift IAM
                check_query = f"SELECT 1 FROM {schema_table_name} LIMIT 1"
                try:
                    result_data, request_id = self.db_connector.execute_sql(check_query, sleep_sec=1)
                    return result_data is not None
                except Exception:
                    return False
            else:
                # PostgreSQL, MySQL, Redshift TCP
                check_query = f"SELECT 1 FROM {schema_table_name} LIMIT 1"
                try:
                    result = self.db_connector.get_query_result(check_query)
                    return result is not None
                except Exception:
                    return False
        except Exception as e:
            logger.debug(f"Error checking if table '{schema_table_name}' exists: {e}")
            return False
    
    def get_table_row_count(self, schema_table_name):
        """
        Gets the total row count from a table.
        Used to calculate incremental rows loaded (after - before).
        
        Args:
            schema_table_name: Full table name including schema (e.g., 'schema.table')
            
        Returns:
            Number of rows in the table, or None if table doesn't exist or unable to determine
        """
        try:
            # BigQuery uses get_metadata_result instead of get_query_result
            if self.pipeline.target_connector_id == 'bigquery':
                count_query = f"SELECT COUNT(*) as row_count FROM `{schema_table_name}`"
                result = self.db_connector.get_metadata_result(count_query)
                if result and len(result) > 0:
                    row_count = result[0].get('row_count')
                    return int(row_count) if row_count is not None else None
                return None
            # Redshift IAM connector uses execute_sql which returns (result_data, request_id)
            elif self.pipeline.target_connector_id == 'redshift' and hasattr(self.db_connector, 'execute_sql') and hasattr(self.db_connector, 'is_serverless'):
                count_query = f"SELECT COUNT(*) as row_count FROM {schema_table_name}"
                result_data, request_id = self.db_connector.execute_sql(count_query, sleep_sec=1)
                if result_data and result_data.get('Records'):
                    # Redshift IAM returns result_data with Records array
                    # Each record is a list of dicts with 'stringValue' or other value types
                    records = result_data.get('Records', [])
                    if records and len(records) > 0:
                        first_record = records[0]
                        if first_record and len(first_record) > 0:
                            # Get the first value from the first record
                            value_dict = first_record[0]
                            # Value can be in 'stringValue', 'longValue', 'doubleValue', etc.
                            row_count = value_dict.get('longValue') or value_dict.get('stringValue') or value_dict.get('doubleValue')
                            if row_count is not None:
                                return int(row_count)
                return None
            else:
                # For other connectors (PostgreSQL, MySQL, Redshift TCP), use get_query_result
                count_query = f"SELECT COUNT(*) as row_count FROM {schema_table_name}"
                result = self.db_connector.get_query_result(count_query)
                
                if result and len(result) > 0:
                    # Handle different result formats from different connectors
                    first_row = result[0]
                    if isinstance(first_row, dict):
                        # Redshift TCP returns dicts
                        row_count = first_row.get('row_count')
                    elif isinstance(first_row, (list, tuple)):
                        # PostgreSQL, MySQL return tuples
                        row_count = first_row[0]
                    else:
                        row_count = first_row
                    
                    return int(row_count) if row_count is not None else None
                return None
        except Exception as e:
            # Table might not exist yet (for before count) - this is expected
            logger.debug(f"Could not get row count for {schema_table_name}: {e}")
            return None
    

