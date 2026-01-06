# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

import os
import sys
import time
from abc import ABC, abstractmethod
from petaly.core.composer import Composer
from petaly.utils.utils import measure_time
from petaly.utils.file_handler import FileHandler
from petaly.core.object_metadata import ObjectMetadata
from petaly.core.type_mapping import TypeMapping
from petaly.core.data_object import DataObject


class DBExtractor(ABC):
    """Abstract base class for database extractors.
    
    This class provides the core functionality for extracting data from database sources.
    It handles metadata extraction, query composition, and data export to CSV files.
    
    Key responsibilities:
    - Extracts metadata from database objects
    - Composes and executes extraction queries
    - Manages file output and metadata storage
    - Handles type mapping and data transformation
    
    Attributes:
        pipeline: The pipeline instance containing configuration and state
        f_handler: FileHandler instance for file operations
        composer: Composer instance for query composition
        m_conf: Main configuration instance
        type_mapping: TypeMapping instance for data type conversions
        object_metadata: ObjectMetadata instance for metadata management
    """

    def __init__(self, pipeline):
        super().__init__()

        self.pipeline = pipeline
        self.f_handler = FileHandler()
        self.composer = Composer()
        self.m_conf = self.pipeline.m_conf
        self.type_mapping = TypeMapping(pipeline)
        self.object_metadata = ObjectMetadata(pipeline)

        if self.m_conf.set_extractor_paths(self.pipeline.source_connector_id):
            self.connector_extract_to_stmt_fpath = self.m_conf.connector_extract_to_stmt_fpath
            self.connector_metadata_sql_fpath = self.m_conf.connector_metadata_sql_fpath
            self.query_origin = self.f_handler.load_file(self.connector_metadata_sql_fpath)

    @abstractmethod
    def extract_to(self, extractor_obj_conf):
        pass

    @abstractmethod
    def get_query_result(self, meta_query):
        pass

    @abstractmethod
    def compose_extract_to_stmt(self, extract_to_stmt, extract_config) -> dict:
        pass

    @measure_time
    def extract_data(self):
        """Extracts data from the database source and exports it to CSV files.
        
        This method orchestrates the entire extraction process:
        1. Cleans up the pipeline output directory
        2. Composes and executes metadata queries
        3. Processes metadata for each object
        4. Extracts data for each object to CSV files
        
        The method handles timing and logging of the extraction process.
        """

        logger.info(f"[--- Extract from {self.pipeline.source_connector_id} ---]")
        start_total_time = time.time()
        # 1. Start with cleanup
        self.f_handler.cleanup_dir(self.pipeline.output_pipeline_dpath)

        # 2. compose_extract_scripts
        meta_query = self.compose_meta_query()

        # 3. get meta query result, expected as a dict
        meta_query_result = self.execute_meta_query(meta_query)

        # 4. save metadata and export scripts
        object_list = self.object_metadata.process_metadata(meta_query_result)

        # 5. run loop for each object
        for object_name in object_list:
            self.extract_per_object(object_name)

        end_total_time = time.time()
        logger.info(f"Extract completed, duration: {round(end_total_time - start_total_time, 2)}s")

    def extract_per_object(self, object_name):
        """Extract a single object from the source.
        
        Args:
            object_name: Name of the object to extract
        """
        logger.info(f"Extract object: {object_name} started...")
        start_time = time.time()

        # Get all export scripts and store data into output directory
        extractor_obj_conf = self.get_extractor_obj_conf(object_name)

        # Run export data
        self.extract_to(extractor_obj_conf)
        
        # Convert to target format if needed (database extractors write CSV, but target might be parquet/json)
        self._convert_to_target_format(extractor_obj_conf, object_name)

        end_time = time.time()
        logger.info(f"Extract object: {object_name} completed | time: {round(end_time - start_time, 2)}s")

    def execute_meta_query(self, meta_query):
        """Executes a metadata query and returns the results.
        
        The method executes the provided SQL query for metadata extraction.
        If the query cannot be executed, it raises a SystemExit error.
        """
        logger.debug("Execute meta-query and create extract scripts")
        if meta_query is not None:
            query_result = self.get_query_result(meta_query)

        else:
            logger.error(
                f"Meta Query for pipeline {self.pipeline.pipeline_name} can not be executed. Review your configuration.")

            query_result = None
        return query_result

    def get_extractor_obj_conf(self, object_name) ->dict:
        """Gets the configuration for extracting a specific object.
        
        The method composes a complete configuration dictionary containing:
        - Object name and paths
        - Metadata directory
        - Extract queries
        - Object settings
        - Blob prefix for cloud storage
        - Extract statements
        """

        extractor_obj_conf = {'object_name': object_name}

        # 1. compose metadata directory
        output_metadata_object_dir = self.pipeline.output_object_metadata_dpath.format(object_name=object_name)
        extractor_obj_conf.update({'output_metadata_object_dir': output_metadata_object_dir})
        metadata_fpath = self.pipeline.output_object_metadata_fpath.format(object_name=object_name)

        # 2. get and compose metadata query
        table_metadata = self.f_handler.load_file_as_dict(metadata_fpath, 'json')
        extract_queries_dict = self.compose_extract_queries(table_metadata)
        extractor_obj_conf.update(extract_queries_dict)

        # 3. add csv_default_settings
        #extractor_obj_conf.update({'object_settings': table_metadata.get('object_settings')})
        data_object = self.get_data_object(object_name)
        logger.debug(f"The object settings combined with default settings: {data_object.object_settings}")
        extractor_obj_conf.update({'object_settings': data_object.object_settings})

        # blob-prefix, used for storage in cloud services (e.g. Redshift (s3), Bigquery (GCS))
        blob_prefix = self.composer.compose_bucket_object_path(self.pipeline.source_attr.get('bucket_pipeline_prefix'),
                                                            self.pipeline.pipeline_name,
                                                            object_name)
        extractor_obj_conf.update({'blob_prefix': blob_prefix})

        # 4. load stmt_extract_to.txt and transform it in later stage
        extract_to_stmt = self.f_handler.load_file(self.connector_extract_to_stmt_fpath)
        extract_to_stmt = self.compose_extract_to_stmt(extract_to_stmt, extractor_obj_conf)
        extractor_obj_conf.update({'extract_to_stmt': extract_to_stmt})

        # 5.  save extract_to_stmt under output_extract_to_file_fpath
        output_extract_to_stmt_fpath = self.pipeline.output_extract_to_stmt_fpath.format(object_name=object_name)
        extractor_obj_conf.update({'extract_to_stmt_fpath': output_extract_to_stmt_fpath})
        self.f_handler.save_file(output_extract_to_stmt_fpath, extract_to_stmt)

        # 6. compose output data dir
        output_data_object_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
        extractor_obj_conf.update({'output_data_object_dir': output_data_object_dir})
        self.f_handler.make_dirs(output_data_object_dir)

        # compose output object file path
        # Check if source connector supports direct export in target format
        # BigQuery and Redshift can export directly to Parquet/JSON
        source_connector_id = self.pipeline.source_connector_id
        target_connector_id = self.pipeline.target_connector_id
        target_category = self.m_conf.get_connector_category(target_connector_id)
        
        # Determine file extension
        file_extension = '.csv'  # Default to CSV
        if source_connector_id in ('bigquery', 'redshift') and target_category == 'file' and target_connector_id in ('parquet', 'json'):
            # BigQuery and Redshift can export directly to Parquet/JSON
            file_extension = f'.{target_connector_id}'
            logger.debug(f"Source connector {source_connector_id} will export directly to {target_connector_id} format")
        
        output_object_fpath = os.path.join(output_data_object_dir, object_name + file_extension)
        extractor_obj_conf.update({'output_object_fpath': output_object_fpath})

        logger.debug(f"Config for data extract: {extractor_obj_conf}")
        return extractor_obj_conf

    def compose_meta_query(self):
        """Composes the metadata query based on pipeline configuration.
        
        The query is built using:
        - Schema information
        - Table list (if specified)
        - Column definitions
        """
        def get_table_stmt(data_objects_list):
            if len(data_objects_list)>0:
                table_stmt = 'AND tb.table_name IN ({tbl_list})'
                table_string = ''
                for tbl in data_objects_list:
                    table_string += "'" + tbl + "',"
                         
                table_string = table_string.rstrip(',')
                return table_stmt.format(tbl_list=table_string)
            else:
                return ''

        logger.debug("Compose data source meta query:")
        logger.debug(f"use_data_objects_spec={self.pipeline.use_data_objects_spec}, data_objects={self.pipeline.data_objects}, data_objects_from_cli={self.pipeline.data_objects_from_cli}")
        
        # if data_objects_from_cli is set, use it to compose the table_stmt and ignore all other settings
        if len(self.pipeline.data_objects_from_cli)>0:
            table_stmt = get_table_stmt(self.pipeline.data_objects_from_cli)
            logger.debug(f"Using CLI objects: {self.pipeline.data_objects_from_cli}, table_stmt: {table_stmt}")
        
        # Check if use_data_objects_spec is 'prefer' (load all tables from schema, use spec if exists)
        elif self.pipeline.use_data_objects_spec == 'prefer':
            # use_data_objects_spec='prefer': load all tables from schema
            # Specifications from data_objects_spec will be applied where available
            table_stmt = ''
            if len(self.pipeline.data_objects) > 0:
                logger.debug(f"use_data_objects_spec='prefer': loading all tables from schema, with custom specs for: {self.pipeline.data_objects}")
            else:
                logger.debug("use_data_objects_spec='prefer': loading all tables from schema with default specifications")
            
        # use_data_objects_spec is 'strict' - load only objects in data_objects_spec[]
        else:
            if len(self.pipeline.data_objects)==0:
                logger.error(f"Pipeline {self.pipeline.pipeline_name} in {self.pipeline.pipeline_fpath} wasn't specified properly. If use_data_objects_spec is set to 'strict', the data_objects_spec[] should have at least one object specification")
                sys.exit()
			
            table_stmt = get_table_stmt(self.pipeline.data_objects)
            logger.debug(f"use_data_objects_spec='strict': loading only specified objects: {self.pipeline.data_objects}, table_stmt: {table_stmt}")

        source_schema = self.pipeline.source_attr.get('database_schema')

        if source_schema is None:
            if self.f_handler.check_dict_key_exist(self.pipeline.source_attr, 'database_schema'):
                logger.warning(f"A source database schema wasn't specified. To continue, specify database_schema in pypeline.yaml ")
                sys.exit()

            source_schema = self.pipeline.source_attr.get('database_name')

        meta_query = self.query_origin.format(schema=source_schema, table_statement_list=table_stmt)

        logger.debug(f"Meta Query:\n {meta_query}")

        return meta_query

    @measure_time
    def compose_extract_queries(self, dict_obj):
        """Composes extraction queries for a specific object.
        
        The method processes object metadata to create:
        - Source schema and object names
        - Column list with type transformations
        """
        extract_obj_conf = {}

        if dict_obj.get('source_object_name') is not None:
            column_list = ''

            transformation = self.type_mapping.get_extractor_type_transformer()
            col_obj = dict_obj.get('columns')

            for idx, val in enumerate(col_obj):

                column_transformation = transformation.get(val['data_type'])

                if column_transformation is not None:
                    column_list += column_transformation.format(column_name=val['column_name']) + ","
                else:
                    column_list += f" {self.db_connector.metaquery_quote}{val['column_name']}{self.db_connector.metaquery_quote},"
                    #columns += self.normalise_column_name(val['column_name']) + ","

            column_list = column_list.rstrip(',')

            extract_obj_conf.update({
                                    'source_schema_name':dict_obj['source_schema_name'],
                                    'source_object_name':dict_obj['source_object_name'],
                                    'column_list':column_list})

            return extract_obj_conf

    def get_data_object(self, object_name):
        """Gets a DataObject instance for the specified object.
        
        Creates and returns a DataObject instance containing the object's
        configuration and settings.
        """
        return DataObject(self.pipeline, object_name)
    
    def _get_output_file_extension(self):
        """Gets the output file extension based on target connector type.
        
        Logic:
        1. Check if target connector is a file connector (csv, parquet, json)
        2. If yes, use that format's extension
        3. Otherwise, default to .csv for database connectors
        """
        target_connector_id = self.pipeline.target_connector_id
        connector_category = self.m_conf.get_connector_category(target_connector_id)
        
        # If target is a file connector, use its format
        if connector_category == 'file':
            # File connectors: csv -> .csv, parquet -> .parquet, json -> .json
            if target_connector_id in ('csv', 'parquet', 'json'):
                return f'.{target_connector_id}'
        
        # Default to .csv for database connectors or unknown types
        return '.csv'
    
    def _convert_to_target_format(self, extractor_obj_conf, object_name):
        """Converts extracted CSV file to target format if needed.
        
        Database extractors typically write CSV files, but some database connectors
        (BigQuery, Redshift) can export directly to Parquet/JSON format.
        
        IMPORTANT: This method is only called for DBExtractor (database sources).
        File extractors (ParquetExtractor, JsonExtractor) already extract directly
        in their native format, so no conversion is needed.
        
        Logic:
        1. Check if source connector supports direct extraction in target format
        2. If yes, skip conversion (source already extracts in correct format)
        3. If no (database source that only exports CSV), convert CSV to target format if needed
        """
        source_connector_id = self.pipeline.source_connector_id
        target_connector_id = self.pipeline.target_connector_id
        source_category = self.m_conf.get_connector_category(source_connector_id)
        target_category = self.m_conf.get_connector_category(target_connector_id)
        
        # If source is a file connector that supports the target format directly, no conversion needed
        # (e.g., ParquetExtractor -> parquet target, JsonExtractor -> json target)
        if source_category == 'file' and source_connector_id == target_connector_id:
            logger.debug(f"Source connector {source_connector_id} already extracts in target format {target_connector_id}. No conversion needed.")
            return
        
        # Check if source is BigQuery or Redshift and target is Parquet/JSON
        # These connectors can export directly in these formats
        if source_connector_id in ('bigquery', 'redshift') and target_category == 'file' and target_connector_id in ('parquet', 'json'):
            logger.debug(f"Source connector {source_connector_id} exports directly to {target_connector_id} format. No conversion needed.")
            return
        
        # Only convert if:
        # 1. Source is a database connector that only exports to CSV (mysql, postgres, etc.)
        # 2. Target is a file connector and not CSV
        if source_category == 'database' and target_category == 'file' and target_connector_id in ('parquet', 'json'):
            csv_fpath = extractor_obj_conf.get('output_object_fpath')
            if not csv_fpath or not os.path.exists(csv_fpath):
                logger.warning(f"CSV file not found for conversion: {csv_fpath}")
                return
            
            # Get target file path with correct extension
            output_data_object_dir = extractor_obj_conf.get('output_data_object_dir')
            target_fpath = os.path.join(output_data_object_dir, object_name + f'.{target_connector_id}')
            
            logger.info(f"Converting {csv_fpath} to {target_fpath} (format: {target_connector_id})")
            
            try:
                if target_connector_id == 'parquet':
                    self._convert_csv_to_parquet(csv_fpath, target_fpath, extractor_obj_conf)
                elif target_connector_id == 'json':
                    self._convert_csv_to_json(csv_fpath, target_fpath, extractor_obj_conf)
                
                # Keep CSV in workspace, but update the output path in config for loader
                # The loader will handle filtering out CSV files when copying to target directory
                if os.path.exists(target_fpath):
                    logger.debug(f"Conversion complete. CSV file kept in workspace: {csv_fpath}, {target_connector_id} file created: {target_fpath}")
                    # Update the output path in config for loader (but CSV still exists in workspace)
                    extractor_obj_conf['output_object_fpath'] = target_fpath
            except Exception as e:
                logger.error(f"Error converting {csv_fpath} to {target_connector_id}: {e}")
                raise
    
    def _convert_csv_to_parquet(self, csv_fpath, parquet_fpath, extractor_obj_conf):
        """Converts CSV file to Parquet format."""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            import csv
            
            object_settings = extractor_obj_conf.get('object_settings', {})
            delimiter = object_settings.get('columns_delimiter', ',')
            
            # Get quote settings to match how CSV was written
            columns_quote = object_settings.get('columns_quote', 'double')
            
            # CRITICAL: When columns_quote is 'none', fields with commas (like JSON) will be split
            # We need to detect this and handle it intelligently
            # First, read the header to determine expected column count
            has_header = object_settings.get('header', True)
            expected_cols = None
            if has_header:
                with open(csv_fpath, 'r', encoding='utf-8') as f:
                    header_line = f.readline().strip()
                    expected_cols = len(header_line.split(delimiter))
                    logger.debug(f"CSV header indicates {expected_cols} expected columns")
            
            # Detect if manual parser is needed by analyzing the CSV file structure
            # When fields contain commas and are unquoted, they may be escaped (e.g., \,)
            # Pandas will split at all commas, including escaped ones, causing corruption
            # We detect this dynamically by analyzing the actual CSV file content
            force_manual_parser = False
            
            if expected_cols:
                # Analyze CSV structure to detect if manual parsing is needed
                try:
                    with open(csv_fpath, 'r', encoding='utf-8') as f:
                        if has_header:
                            header_line = f.readline().strip()
                        first_data_line = f.readline().strip()
                        
                        if first_data_line:
                            # Detection method 1: Check for escaped comma patterns (\,)
                            # This indicates fields with commas were escaped during extraction
                            if '\\,' in first_data_line:
                                force_manual_parser = True
                                logger.info(f"Detected escaped commas (\,) in CSV data - will use manual parser to preserve field integrity")
                            
                            # Detection method 2: Count delimiters vs expected columns
                            # If there are significantly more delimiters than expected columns,
                            # it suggests fields contain unescaped commas that will be split incorrectly
                            delimiter_count = first_data_line.count(delimiter)
                            if not force_manual_parser and delimiter_count >= expected_cols * 2:
                                force_manual_parser = True
                                logger.info(f"Detected excessive delimiters ({delimiter_count} vs {expected_cols} expected columns) - "
                                          f"will use manual parser to prevent incorrect field splitting")
                            
                            # Detection method 3: Try a test parse to see if pandas produces wrong column count
                            # This catches cases where escaped commas aren't visible but still cause issues
                            if not force_manual_parser:
                                try:
                                    test_df = pd.read_csv(csv_fpath, nrows=1, delimiter=delimiter, 
                                                         dtype=str, quoting=csv.QUOTE_NONE if columns_quote == 'none' else csv.QUOTE_MINIMAL,
                                                         engine='python', header=0 if has_header else None)
                                    if len(test_df.columns) != expected_cols:
                                        force_manual_parser = True
                                        logger.info(f"Test parse detected column mismatch ({len(test_df.columns)} vs {expected_cols} expected) - "
                                                  f"will use manual parser to ensure correct parsing")
                                except Exception:
                                    # If test parse fails, it's likely due to inconsistent field counts
                                    force_manual_parser = True
                                    logger.info(f"Test parse failed - will use manual parser to handle inconsistent field counts")
                                    
                except Exception as e:
                    logger.debug(f"Could not analyze CSV structure: {e}")
                    # If analysis completely fails, we'll rely on pandas attempt and fallback logic below
            
            # Determine quoting settings
            if columns_quote == 'double':
                quotechar = '"'
                quoting = csv.QUOTE_MINIMAL
            elif columns_quote == 'single':
                quotechar = "'"
                quoting = csv.QUOTE_MINIMAL
            else:
                # columns_quote is 'none' - but we may still need to handle quoted fields
                # if the CSV file contains fields with commas
                quotechar = None
                quoting = csv.QUOTE_NONE
            
            # Escape character (MySQL uses backslash)
            escapechar = '\\' if quotechar else None
            
            # Read CSV with same settings as extraction
            read_csv_kwargs = {
                'delimiter': delimiter,
                'dtype': str,
                'keep_default_na': False,
                'na_values': [],
                'quoting': quoting,
                'engine': 'python'  # Use Python engine for better handling
            }
            
            if quotechar:
                read_csv_kwargs['quotechar'] = quotechar
            if escapechar:
                read_csv_kwargs['escapechar'] = escapechar
            
            if has_header:
                read_csv_kwargs['header'] = 0
            else:
                read_csv_kwargs['header'] = None
            
            # Handle bad lines
            try:
                read_csv_kwargs['on_bad_lines'] = 'skip'
            except TypeError:
                try:
                    read_csv_kwargs['error_bad_lines'] = False
                except TypeError:
                    pass
            
            logger.debug(f"Reading CSV with settings: delimiter={delimiter}, quotechar={quotechar}, escapechar={escapechar}, quoting={quoting}, engine=python")
            
            # Read CSV and check if column count matches expected
            df = None
            use_manual_parser = False
            
            import warnings
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    df = pd.read_csv(csv_fpath, **read_csv_kwargs)
                    
                    # Check if we got more columns than expected (indicates fields with commas were split)
                    if expected_cols and len(df.columns) > expected_cols:
                        logger.warning(f"CSV has {len(df.columns)} columns but header indicates {expected_cols} columns. "
                                     f"This suggests fields with commas were split (likely due to columns_quote: none). "
                                     f"Will use manual parser...")
                        use_manual_parser = True
                    elif expected_cols and len(df.columns) != expected_cols:
                        logger.warning(f"CSV has {len(df.columns)} columns but expected {expected_cols}. "
                                     f"Will use manual parser...")
                        use_manual_parser = True
                    
                    # Try reading with QUOTE_MINIMAL as fallback if we got wrong column count
                    if use_manual_parser:
                        read_csv_kwargs_fallback = read_csv_kwargs.copy()
                        read_csv_kwargs_fallback['quoting'] = csv.QUOTE_MINIMAL
                        read_csv_kwargs_fallback['quotechar'] = '"'
                        
                        try:
                            df_fallback = pd.read_csv(csv_fpath, **read_csv_kwargs_fallback)
                            if len(df_fallback.columns) == expected_cols:
                                logger.info(f"Fallback reading with QUOTE_MINIMAL succeeded: {len(df_fallback.columns)} columns match expected {expected_cols}")
                                df = df_fallback
                                use_manual_parser = False
                            else:
                                logger.warning(f"Fallback reading still has {len(df_fallback.columns)} columns, expected {expected_cols}. Will use manual parser.")
                        except Exception as e:
                            logger.warning(f"Fallback reading failed: {e}. Will use manual parser.")
                    
                    # Log summary of skipped lines if any
                    skipped_lines = [str(warning.message) for warning in w if 'Skipping line' in str(warning.message)]
                    if skipped_lines:
                        logger.warning(f"CSV parsing skipped {len(skipped_lines)} malformed lines. "
                                     f"These lines had incorrect field counts and were excluded from conversion. "
                                     f"First few problematic lines: {skipped_lines[:5]}")
                        use_manual_parser = True
                        
                except Exception as e:
                    # Pandas failed to read (likely due to inconsistent field counts)
                    logger.warning(f"Pandas failed to read CSV with QUOTE_NONE: {e}. "
                                 f"This is expected when columns_quote: none with comma-containing data. "
                                 f"Will use manual parser...")
                    use_manual_parser = True
            
            # Use manual parser if needed
            if force_manual_parser or use_manual_parser or (expected_cols and (df is None or len(df.columns) != expected_cols)):
                if df is not None:
                    logger.warning(f"CSV has {len(df.columns)} columns but expected {expected_cols}. "
                                 f"This indicates fields with commas were split (columns_quote: none with comma-containing data). "
                                 f"Attempting manual line-by-line parsing to reconstruct fields...")
                else:
                    logger.warning(f"CSV reading failed or produced incorrect column count. "
                                 f"Expected {expected_cols} columns. "
                                 f"Attempting manual line-by-line parsing to reconstruct fields...")
                
                # Read CSV line by line and manually parse, respecting expected column count
                # This handles the case where columns_quote: none but fields contain commas
                import io
                rows_data = []
                header_row = None
                
                with open(csv_fpath, 'r', encoding='utf-8') as f:
                    if has_header:
                        header_line = f.readline().strip()
                        header_row = header_line.split(delimiter, expected_cols - 1)  # Split into expected_cols parts
                        if len(header_row) != expected_cols:
                            # If header itself is split, try to reconstruct
                            header_row = header_line.split(delimiter)[:expected_cols]
                        logger.debug(f"Parsed header: {header_row}")
                    
                    # Read data lines
                    # CRITICAL: When columns_quote: none, MySQL escapes commas with backslash (\,)
                    # Python's split() will split at ALL commas, including escaped ones
                    # We need to parse manually, respecting escaped commas
                    for line_num, line in enumerate(f, start=2 if has_header else 1):
                        line = line.rstrip('\n\r')
                        if not line:
                            continue
                        
                        # Parse line character by character, respecting escaped commas
                        # When columns_quote: none, MySQL escapes commas as \, (backslash-comma)
                        # We need to split on commas, but NOT on escaped commas
                        parts = []
                        current_part = []
                        i = 0
                        
                        while i < len(line):
                            char = line[i]
                            
                            # Check if this is a delimiter (comma) that is NOT escaped
                            if char == delimiter:
                                # Check if previous character is a backslash (escape)
                                if i == 0 or line[i-1] != '\\':
                                    # This is a real delimiter, not escaped
                                    parts.append(''.join(current_part))
                                    current_part = []
                                    
                                    # If we've collected enough parts, rest goes to last column
                                    if len(parts) >= expected_cols - 1:
                                        # Collect remaining characters as last column
                                        remaining = line[i+1:] if i+1 < len(line) else ''
                                        current_part = list(remaining)
                                        break
                                else:
                                    # This comma is escaped, include it in current part
                                    current_part.append(char)
                            else:
                                current_part.append(char)
                            
                            i += 1
                        
                        # Add the last part
                        if current_part:
                            parts.append(''.join(current_part))
                        
                        # Ensure we have exactly expected_cols parts
                        if len(parts) > expected_cols:
                            # Merge extra parts into the last column
                            parts[expected_cols - 1] = delimiter.join(parts[expected_cols - 1:])
                            parts = parts[:expected_cols]
                        
                        # Pad with empty strings if needed
                        while len(parts) < expected_cols:
                            parts.append('')
                        
                        # CRITICAL: MySQL escapes commas with backslash when columns_quote: none
                        # Unescape all columns (not just the last one, in case other columns have escaped chars)
                        for j in range(len(parts)):
                            parts[j] = parts[j].replace('\\,', ',')
                            parts[j] = parts[j].replace('\\"', '"')
                            parts[j] = parts[j].replace("\\'", "'")
                        
                        rows_data.append(parts)
                        
                        if line_num <= 3:
                            logger.debug(f"Line {line_num} parsed into {len(parts)} columns")
                            if expected_cols > 5:  # If geometry column exists
                                logger.debug(f"  Last column (geometry?) length: {len(parts[-1])}")
                
                # Create DataFrame from manually parsed data
                if has_header and header_row:
                    df = pd.DataFrame(rows_data, columns=header_row)
                else:
                    df = pd.DataFrame(rows_data, columns=[f'col_{i}' for i in range(expected_cols)])
                
                logger.info(f"Manually parsed CSV: {len(df)} rows, {len(df.columns)} columns")
                
                # Verify we now have the correct column count
                if len(df.columns) != expected_cols:
                    logger.error(f"Manual parsing failed: still have {len(df.columns)} columns, expected {expected_cols}")
                    raise ValueError(f"Failed to parse CSV with columns_quote: none. "
                                   f"Consider using columns_quote: double or single for data containing commas.")
            
            # Verify final DataFrame structure
            logger.debug(f"Final DataFrame shape: {df.shape}, columns: {list(df.columns)}")
            if len(df) > 0 and 'geometry' in df.columns:
                geom_sample = str(df['geometry'].iloc[0])
                logger.debug(f"Geometry column sample: length={len(geom_sample)}, starts_with={geom_sample[:50]}...")
            
            # Convert to Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_fpath)
            
            logger.debug(f"Converted CSV to Parquet: {csv_fpath} -> {parquet_fpath}")
        except ImportError:
            logger.error("PyArrow is required for Parquet conversion. Please install: pip install pyarrow")
            raise
        except Exception as e:
            logger.error(f"Error converting CSV to Parquet: {e}")
            raise
    
    def _convert_csv_to_json(self, csv_fpath, json_fpath, extractor_obj_conf):
        """Converts CSV file to JSON format."""
        try:
            import pandas as pd
            import csv
            
            object_settings = extractor_obj_conf.get('object_settings', {})
            delimiter = object_settings.get('columns_delimiter', ',')
            
            # Get quote settings to match how CSV was written
            columns_quote = object_settings.get('columns_quote', 'double')
            if columns_quote == 'double':
                quotechar = '"'
                quoting = csv.QUOTE_MINIMAL
            elif columns_quote == 'single':
                quotechar = "'"
                quoting = csv.QUOTE_MINIMAL
            else:
                quotechar = None
                quoting = csv.QUOTE_NONE
            
            # Escape character (MySQL uses backslash)
            escapechar = '\\' if quotechar else None
            
            # Read CSV with same settings as extraction
            # Try C engine first (faster), fall back to Python engine if needed
            read_csv_kwargs = {
                'delimiter': delimiter,
                'dtype': str,
                'keep_default_na': False,
                'na_values': [],
                'quoting': quoting
            }
            
            if quotechar:
                read_csv_kwargs['quotechar'] = quotechar
            if escapechar:
                read_csv_kwargs['escapechar'] = escapechar
            
            # Check if header is present
            has_header = object_settings.get('header', True)
            if has_header:
                read_csv_kwargs['header'] = 0
            else:
                read_csv_kwargs['header'] = None
            
            # Use Python engine for better handling of complex quoting/escaping
            # The C engine often fails with embedded newlines or complex escape sequences
            read_csv_kwargs['engine'] = 'python'
            
            # Handle bad lines: try to skip them silently, but log a summary
            # This prevents data loss warnings from cluttering the output
            try:
                read_csv_kwargs['on_bad_lines'] = 'skip'  # Skip bad lines silently
            except TypeError:
                # Older pandas versions use error_bad_lines
                try:
                    read_csv_kwargs['error_bad_lines'] = False
                except TypeError:
                    pass  # Parameter not supported in this pandas version
            
            logger.debug(f"Reading CSV with settings: delimiter={delimiter}, quotechar={quotechar}, escapechar={escapechar}, quoting={quoting}, engine=python")
            
            # Read CSV and capture any warnings about skipped lines
            import warnings
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                df = pd.read_csv(csv_fpath, **read_csv_kwargs)
                
                # Log summary of skipped lines if any
                skipped_lines = [str(warning.message) for warning in w if 'Skipping line' in str(warning.message)]
                if skipped_lines:
                    logger.warning(f"CSV parsing skipped {len(skipped_lines)} malformed lines. "
                                 f"These lines had incorrect field counts and were excluded from conversion. "
                                 f"First few problematic lines: {skipped_lines[:5]}")
            
            # Convert to JSON (records format - array of objects)
            df.to_json(json_fpath, orient='records', lines=False, date_format='iso')
            
            logger.debug(f"Converted CSV to JSON: {csv_fpath} -> {json_fpath}")
        except Exception as e:
            logger.error(f"Error converting CSV to JSON: {e}")
            raise
