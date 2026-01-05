# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

from abc import ABC, abstractmethod

import os
import sys
import time
from pyarrow import csv, lib as pyarrow_lib

from petaly.core.composer import Composer
from petaly.utils.file_handler import FileHandler
from petaly.core.object_metadata import ObjectMetadata
from petaly.core.data_object import DataObject


class FExtractor(ABC):
    """Abstract base class for file extractors.
    
    This class provides the core functionality for extracting data from file-based sources.
    It handles file reading, metadata extraction, and data export to CSV files.
    
    Key responsibilities:
    - Extracts data from various file formats (CSV, etc.)
    - Analyzes file structure and metadata
    - Manages file output and metadata storage
    - Handles file compression and decompression
    
    Attributes:
        max_analysis_size_bytes: Maximum file size (in bytes) to analyze for metadata extraction.
                                 Files larger than this will only have the first portion analyzed.
                                 Default: 10MB (10 * 1024 * 1024 bytes)
    """
    
    # Default maximum analysis size: 10MB
    DEFAULT_MAX_ANALYSIS_SIZE = 10 * 1024 * 1024  # 10MB in bytes
    
    @staticmethod
    def get_file_format_from_delimiter(columns_delimiter):
        """
        Determines file format (csv/tsv) based on delimiter from csv_default_settings.
        
        Args:
            columns_delimiter: The delimiter character (e.g., ',', '\t', '|')
            
        Returns:
            str: File format extension ('csv' for comma, 'tsv' for tab, 'csv' as default)
        """
        if columns_delimiter == '\t' or columns_delimiter == '\\t':
            return 'tsv'
        else:
            # Default to csv for comma, pipe, semicolon, or any other delimiter
            return 'csv'

    def __init__(self, pipeline, max_analysis_size_bytes=None):
        """
        Initialize the FExtractor instance.
        
        Args:
            pipeline: Pipeline instance containing configuration
            max_analysis_size_bytes: Maximum file size (in bytes) to analyze for metadata.
                                     If None, reads from petaly.ini (csv_analysis_max_size_mb) or uses DEFAULT_MAX_ANALYSIS_SIZE (10MB).
                                     Files larger than this will only have the first portion analyzed.
        """
        self.pipeline = pipeline
        self.m_conf = self.pipeline.m_conf
        self.composer = Composer()
        self.f_handler = FileHandler()
        self.object_metadata = ObjectMetadata(pipeline)
        self.csv_default_settings = pipeline.data_attributes.get("csv_default_settings")
        
        # Set configurable max analysis size
        # Priority: 1) provided parameter, 2) petaly.ini config, 3) default (10MB)
        if max_analysis_size_bytes is not None:
            self.max_analysis_size_bytes = max_analysis_size_bytes
        else:
            # Read from petaly.ini configuration (value in MB, convert to bytes)
            try:
                csv_analysis_max_size_mb = float(pipeline.m_conf.global_settings.get('csv_analysis_max_size_mb', '10'))
                if csv_analysis_max_size_mb == 0:
                    # 0 means analyze entire file (set to a very large number)
                    self.max_analysis_size_bytes = float('inf')
                else:
                    self.max_analysis_size_bytes = int(csv_analysis_max_size_mb * 1024 * 1024)  # Convert MB to bytes
            except (ValueError, TypeError):
                # Fallback to default if config value is invalid
                logger.warning(f"Invalid csv_analysis_max_size_mb in config, using default: {self.DEFAULT_MAX_ANALYSIS_SIZE / 1024 / 1024} MB")
                self.max_analysis_size_bytes = self.DEFAULT_MAX_ANALYSIS_SIZE
        pass

    @abstractmethod
    def extract_to(self, extractor_obj_conf):
        pass

    def extract_data(self):
        """Extracts data from file sources and exports it to CSV files.
        
        This method orchestrates the entire extraction process:
        1. Saves metadata and export scripts
        2. Processes each object in the object list
        3. Cleans up pipeline directories
        4. Extracts data to files
        5. Extracts metadata from files if needed
        
        The method handles timing and logging of the extraction process.
        """

        logger.info(f"[--- Extract from {self.pipeline.source_connector_id} ---]")
        start_total_time = time.time()

        # 1. save metadata and export scripts
        object_list = self.pipeline.data_objects

        # 2. run loop for each object
        for object_name in object_list:
            logger.info(f"Extract object: {object_name} started...")
            start_time = time.time()

            extractor_obj_conf = self.get_extractor_obj_conf(object_name)

            # 3. cleanup pipeline directory before run
            self.f_handler.cleanup_dir(extractor_obj_conf.get('output_data_object_dir'))

            file_list = self.extract_to(extractor_obj_conf)

            connector_category = self.pipeline.m_conf.get_connector_category(self.pipeline.target_connector_id)
            if connector_category in ('database'):
                self.extract_metadata_from_file(file_list[0], object_name, self.file_format)

            end_time = time.time()
            logger.info(f"Extract object: {object_name} completed | time: {round(end_time - start_time, 2)}s")

        end_total_time = time.time()
        logger.info(f"Extract completed, duration: {round(end_total_time - start_total_time, 2)}s")

    def extract_metadata_from_file(self, first_file_fpath, object_name, file_format):
        """Extracts metadata from a file and saves it.
        
        The method:
        1. Checks if file is compressed and decompresses if needed
        2. Analyzes file structure
        3. Composes metadata
        4. Saves metadata to file
        """
        logger.debug(f"Check if the file {first_file_fpath} is compressed.")

        # check if file is gzipped. if gzipped try to unzip it
        is_gzipped, first_file_fpath = self.f_handler.check_gzip_modify_path(first_file_fpath)

        if is_gzipped:
            first_file_fpath = self.f_handler.gunzip_file(first_file_fpath, cleanup_file=True)

        # analyse file structure (using chunked PyArrow, max 10MB)
        # Format is determined by file_format parameter (parquet, json, csv) or file extension
        table_data = self.analyse_file_structure(first_file_fpath, object_name, file_format)

        meta_table = self.compose_metadata_file_from_table(table_data, object_name)
        self.save_metadata_into_file(meta_table)

    def get_extractor_obj_conf(self, object_name) -> dict:
        """Gets the configuration for extracting a specific object.
        
        The method composes a complete configuration containing:
        - Object name and paths
        - Output directories
        - Object source directory
        - Blob prefix
        - File names
        - Object settings
        - Output file paths
        """

        extractor_obj_conf = {'object_name': object_name}

        # 1. compose output data dir
        output_data_object_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
        extractor_obj_conf.update({'output_data_object_dir': output_data_object_dir})

        # 2. compose metadata directory
        output_metadata_object_dir = self.pipeline.output_object_metadata_dpath.format(object_name=object_name)
        extractor_obj_conf.update({'output_metadata_object_dir': output_metadata_object_dir})

        # 3. add csv_default_settings
        # extractor_obj_conf.update({'object_settings': table_metadata.get('object_settings')})
        data_object = self.get_data_object(object_name)

        # Validate and compose source directory for file connectors
        # Logic for source_dir and object_source_dir (complementary parameters):
        # 1. If source_dir is empty: object_source_dir must be an absolute path
        # 2. If source_dir is set and object_source_dir is set: final path = source_dir + / + object_source_dir
        # 3. If object_source_dir is not set but source_dir is: final path = source_dir + / + object_name
        if self.pipeline.source_attr.get('connector_type') in ('csv', 'parquet', 'json'):
            source_dir = self.pipeline.source_attr.get('source_dir')
            object_source_dir = data_object.object_source_dir
            
            # For file connectors, either source_dir in source_attributes or object_source_dir in object_spec must be set
            if not source_dir and not object_source_dir:
                logger.error(f"Incorrect source configuration in file: {self.pipeline.pipeline_fpath}")
                logger.error(f"For file connectors (CSV/Parquet/JSON) as source, you must specify either:"
                             f"\n  1. source_dir in source_attributes (base directory for all objects), OR"
                             f"\n  2. object_source_dir in object_spec (absolute path for this specific object)")
                logger.error(f"Current configuration:"
                             f"\n  source_attributes.source_dir: {source_dir}"
                             f"\n  object_spec.object_source_dir: {object_source_dir}")
                sys.exit()
            
            # Compose final object_source_dir based on source_dir and object_source_dir
            if source_dir:
                # Case 1: source_dir is set
                if object_source_dir:
                    # Both are set: combine them (source_dir + / + object_source_dir)
                    final_object_source_dir = os.path.join(source_dir, object_source_dir)
                    logger.debug(f"Combining source_dir and object_source_dir: {source_dir} + / + {object_source_dir} = {final_object_source_dir}")
                else:
                    # Only source_dir is set: combine source_dir + / + object_name
                    final_object_source_dir = os.path.join(source_dir, object_name)
                    logger.debug(f"Combining source_dir and object_name: {source_dir} + / + {object_name} = {final_object_source_dir}")
            else:
                # Case 2: source_dir is empty, object_source_dir must be an absolute path
                if not object_source_dir:
                    # This should not happen due to validation above, but double-check
                    logger.error(f"Incorrect source configuration: source_dir is empty and object_source_dir is not set")
                    sys.exit()
                
                # Validate that object_source_dir is an absolute path when source_dir is empty
                if not os.path.isabs(object_source_dir):
                    logger.error(f"Incorrect source configuration in file: {self.pipeline.pipeline_fpath}")
                    logger.error(f"When source_dir is empty, object_source_dir must be an absolute path.")
                    logger.error(f"Current configuration:"
                                 f"\n  source_attributes.source_dir: {source_dir} (empty)"
                                 f"\n  object_spec.object_source_dir: {object_source_dir} (not absolute)")
                    sys.exit()
                
                final_object_source_dir = object_source_dir
                logger.debug(f"Using object_source_dir as absolute path (source_dir is empty): {object_source_dir}")
            
            # Update data_object with the final composed path
            data_object.object_source_dir = final_object_source_dir

        if data_object.object_source_dir is None:
            if self.pipeline.source_attr.get('connector_type') in ('s3', 'gcs'):
                if self.pipeline.source_attr.get('bucket_pipeline_prefix') is None:
                    logger.error(f"Incorrect source or object specification in file: {self.pipeline.pipeline_fpath} "
                             f"\nEither bucket_pipeline_prefix in source_attributes or object_source_dir in object_spec, "
                                 f"or both, must be specified and cannot be empty. "
                                 f"\nThe object_source_dir is complementary to the bucket_pipeline_prefix. "
                                 f"E.g. for bucket path: bucket_name/bucket_pipeline_prefix/object_source_dir")
                    sys.exit()

        extractor_obj_conf.update({'object_source_dir': data_object.object_source_dir})

        blob_prefix = str(self.pipeline.source_attr.get('bucket_pipeline_prefix') or '').strip('/')
        blob_prefix = blob_prefix + '/' + str(data_object.object_source_dir or '').strip('/')
        extractor_obj_conf.update({'blob_prefix': blob_prefix.strip('/')})

        file_names = data_object.file_names
        if len(file_names) == 0 or file_names[0] is None:
            file_names = None

        extractor_obj_conf.update({'file_names': file_names})

        logger.debug(f"The object settings combined with default settings: {data_object.object_settings}")
        extractor_obj_conf.update({'object_settings': data_object.object_settings})

        # 6. create output object data dir and output_file_path
        output_data_object_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
        extractor_obj_conf.update({'output_data_object_dir': output_data_object_dir})

        self.f_handler.cleanup_dir(output_data_object_dir)
        self.f_handler.make_dirs(output_data_object_dir)

        # compose output object file path - use target connector format if it's a file connector
        file_extension = self._get_output_file_extension()
        output_object_fpath = os.path.join(output_data_object_dir, object_name + file_extension)
        extractor_obj_conf.update({'output_object_fpath': output_object_fpath})

        logger.debug(f"Config for data extract: {extractor_obj_conf}")
        return extractor_obj_conf

    def save_metadata_into_file(self, meta_table):
        """Saves table metadata to a file.
        
        The metadata is saved in JSON format to the object's metadata directory.
        """
        object_name = meta_table.get('source_object_name')
        source_object_fpath = self.pipeline.output_object_metadata_fpath.format(object_name=object_name)
        logger.debug(f"Format and save metadata for table {meta_table.get('source_object_name')} in {source_object_fpath}")
        self.f_handler.save_dict_to_file(source_object_fpath, meta_table, 'json')

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

    def extract_metadata_from_table(self, table_data):
        """Extracts metadata directly from a PyArrow Table (no Parquet conversion needed).
        
        The method extracts and formats column metadata including:
        - Column name and type
        - Physical type
        - Statistics (min, max, distinct_count, null_count)
        
        Note: Index columns (starting with '__index') are excluded as they are internal
        pandas/Parquet metadata and should not be included in database tables.
        """
        import pyarrow as pa

        schema = table_data.schema
        column_names = schema.names
        column_types = schema.types

        pq_columns_metadata_arr = []
        
        # Collect index columns to skip (for summary logging)
        index_columns = []

        for i, field in enumerate(schema):
            # Skip index columns (internal pandas/Parquet metadata)
            column_name = field.name
            if column_name.startswith('__index'):
                index_columns.append(column_name)
                continue
            
            column_dict = {}
            col = table_data.column(i)

            # column name
            column_dict.update({'column_name': column_name})

            # column type
            column_type = str(field.type)
            if column_type == 'null':
                column_type = 'string'
            column_dict.update({'data_type': column_type})

            # physical type (same as logical type for CSV)
            column_dict.update({'physical_type': str(field.type)})
            
            # Encoding and compression (not applicable for CSV, but kept for compatibility)
            column_dict.update({'encodings': 'plain'})
            column_dict.update({'compression': 'none'})

            # Calculate statistics
            try:
                # Null count
                null_count = col.null_count
                
                # Distinct count (compute unique values)
                unique_values = col.unique()
                distinct_count = len(unique_values)
                
                # Min/Max (only for numeric/temporal types)
                min_val = None
                max_val = None
                field_type_id = field.type.id
                
                # Check if type supports min/max operations
                numeric_types = [
                    pa.int8().id, pa.int16().id, pa.int32().id, pa.int64().id,
                    pa.uint8().id, pa.uint16().id, pa.uint32().id, pa.uint64().id,
                    pa.float32().id, pa.float64().id,
                    pa.date32().id, pa.date64().id,
                    pa.timestamp('ns').id, pa.timestamp('us').id, pa.timestamp('ms').id, pa.timestamp('s').id
                ]
                
                if field_type_id in numeric_types:
                    try:
                        min_val = col.min().as_py()
                        max_val = col.max().as_py()
                    except Exception as e:
                        logger.debug(f"Could not compute min/max for column {column_name}: {e}")
                
                column_dict.update({'is_stats_set': True})
                column_dict.update({'statistics_null_count': null_count})
                column_dict.update({'statistics_distinct_count': distinct_count})
                column_dict.update({'statistics_min': str(min_val) if min_val is not None else None})
                column_dict.update({'statistics_max': str(max_val) if max_val is not None else None})
                column_dict.update({'statistics_num_values': len(col) - null_count})
                
            except Exception as e:
                logger.debug(f"Could not compute statistics for column {column_name}: {e}")
                column_dict.update({'is_stats_set': False})
                column_dict.update({'statistics_null_count': None})
                column_dict.update({'statistics_distinct_count': None})
                column_dict.update({'statistics_min': None})
                column_dict.update({'statistics_max': None})
                column_dict.update({'statistics_num_values': None})

            pq_columns_metadata_arr.append(column_dict)
        
        # Log summary of skipped index columns
        if index_columns:
            logger.debug(f"Skipped {len(index_columns)} index column(s) in metadata extraction: {', '.join(index_columns[:5])}{'...' if len(index_columns) > 5 else ''}")

        return pq_columns_metadata_arr

    def compose_metadata_file_from_table(self, table_data, object_name):
        """Composes a metadata file for an object from PyArrow Table.
        
        The method creates a metadata table containing:
        - Source object information
        - Column definitions
        - Object settings
        """
        pq_columns_metadata_arr = self.extract_metadata_from_table(table_data)

        if len(pq_columns_metadata_arr) == 0:
            logger.warning("The process has failed to extract the metadata from the file, the column definition is empty.")
            sys.exit()
        meta_table = self.object_metadata.compose_object_meta_from_file(object_name, pq_columns_metadata_arr)
        return meta_table

    def describe_parquet_metadata(self, table_data):
        """Describes metadata from PyArrow Table to stdout (kept for backward compatibility).
        """
        # extract metadata from table
        pq_columns_metadata_arr = self.extract_metadata_from_table(table_data)
        header_str = ""
        col_separator = " | "
        output_text = ""

        # exit if array is empty
        if len(pq_columns_metadata_arr) == 0:
            logger.warning("pq_columns_metadata_arr is empty.")
            sys.exit()

        # create the header line
        for key in pq_columns_metadata_arr[0]:
            header_str += key + col_separator
        output_text += header_str + '\n'
        output_text += '-'*len(header_str)  + '\n'

        # compose output row by row
        for i in range(len(pq_columns_metadata_arr)):
            row_str = ""
            for key in pq_columns_metadata_arr[i]:
                row_str += str(pq_columns_metadata_arr[i].get(key)) + col_separator
            output_text += row_str + '\n'
        return output_text

    def analyse_file_structure(self, output_source_file, object_name, file_format_extension=None):
        """Analyzes the structure of a file (CSV/TSV/Parquet/JSON) to determine column formats using PyArrow.
        
        The method:
        1. Validates file existence
        2. Detects file format (parquet, json, or csv/tsv)
        3. Reads file using appropriate PyArrow reader (Parquet, JSON, or CSV)
        4. Returns PyArrow Table for direct metadata extraction
        
        Args:
            output_source_file: Path to the source file
            object_name: Name of the object being processed
            file_format_extension: File format ('parquet', 'json', 'csv', or None for auto-detect from extension)
        """
        is_file = self.f_handler.is_file(output_source_file)
        if is_file == False:
            logger.error(f"Output source file {output_source_file} doesn't exists")
            sys.exit()

        # Detect file format: use parameter if provided, otherwise detect from file extension
        if file_format_extension:
            detected_format = file_format_extension.lower()
        else:
            # Auto-detect from file extension
            file_ext = os.path.splitext(output_source_file)[1].lower()
            if file_ext in ('.parquet', '.parq'):
                detected_format = 'parquet'
            elif file_ext == '.json':
                detected_format = 'json'
            else:
                # Default to CSV/TSV for other extensions
                detected_format = 'csv'
        
        logger.debug(f"Detected file format: {detected_format} for file: {output_source_file}")

        # Handle Parquet files
        if detected_format == 'parquet':
            return self._read_parquet_file(output_source_file)
        
        # Handle JSON files
        if detected_format == 'json':
            return self._read_json_file(output_source_file)
        
        # Handle CSV/TSV files (existing logic)
        return self._read_csv_file(output_source_file)
    
    def _read_parquet_file(self, output_source_file):
        """Reads a Parquet file using PyArrow and returns a Table."""
        try:
            import pyarrow.parquet as pq
            
            # Check file size
            file_size = os.path.getsize(output_source_file)
            max_analysis_size = self.max_analysis_size_bytes
            
            if max_analysis_size == float('inf') or file_size <= max_analysis_size:
                # Read entire file
                max_size_mb = max_analysis_size / 1024 / 1024 if max_analysis_size != float('inf') else 'unlimited'
                logger.debug(f"Reading entire Parquet file (size: {file_size / 1024 / 1024:.2f} MB, limit: {max_size_mb} MB)")
                table = pq.read_table(output_source_file)
            else:
                # Read only first row group(s) for large files (limit to configured max size)
                max_size_mb = max_analysis_size / 1024 / 1024
                logger.info(f"Parquet file size ({file_size / 1024 / 1024:.2f} MB) exceeds {max_size_mb:.2f} MB limit. Reading first row group(s) only.")
                
                # Open Parquet file
                parquet_file = pq.ParquetFile(output_source_file)
                
                # Read first row group (or multiple if small)
                row_groups_to_read = []
                total_size = 0
                for i in range(parquet_file.num_row_groups):
                    row_group_metadata = parquet_file.metadata.row_group(i)
                    row_group_size = row_group_metadata.total_byte_size
                    if total_size + row_group_size > max_analysis_size and total_size > 0:
                        break
                    row_groups_to_read.append(i)
                    total_size += row_group_size
                
                if not row_groups_to_read:
                    # If first row group is too large, read it anyway
                    row_groups_to_read = [0]
                
                table = parquet_file.read_row_groups(row_groups_to_read)
                logger.debug(f"Read {len(row_groups_to_read)} row group(s): {len(table)} rows for analysis")
            
            logger.debug(f"Successfully read Parquet file. Rows analyzed: {len(table)}")
            return table
            
        except Exception as e:
            logger.error(f"Error reading Parquet file: {e}")
            logger.info(f"Check that the {output_source_file} file is a valid Parquet file.")
            sys.exit()
    
    def _read_json_file(self, output_source_file):
        """Reads a JSON file using PyArrow and returns a Table.
        
        Supports multiple JSON formats:
        - Array format: [{"col1": "val1"}, {"col1": "val2"}]
        - Newline-delimited JSON (NDJSON): {"col1": "val1"}\n{"col1": "val2"}
        - Single object: {"col1": "val1", "col2": "val2"}
        """
        try:
            import pyarrow.json as json_reader
            import pandas as pd
            
            # Check file size
            file_size = os.path.getsize(output_source_file)
            max_analysis_size = self.max_analysis_size_bytes
            
            # Try PyArrow first (fastest, but requires consistent structure)
            try:
                if max_analysis_size == float('inf') or file_size <= max_analysis_size:
                    max_size_mb = max_analysis_size / 1024 / 1024 if max_analysis_size != float('inf') else 'unlimited'
                    logger.debug(f"Reading entire JSON file with PyArrow (size: {file_size / 1024 / 1024:.2f} MB, limit: {max_size_mb} MB)")
                    table = json_reader.read_json(output_source_file)
                else:
                    max_size_mb = max_analysis_size / 1024 / 1024
                    logger.info(f"JSON file size ({file_size / 1024 / 1024:.2f} MB) exceeds {max_size_mb:.2f} MB limit. Reading entire file for analysis.")
                    table = json_reader.read_json(output_source_file)
                
                logger.debug(f"Successfully read JSON file with PyArrow. Rows analyzed: {len(table)}")
                return table
                
            except Exception as pyarrow_error:
                # PyArrow failed (likely due to inconsistent structure)
                # Fallback to pandas which is more tolerant of structure variations
                logger.debug(f"PyArrow JSON reader failed: {pyarrow_error}. Trying pandas fallback...")
                
                # Read with pandas - it's more tolerant of JSON structure variations
                try:
                    # Try reading as array format first
                    df = pd.read_json(output_source_file, orient='records', lines=False)
                except Exception:
                    # If that fails, try newline-delimited JSON format
                    try:
                        df = pd.read_json(output_source_file, orient='records', lines=True)
                    except Exception:
                        # Last resort: try reading as single JSON object and wrap in list
                        import json
                        with open(output_source_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            if isinstance(data, dict):
                                # Single object - wrap in list
                                df = pd.DataFrame([data])
                            elif isinstance(data, list):
                                # Array of objects
                                df = pd.DataFrame(data)
                            else:
                                raise ValueError(f"Unsupported JSON structure: {type(data)}")
                
                # Convert pandas DataFrame to PyArrow Table
                import pyarrow as pa
                table = pa.Table.from_pandas(df)
                
                logger.debug(f"Successfully read JSON file with pandas fallback. Rows analyzed: {len(table)}")
                return table
            
        except Exception as e:
            logger.error(f"Error reading JSON file: {e}")
            logger.info(f"Check that the {output_source_file} file is a valid JSON file.")
            logger.info(f"Supported formats: array format [{{...}}, {{...}}] or newline-delimited JSON (one object per line).")
            sys.exit()
    
    def _read_csv_file(self, output_source_file):
        """Reads a CSV/TSV file using PyArrow and returns a Table."""
        # Check file size
        file_size = os.path.getsize(output_source_file)
        max_analysis_size = self.max_analysis_size_bytes
        
        parse_options = csv.ParseOptions(delimiter=self.csv_default_settings.get("columns_delimiter"))
        read_options = csv.ReadOptions()
        
        # Enable automatic type detection for CSV/TSV files (if type_autodetection is enabled)
        # PyArrow's CSV reader can automatically infer types (int, float, date, etc.) from the data
        # By default, PyArrow reads all columns as strings unless we enable type inference
        # Note: PyArrow will attempt to parse numeric values, dates, etc. automatically
        type_autodetection = self.csv_default_settings.get("type_autodetection", True)
        if type_autodetection:
            # Type detection enabled - PyArrow will infer types automatically
            convert_options = csv.ConvertOptions(
                strings_can_be_null=False,  # Allow null strings
                null_values=['', r'\N', 'NULL', 'null', 'None', 'N/A', 'n/a']  # Common null representations
            )
            logger.debug("Type autodetection is enabled - PyArrow will automatically infer column types")
        else:
            # Type detection disabled - all columns will be read as strings
            # To force all columns as strings, we need to read the file first to get column names,
            # then create a schema with all string types
            # However, for simplicity, we'll use a two-pass approach: read once to get schema, then read again with string types
            # For now, we'll read without ConvertOptions and let PyArrow default to strings, but handle nulls manually
            # Actually, PyArrow by default reads as strings when no type inference is specified
            # We still need ConvertOptions for null handling, but we'll read the header first to get column names
            try:
                # Read just the header to get column names
                read_options_header = csv.ReadOptions(skip_rows=0, column_names=None)
                parse_options_header = csv.ParseOptions(delimiter=self.csv_default_settings.get("columns_delimiter"))
                # Read first row to get column names
                with open(output_source_file, 'rb') as f:
                    first_batch = csv.open_csv(f, parse_options=parse_options_header, read_options=read_options_header).read_next_batch()
                    if first_batch:
                        column_names = first_batch.schema.names
                        # Create schema with all string types
                        import pyarrow as pa
                        string_schema = pa.schema([(name, pa.string()) for name in column_names])
                        convert_options = csv.ConvertOptions(
                            column_types=string_schema,
                            strings_can_be_null=False,
                            null_values=['', r'\N', 'NULL', 'null', 'None', 'N/A', 'n/a']
                        )
                    else:
                        # Fallback: use empty dict (PyArrow will read as strings by default)
                        convert_options = csv.ConvertOptions(
                            strings_can_be_null=False,
                            null_values=['', r'\N', 'NULL', 'null', 'None', 'N/A', 'n/a']
                        )
            except Exception as e:
                logger.debug(f"Could not pre-read schema for string types, using default: {e}")
                # Fallback: use default (PyArrow will read as strings when no type inference)
                convert_options = csv.ConvertOptions(
                    strings_can_be_null=False,
                    null_values=['', r'\N', 'NULL', 'null', 'None', 'N/A', 'n/a']
                )
            logger.debug("Type autodetection is disabled - all columns will be read as strings")

        try:
            # Handle infinity case (csv_analysis_max_size_mb=0 means analyze entire file)
            if max_analysis_size == float('inf'):
                # Analyze entire file (csv_analysis_max_size_mb=0 in config)
                logger.debug(f"Reading entire file (csv_analysis_max_size_mb=0, no size limit, file size: {file_size / 1024 / 1024:.2f} MB)")
                file_data = csv.read_csv(output_source_file, parse_options=parse_options, convert_options=convert_options)
            elif file_size > max_analysis_size:
                max_size_mb = max_analysis_size / 1024 / 1024
                logger.debug(f"Start reading the csv file: {output_source_file} (size: {file_size / 1024 / 1024:.2f} MB, max analysis: {max_size_mb:.2f} MB)")
                # Use chunked reading for large files (limit to configured max size)
                logger.info(f"File size ({file_size / 1024 / 1024:.2f} MB) exceeds {max_size_mb:.2f} MB limit. Analyzing first {max_size_mb:.2f} MB only.")
                
                # Set block size to configured max size to limit reading
                # Note: PyArrow respects line boundaries, so if a line spans the boundary,
                # the complete line will be included. This means the actual analyzed size
                # may be slightly larger than the configured limit (typically a few KB to MB).
                # This ensures valid CSV structure and prevents partial/corrupted records.
                read_options.block_size = max_analysis_size
                
                # Open CSV stream with limited block size
                csv_stream = csv.open_csv(output_source_file, parse_options=parse_options, read_options=read_options, convert_options=convert_options)
                
                # Read first batch (may be slightly larger than max_size_mb due to complete line inclusion)
                try:
                    first_batch = csv_stream.read_next_batch()
                    if first_batch is None:
                        logger.error(f"Could not read any data from CSV file")
                        sys.exit()
                    
                    # Convert RecordBatch to Table (this is our sample, up to configured max size)
                    # Note: The actual size may exceed max_analysis_size slightly to include complete lines
                    # PyArrow's read_next_batch() returns a RecordBatch, which needs to be converted to Table
                    import pyarrow as pa
                    file_data = pa.Table.from_batches([first_batch])
                    logger.debug(f"Read sample batch: {len(file_data)} rows for analysis")
                except StopIteration:
                    logger.error(f"Could not read data from CSV file")
                    sys.exit()
                finally:
                    # Close the stream
                    csv_stream.close()
            else:
                # Read entire file for small files (file size <= max_analysis_size)
                max_size_mb = max_analysis_size / 1024 / 1024
                logger.debug(f"Reading entire file (size: {file_size / 1024 / 1024:.2f} MB, within {max_size_mb:.2f} MB limit)")
                file_data = csv.read_csv(output_source_file, parse_options=parse_options, convert_options=convert_options)
                
        except pyarrow_lib.ArrowInvalid as err:
            logger.error(f"Error {err}")
            logger.info(f"Check that the {output_source_file} file matches the parsing options: {self.csv_default_settings}")
            sys.exit()
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            logger.info(f"Check that the {output_source_file} file matches the parsing options: {self.csv_default_settings}")
            sys.exit()

        logger.debug(f"Successfully read CSV file. Rows analyzed: {len(file_data)}")
        return file_data

    def get_data_object(self, object_name):
        """Gets a DataObject instance for the specified object.
        
        Creates and returns a DataObject instance containing the object's
        configuration and settings.
        """
        return DataObject(self.pipeline, object_name)

