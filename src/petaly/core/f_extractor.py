# Copyright © 2024-2025 Pavel Rabaev
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        Determines file format (csv/tsv) based on delimiter from object_default_settings.
        
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
        self.composer = Composer()
        self.f_handler = FileHandler()
        self.object_metadata = ObjectMetadata(pipeline)
        self.object_default_settings = pipeline.data_attributes.get("object_default_settings")
        
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
        # Note: Format is determined by delimiter in object_default_settings, not file extension
        # Files can have any extension (.csv, .tsv, .txt, etc.) and will be parsed correctly
        table_data = self.analyse_file_structure(first_file_fpath, object_name)

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

        # 3. add object_default_settings
        # extractor_obj_conf.update({'object_settings': table_metadata.get('object_settings')})
        data_object = self.get_data_object(object_name)

        if data_object.object_source_dir is None:
            if self.pipeline.source_attr.get('connector_type') in ('csv'):
                logger.error(f"Incorrect object specification in file: {self.pipeline.pipeline_fpath} "
                               f"\ndata_objects_spec: "
                               f"\n- object_spec:"
                               f"\n    object_name: {object_name}"
                               f"\n    object_source_dir: IS EMPTY")
                sys.exit()
            elif self.pipeline.source_attr.get('connector_type') in ('s3', 'gcs'):
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

        # compose output object file path (always .csv for file extracts)
        output_object_fpath = os.path.join(output_data_object_dir, object_name + '.csv')
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

    def extract_metadata_from_table(self, table_data):
        """Extracts metadata directly from a PyArrow Table (no Parquet conversion needed).
        
        The method extracts and formats column metadata including:
        - Column name and type
        - Physical type
        - Statistics (min, max, distinct_count, null_count)
        """
        import pyarrow as pa

        schema = table_data.schema
        column_names = schema.names
        column_types = schema.types

        pq_columns_metadata_arr = []

        for i, field in enumerate(schema):
            column_dict = {}
            col = table_data.column(i)

            # column name
            column_name = field.name
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
        """Analyzes the structure of a delimited file (CSV/TSV) to determine column formats using chunked PyArrow.
        
        The method:
        1. Validates file existence
        2. Reads delimited file in chunks (max 10MB for analysis) using delimiter from object_default_settings
        3. Returns PyArrow Table for direct metadata extraction (no Parquet conversion)
        
        Note: File format is determined by delimiter in object_default_settings, not by file extension.
              Files can have any extension (.csv, .tsv, .txt, etc.) - the delimiter determines how to parse them.
        """
        is_file = self.f_handler.is_file(output_source_file)
        if is_file == False:
            logger.error(f"Output source file {output_source_file} doesn't exists")
            sys.exit()

        # File extension check removed - format is determined by delimiter in object_default_settings, not extension
        # Files can have any extension (.csv, .tsv, .txt, etc.) and will be parsed based on columns_delimiter

        # Check file size
        file_size = os.path.getsize(output_source_file)
        max_analysis_size = self.max_analysis_size_bytes
        
        parse_options = csv.ParseOptions(delimiter=self.object_default_settings.get("columns_delimiter"))
        read_options = csv.ReadOptions()

        try:
            # Handle infinity case (csv_analysis_max_size_mb=0 means analyze entire file)
            if max_analysis_size == float('inf'):
                # Analyze entire file (csv_analysis_max_size_mb=0 in config)
                logger.debug(f"Reading entire file (csv_analysis_max_size_mb=0, no size limit, file size: {file_size / 1024 / 1024:.2f} MB)")
                file_data = csv.read_csv(output_source_file, parse_options=parse_options)
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
                csv_stream = csv.open_csv(output_source_file, parse_options=parse_options, read_options=read_options)
                
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
                file_data = csv.read_csv(output_source_file, parse_options=parse_options)
                
        except pyarrow_lib.ArrowInvalid as err:
            logger.error(f"Error {err}")
            logger.info(f"Check that the {output_source_file} file matches the parsing options: {self.object_default_settings}")
            sys.exit()
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            logger.info(f"Check that the {output_source_file} file matches the parsing options: {self.object_default_settings}")
            sys.exit()

        logger.debug(f"Successfully read CSV file. Rows analyzed: {len(file_data)}")
        return file_data

    def get_data_object(self, object_name):
        """Gets a DataObject instance for the specified object.
        
        Creates and returns a DataObject instance containing the object's
        configuration and settings.
        """
        return DataObject(self.pipeline, object_name)

