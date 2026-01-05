# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
import sys
import pandas as pd

logger = logging.getLogger(__name__)

import os
from petaly.core.f_extractor import FExtractor

class CsvExtractor(FExtractor):

    def __init__(self, pipeline):
        self.file_format = 'csv'
        super().__init__(pipeline)

    def extract_data(self):
        super().extract_data()

    def extract_to(self, extractor_obj_conf):
        """
        Extracts delimited files (CSV/TSV/TXT) and processes them to remove excluded columns.
        Format is determined by columns_delimiter in csv_default_settings, not file extension.
        """
        object_source_dir = extractor_obj_conf.get('object_source_dir')
        file_list = extractor_obj_conf.get('file_names')
        prepared_file_list = []
        
        # Get excluded columns from data object
        data_object = self.get_data_object(extractor_obj_conf.get('object_name'))
        exclude_columns = data_object.exclude_columns if data_object.exclude_columns else []
        object_settings = extractor_obj_conf.get('object_settings')

        if file_list is None:
            # Collect all files regardless of extension - format is determined by columns_delimiter, not extension
            # CSV/TSV/TXT files can have any extension (.csv, .tsv, .txt, or any other)
            all_files = self.f_handler.get_all_files_from_dir(os.path.join(object_source_dir, '*'))
            file_list = [os.path.basename(f) for f in all_files if os.path.isfile(f)]

        for file_name in file_list:
            file_source_fpath = os.path.join(object_source_dir, file_name)

            if self.f_handler.is_file(file_source_fpath):
                # Process delimited file (CSV/TSV/TXT) to remove excluded columns
                output_fpath = os.path.join(extractor_obj_conf.get('output_data_object_dir'), file_name)
                self.process_csv_file(file_source_fpath, output_fpath, exclude_columns, object_settings)
                prepared_file_list.append(output_fpath)
            else:
                logger.error(f"The file: {file_source_fpath} wasn't found. Check the source and pipeline.yaml configuration.")
                sys.exit()

        logger.debug(f"The following file list is prepared for further processing:\n{prepared_file_list}")

        return prepared_file_list

    def process_csv_file(self, input_fpath, output_fpath, exclude_columns, object_settings):
        """
        Processes a delimited file (CSV/TSV/TXT) to remove excluded columns.
        Format is determined by columns_delimiter in object_settings, not file extension.
        
        NULL handling:
        - Preserves \\N as literal string for databases that recognize it (PostgreSQL, MySQL, Redshift)
        - For BigQuery, empty strings are treated as NULL, but \\N is preserved for consistency
        - Each database loader handles NULL recognition according to its own conventions:
          * PostgreSQL: Recognizes \\N as NULL when NULL '\\N' is specified in COPY
          * MySQL: Recognizes \\N as NULL by default in LOAD DATA INFILE
          * Redshift: Recognizes \\N as NULL by default in COPY (with EMPTYASNULL)
          * BigQuery: Treats empty strings as NULL, but \\N is preserved as string literal
        """
        try:
            # Read delimited file with proper delimiter (CSV/TSV/TXT - format determined by delimiter, not extension)
            delimiter = object_settings.get('columns_delimiter', ',')
            # Read as strings to preserve \N as literal string (don't convert to NaN)
            df = pd.read_csv(input_fpath, delimiter=delimiter, dtype=str, keep_default_na=False, na_values=[])
            
            # Remove excluded columns
            if exclude_columns:
                logger.debug(f"Removing excluded columns: {exclude_columns}")
                df = df.drop(columns=exclude_columns, errors='ignore')
            
            # Write processed file with same delimiter
            # Preserve \N as-is - each database loader will handle NULL recognition appropriately
            df.to_csv(output_fpath, sep=delimiter, index=False, na_rep=r'\N')
            logger.debug(f"Processed delimited file (delimiter='{delimiter}'): {input_fpath} -> {output_fpath}")
            
        except Exception as e:
            logger.error(f"Error processing delimited file {input_fpath}: {e}")
            # Fallback to simple copy if processing fails
            # Extract directory and filename from output_fpath
            import os
            output_dir = os.path.dirname(output_fpath)
            output_filename = os.path.basename(output_fpath)
            self.f_handler.cp_file(input_fpath, output_dir, target_file_name=output_filename)
