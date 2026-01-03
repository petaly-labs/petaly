# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
import sys
import pandas as pd

logger = logging.getLogger(__name__)

import os
from petaly.core.f_extractor import FExtractor

class JsonExtractor(FExtractor):

    def __init__(self, pipeline):
        self.file_format = 'json'
        super().__init__(pipeline)

    def extract_data(self):
        super().extract_data()

    def extract_to(self, extractor_obj_conf):
        """
        Extracts JSON files and processes them to remove excluded columns.
        """
        object_source_dir = extractor_obj_conf.get('object_source_dir')
        file_list = extractor_obj_conf.get('file_names')
        prepared_file_list = []
        
        # Get excluded columns from data object
        data_object = self.get_data_object(extractor_obj_conf.get('object_name'))
        exclude_columns = data_object.exclude_columns if data_object.exclude_columns else []
        object_settings = extractor_obj_conf.get('object_settings')

        if file_list is None:
            # Collect all JSON files
            all_files = self.f_handler.get_all_files_from_dir(os.path.join(object_source_dir, '*'))
            file_list = [os.path.basename(f) for f in all_files if os.path.isfile(f) and f.endswith('.json')]

        for file_name in file_list:
            file_source_fpath = os.path.join(object_source_dir, file_name)

            if self.f_handler.is_file(file_source_fpath):
                # Process JSON file to remove excluded columns
                output_fpath = os.path.join(extractor_obj_conf.get('output_data_object_dir'), file_name)
                self.process_json_file(file_source_fpath, output_fpath, exclude_columns, object_settings)
                prepared_file_list.append(output_fpath)
            else:
                logger.error(f"The file: {file_source_fpath} wasn't found. Check the source and pipeline.yaml configuration.")
                sys.exit()

        logger.debug(f"The following file list is prepared for further processing:\n{prepared_file_list}")

        return prepared_file_list

    def process_json_file(self, input_fpath, output_fpath, exclude_columns, object_settings):
        """
        Processes a JSON file to remove excluded columns.
        """
        try:
            import json
            
            # Read JSON file
            with open(input_fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            if isinstance(data, list) and data and isinstance(data[0], dict):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                try:
                    df = pd.DataFrame(data)
                except:
                    logger.warning(f"Cannot process JSON structure in {input_fpath}, falling back to copy")
                    import os
                    output_dir = os.path.dirname(output_fpath)
                    output_filename = os.path.basename(output_fpath)
                    self.f_handler.cp_file(input_fpath, output_dir, target_file_name=output_filename)
                    return
            
            # Remove excluded columns
            if exclude_columns:
                logger.debug(f"Removing excluded columns: {exclude_columns}")
                df = df.drop(columns=exclude_columns, errors='ignore')
            
            # Write processed JSON file
            df.to_json(output_fpath, orient='records', indent=2)
            logger.debug(f"Processed JSON file: {input_fpath} -> {output_fpath}")
            
        except Exception as e:
            logger.error(f"Error processing JSON file {input_fpath}: {e}")
            # Fallback to simple copy if processing fails
            import os
            output_dir = os.path.dirname(output_fpath)
            output_filename = os.path.basename(output_fpath)
            self.f_handler.cp_file(input_fpath, output_dir, target_file_name=output_filename)
