# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)


import logging
logger = logging.getLogger(__name__)

import os
import sys
from petaly.core.f_loader import FLoader


class ParquetLoader(FLoader):

    def __init__(self, pipeline):
        self.file_format = 'parquet'
        super().__init__(pipeline)

    def load_data(self):
        super().load_data()

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')

        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')
        data_object = super().get_data_object(object_name)

        dest_object_name = object_name
        if data_object.destination_object_name is not None:
            dest_object_name = data_object.destination_object_name

        # get target file directory
        dest_file_dpath = self.get_target_object_dir(object_name, data_object)

        # get target file format
        logger.debug(f"Destination file format: {self.file_format}")

        dir_exists, files_in_dir = self.f_handler.check_dir(output_data_object_dir)

        if dir_exists is True and files_in_dir> 0:

            logger.debug(f"Output file dir: {output_data_object_dir}")
            logger.info(f"Load object: {object_name} destination directory: {dest_file_dpath}")

            file_list = loader_obj_conf.get('file_list')

            # Filter out CSV files - only copy parquet files to target directory
            # CSV files remain in workspace for reference/debugging
            parquet_files = [f for f in file_list if f.endswith('.parquet') or f.endswith('.parq')]
            
            if not parquet_files:
                logger.warning(f"No parquet files found in file_list. CSV files are kept in workspace and not copied to target directory.")
            
            for file_path in parquet_files:
                file_name = os.path.basename(file_path)
                # Rename file if destination_object_name is different from object_name

                dest_file_name = file_name.replace(object_name, dest_object_name)

                self.f_handler.cp_file(file_path, dest_file_dpath, target_file_name=dest_file_name)
                logger.debug(f"Load: File {file_path} from output directory is copied to: {os.path.join(dest_file_dpath, dest_file_name)}")

        else:
            logger.error(f"Load object {object_name} failed. Check the source and pipeline.yaml configuration. "
                         f"Output directory doesn't exist or is empty: {output_data_object_dir}")
