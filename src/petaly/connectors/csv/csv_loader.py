# Copyright © 2024 Pavel Rabaev
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

import os
import sys
import time
from petaly.core.f_loader import FLoader


class CsvLoader(FLoader):

    def __init__(self, pipeline):
        self.file_format = 'csv'
        super().__init__(pipeline)

    def load_data(self):

        logger.info(f"[--- Load into {self.pipeline.target_connector_id} ---]")
        start_total_time = time.time()

        if self.pipeline.data_attributes.get("data_objects_spec_mode") == 'only':
            #object_list = super().get_data_object_list()
            object_list = self.pipeline.data_objects
        else:
            object_list = self.f_handler.get_all_dir_names(self.pipeline.output_pipeline_dpath)

        for object_name in object_list:
            output_object_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
            data_object_dict = super().get_data_object(object_name)

            dest_file_dir = self.pipeline.target_attr.get("destination_file_dir")
            if dest_file_dir is None:
                logger.warning(f"The pipeline->target_attribute->destination_file_dir in pipeline.yaml is not specified.")
                sys.exit()

            dest_object_name = object_name
            if data_object_dict.destination_object_name is not None:
                dest_object_name = data_object_dict.destination_object_name

            # get target file directory destination_file_dir/pipeline_name/object_name
            dest_file_dpath = os.path.join(dest_file_dir, self.pipeline.pipeline_name, dest_object_name)

            # get target file format
            logger.debug(f"Destination file format: {self.file_format}")

            dir_exists, files_in_dir = self.f_handler.check_dir(output_object_dir)

            if dir_exists is True and files_in_dir> 0:

                logger.debug(f"Output file dir: {output_object_dir}")
                logger.info(f"Load object: {object_name} started.. | destination directory: {dest_file_dpath}")
                start_time = time.time()

                # lists file with .csv extension, also files ends with .csv.gz. The logic can be improved.
                file_list = self.f_handler.get_file_names_with_extensions(output_object_dir, self.file_format,  self.file_format + '.gz')

                for file in file_list:
                    logger.debug(f"File: {file}")
                    file_source_fpath = os.path.join(output_object_dir, file)

                    # Rename file if destination_object_name is different from object_name
                    dest_file_name = file.replace(object_name, dest_object_name)

                    logger.debug(f"Load: Upload performed to destination path: {os.path.join(dest_file_dpath, dest_file_name)}")
                    self.f_handler.cp_file(file_source_fpath, dest_file_dpath, target_file_name=dest_file_name)

                end_time = time.time()
                logger.info(f"Load object: {object_name} completed | time: {round(end_time - start_time, 2)}s")


            else:
                logger.error(f"Load object {object_name} failed. Check the source and pipeline.yaml configuration. "
                             f"Output directory doesn't exist or is empty: {output_object_dir}")

        end_total_time = time.time()
        logger.info(f"Load completed, duration: {round(end_total_time - start_total_time, 2)}s")
