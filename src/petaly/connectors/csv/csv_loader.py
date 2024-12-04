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
from petaly.core.f_loader import FLoader


class CsvLoader(FLoader):

    def __init__(self, pipeline):
        super().__init__(pipeline)

    def load_data(self):

        if self.pipeline.data_attributes.get("data_objects_spec_mode") == 'only':
            object_list = super().get_data_object_list()
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
            target_file_format = 'csv'
            logger.info(f"Destination file format: csv")

            logger.info(f"Output file dir: {output_object_dir}")
            file_list = self.f_handler.get_all_dir_files(output_object_dir,
                                        target_file_format, file_names_only=True)

            logger.info(f"Destination file dir: {dest_file_dpath}")
            for file in file_list:
                logger.info(f"File: {file}")
                file_source_fpath = os.path.join(output_object_dir, file)

                # Rename file if destination_object_name is different from object_name
                dest_file_name = file.replace(object_name, dest_object_name)

                logger.info(f"Destination file path: {os.path.join(dest_file_dpath, dest_file_name)}")
                self.f_handler.cp_file(file_source_fpath, dest_file_dpath, target_file_name=dest_file_name)
