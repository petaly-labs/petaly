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

    def load_data(self, recreate_table=False):

        object_list = super().get_data_object_list()

        for object_name in object_list:
            output_data_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
            data_object_dict = super().get_data_object(object_name)

            # get target file directory
            target_file_dir = data_object_dict.target_file_dir
            if target_file_dir is None:
                logger.warning(f"object_attributes.target_file_dir in pipeline.yaml was not specified.")
                sys.exit()

            # get target file format
            target_file_format = data_object_dict.target_file_format
            if target_file_format is None:
                logger.warning(f"object_attributes.target_file_format in pipeline.yaml was not specified")
                target_file_format = 'csv'
                logger.info(f"File format was set to csv")

            logger.info(f"Move files from {output_data_dir} to {target_file_dir}")
            file_list = self.f_handler.get_all_dir_files(output_data_dir,
                                        target_file_format, file_names_only=True)

            for file in file_list:
                logger.info(f"File: {file}")
                file_source_fpath = os.path.join(output_data_dir, file)
                self.f_handler.cp_file(file_source_fpath, target_file_dir)
