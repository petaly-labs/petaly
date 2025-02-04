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
import sys

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
        """
        object_source_dir = extractor_obj_conf.get('object_source_dir')
        file_list = extractor_obj_conf.get('file_names')
        prepared_file_list = []

        if file_list is None:
            file_list = self.f_handler.get_file_names_with_extensions(object_source_dir, self.file_format)

        for file_name in file_list:
            file_source_fpath = os.path.join(object_source_dir, file_name)

            if self.f_handler.is_file(file_source_fpath):
                self.f_handler.cp_file(file_source_fpath, extractor_obj_conf.get('output_data_object_dir'))
            else:
                logger.error(f"The file: {file_source_fpath} wasn't found. Check the source and pipeline.yaml configuration.")
                sys.exit()

            prepared_file_list.append(file_source_fpath)

        logger.debug(f"The following file list is prepared for further processing:\n{prepared_file_list}")

        return prepared_file_list
