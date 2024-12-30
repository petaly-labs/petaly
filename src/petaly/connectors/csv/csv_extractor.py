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
        # cleanup pipeline directory before run
        output_object_dpath = extractor_obj_conf.get('output_object_dpath')
        self.f_handler.cleanup_dir(output_object_dpath)

        object_name = extractor_obj_conf.get('object_name')
        object_source_dir = extractor_obj_conf.get('object_source_dir')
        file_names = extractor_obj_conf.get('file_names')

        if len(file_names) == 0 or file_names[0] is None:
            file_names = self.f_handler.get_file_names_with_extensions(object_source_dir, self.file_format)

        for file_name in file_names:
            file_source_fpath = os.path.join(object_source_dir, file_name)
            if self.f_handler.is_file(file_source_fpath):
                self.f_handler.cp_file(file_source_fpath, output_object_dpath)
            else:
                logger.error(f"The file: {file_source_fpath} wasn't found. Check the source and pipeline.yaml configuration.")
                return False

        first_file_fpath = os.path.join(output_object_dpath, file_names[0])

        logger.info(f"Check if the file {first_file_fpath} is compressed.")
        # check if file is gzip. if gzip unzip it
        if self.f_handler.is_file_gzip(first_file_fpath):
            if not self.f_handler.check_file_extension(first_file_fpath, '.gz'):
                first_file_fpath = self.f_handler.add_extension(first_file_fpath,'.gz')
            first_file_fpath = self.f_handler.gunzip_file(first_file_fpath, cleanup_file=True)

        # analyse file structure
        parquet_fpath = self.analyse_file_structure(first_file_fpath, object_name, file_format_extension= '.' + self.file_format)

        meta_table = self.compose_metadata_file(parquet_fpath, object_name)
        self.save_metadata_into_file(meta_table)

        # self.describe_parquet_metadata(parquet_fpath)
