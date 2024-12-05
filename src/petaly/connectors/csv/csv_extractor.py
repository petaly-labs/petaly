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

import os
import sys
import logging
logger = logging.getLogger(__name__)
from petaly.core.f_extractor import FExtractor
from pprint import pprint

class CsvExtractor(FExtractor):

    def __init__(self, pipeline):
        self.file_format = 'csv'
        super().__init__(pipeline)

    def extract_data(self):
        """
        """
        object_list = super().get_data_object_list()

        # cleanup pipeline directory before run
        self.f_handler.cleanup_dir(self.pipeline.output_pipeline_dpath)

        for object_name in object_list:

            data_object_dict = super().get_data_object(object_name)
            #print(data_object_dict)

            # check files_source_dir
            if data_object_dict.files_source_dir is None:
                logger.warning(f"The data_objects_spec->{object_name}->files_source_dir in pipeline.yaml is not specified.")
                sys.exit()

            file_list = data_object_dict.file_names

            if len(file_list) == 0 or file_list[0] is None:
                file_list = self.f_handler.get_all_dir_files(data_object_dict.files_source_dir, self.file_format, file_names_only=True)

            for file in file_list:
                file_source_fpath = os.path.join(data_object_dict.files_source_dir, file)
                destination_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
                self.f_handler.cp_file(file_source_fpath, destination_dir)

            first_file_fpath = os.path.join(destination_dir, file_list[0])

            # analyse file structure
            parquet_fpath = self.analyse_file_structure(first_file_fpath, data_object_dict, self.file_format)

            meta_table = self.compose_metadata_file(parquet_fpath, object_name )
            self.save_metadata_into_file(meta_table)

            # self.describe_parquet_metadata(parquet_fpath)


