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
import logging

from petaly.connectors.gcp.gs.gs_connector import GSConnector
from petaly.core.f_extractor import FExtractor
from petaly.utils.file_handler import FileHandler
from petaly.utils.utils import FormatDict

class GSExtractor(FExtractor):
    def __init__(self, pipeline):
        self.gs_connector = GSConnector()
        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.source_attr.get('gcp_bucket_name')
        self.cloud_project_id = self.pipeline.source_attr.get('gcp_project_id')
        self.cloud_region = self.pipeline.source_attr.get('gcp_region')

        self.f_handler = FileHandler()

    def extract_data(self):
        super().extract_data()

    def extract_to(self, extractor_obj_conf):

        output_object_dpath = extractor_obj_conf.get('output_object_dpath')
        self.f_handler.cleanup_dir(output_object_dpath)
        #object_name = extractor_obj_conf.get('object_name')
        #data_object = super().get_data_object(object_name)

        object_source_dir = extractor_obj_conf.get('object_source_dir')
        blob_prefix = object_source_dir.strip('/')
        file_names = extractor_obj_conf.get('file_names')
        output_file_dir = os.path.dirname(extractor_obj_conf.get('output_object_fpath'))

        file_names_full_path = []
        for file_name in file_names:
            file_names_full_path.append(f"{blob_prefix}/{file_name}")

        # download export from bucket into local folder
        downloaded_file_list = self.gs_connector.download_files_from_bucket(
                                                    self.cloud_bucket_name,
                                                    blob_prefix,
                                                    specific_file_list=file_names_full_path,
                                                    destination_directory=output_file_dir)

        logging.debug(f"Following file list were downloaded from bucket:\n{downloaded_file_list}")


