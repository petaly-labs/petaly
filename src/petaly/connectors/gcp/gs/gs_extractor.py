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
logger = logging.getLogger(__name__)

from petaly.connectors.gcp.gs.gs_connector import GSConnector
from petaly.core.f_extractor import FExtractor
from petaly.utils.file_handler import FileHandler


class GSExtractor(FExtractor):
    def __init__(self, pipeline):
        self.gs_connector = GSConnector()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.source_attr.get('gcp_bucket_name')
        self.cloud_project_id = self.pipeline.source_attr.get('gcp_project_id')
        self.cloud_region = self.pipeline.source_attr.get('gcp_region')
        self.file_format = 'csv'
        self.f_handler = FileHandler()

    def extract_data(self):
        super().extract_data()

    def extract_to(self, extractor_obj_conf):
        """ Download export from bucket into local folder
        """
        output_data_object_dir = extractor_obj_conf.get('output_data_object_dir')
        self.f_handler.cleanup_dir(output_data_object_dir)

        file_list = self.gs_connector.download_files_from_bucket(
                                                    bucket_name=self.cloud_bucket_name,
                                                    blob_prefix=extractor_obj_conf.get('blob_prefix'),
                                                    file_names=extractor_obj_conf.get('file_names'),
                                                    destination_dpath=output_data_object_dir)

        logger.debug(f"The following file list were downloaded from bucket:\n{file_list}")

        return file_list
