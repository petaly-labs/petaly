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

import logging

from petaly.connectors.aws.s3.s3_connector import S3Connector
from petaly.core.f_extractor import FExtractor
from petaly.utils.file_handler import FileHandler


class S3Extractor(FExtractor):
    def __init__(self, pipeline):
        self.s3_connector = S3Connector(pipeline.source_attr, aws_session=None)
        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.source_attr.get('aws_bucket_name')
        self.file_format = 'csv'
        self.f_handler = FileHandler()

    def extract_data(self):
        super().extract_data()

    def extract_to(self, extractor_obj_conf):
        """ Download export from bucket into local folder
        """

        file_list = self.s3_connector.download_files_from_bucket(
                                                    bucket_name=self.cloud_bucket_name,
                                                    blob_prefix=extractor_obj_conf.get('blob_prefix'),
                                                    file_names=extractor_obj_conf.get('file_names'),
                                                    destination_dpath=extractor_obj_conf.get('output_data_object_dir'))

        logger.debug(f"The following file list were downloaded from bucket:\n{file_list}")

        return file_list
