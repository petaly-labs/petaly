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


import os

from petaly.utils.file_handler import FileHandler
from petaly.core.f_loader import FLoader
from petaly.connectors.aws.s3.s3_connector import S3Connector


class S3Loader(FLoader):
    def __init__(self, pipeline):
        self.s3_connector = S3Connector(pipeline.target_attr, aws_session=None)
        self.f_handler = FileHandler()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.target_attr.get('aws_bucket_name')
        self.cloud_bucket_path = self.s3_connector.bucket_prefix + self.cloud_bucket_name + '/'


    def load_data(self):
        super().load_data()

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')
        blob_prefix = self.pipeline.target_attr.get('destination_prefix_path')

        self.s3_connector.drop_object_from_bucket(self.cloud_bucket_name, blob_prefix, object_name)
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')
        self.f_handler.gzip_csv_files(output_data_object_dir, cleanup_file=True)
        file_list = self.f_handler.get_specific_files(output_data_object_dir, '*.csv.gz')

        self.load_files_to_s3(file_list, self.cloud_bucket_name, blob_prefix, object_name)

    def load_files_to_s3(self, local_file_list, cloud_bucket_name, blob_prefix, object_name):
        """ upload file to S3 bucket
        """
        file_list = []

        for file_local_fpath in local_file_list:
            #file_name = os.path.basename(file_local_fpath)
            file_list.append(file_local_fpath)

        logger.debug(f"Upload files to S3; Bucket: {cloud_bucket_name}, Prefix: {blob_prefix}, Object-Name: {object_name}, File-List: {file_list}")
        self.s3_connector.load_files_to_s3_bucket(self.cloud_bucket_name, blob_prefix, object_name, file_list)
