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

from petaly.utils.file_handler import FileHandler
from petaly.core.f_loader import FLoader
from petaly.utils.utils import FormatDict
from petaly.connectors.gcp.gs.gs_connector import GSConnector


class GSLoader(FLoader):
    def __init__(self, pipeline):
        self.gs_connector = GSConnector()
        self.f_handler = FileHandler()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.target_attr.get('gcp_bucket_name')
        self.cloud_project_id = self.pipeline.target_attr.get('gcp_project_id')
        self.cloud_region = self.pipeline.target_attr.get('gcp_region')
        #self.cloud_service_account = self.pipeline.target_attr.get('gcp_service_account')

        self.cloud_bucket_path = self.gs_connector.bucket_prefix + self.cloud_bucket_name + '/'
        self.load_from_bucket = False if self.cloud_bucket_name is None else True

    def load_data(self):
        super().load_data()

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')

        self.gs_connector.delete_gs_folder(self.cloud_bucket_name, object_name)

        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')
        self.f_handler.gzip_csv_files(output_data_object_dir, cleanup_file=True)
        file_list = self.f_handler.get_specific_files(output_data_object_dir, '*.csv.gz')

        if self.load_from_bucket == True:
            blob_prefix_dir = self.pipeline.target_attr.get('destination_blob_dir')
            bucket_file_list = self.load_files_to_gs(file_list, self.cloud_bucket_name, blob_prefix_dir, object_name)


    def deprecated_cleanup_cloud(self):
        """ function to recursively search for files named object_metadata.yaml in the pipeline's output directory.
        """

        dir_files = self.f_handler.get_specific_files(self.pipeline.output_pipeline_dpath, self.pipeline.object_metadata_fname)

        for file in dir_files:
            # Conduct Paths and Names
            self.gs_connector.delete_gs_folder(self.cloud_bucket_name, file)

        self.f_connector.delete_gs_folder(self.cloud_bucket_name, self.pipeline.pipeline_name)

    def deprecated_load_file_to_gs(self, file_local_fpath, cloud_bucket_name, object_name ):
        """ upload file to GS bucket
        """

        file_name = os.path.basename(file_local_fpath)

        blob_prefix_dir = cloud_bucket_name
        if blob_prefix_dir is None:
            blob_prefix_dir = ''

        blob_dir = blob_prefix_dir + '/'+ self.pipeline.pipeline_name + "/" + object_name

        blob_path = blob_dir + "/" + file_name
        self.gs_connector.upload_blob(file_local_fpath, cloud_bucket_name, blob_path)
        return self.cloud_bucket_path + blob_path

    def load_files_to_gs(self, local_file_list, cloud_bucket_name, blob_prefix_dir, object_name):
        """ upload file to GS bucket
        """
        bucket_file_list = []
        for file_local_fpath in local_file_list:
            file_name = os.path.basename(file_local_fpath)

            if blob_prefix_dir is None:
                blob_prefix_dir = ''

            blob_prefix_dir = blob_prefix_dir.strip('/')

            blob_path = blob_prefix_dir + '/' + object_name + '/' + file_name
            self.gs_connector.upload_blob(file_local_fpath, cloud_bucket_name, blob_path)

            full_blob_path = self.gs_connector.bucket_prefix + cloud_bucket_name + '/' + blob_path
            bucket_file_list.append(self.gs_connector.bucket_prefix + cloud_bucket_name + '/' + blob_path)

            logging.debug(f"File upload: {full_blob_path}")

        return bucket_file_list