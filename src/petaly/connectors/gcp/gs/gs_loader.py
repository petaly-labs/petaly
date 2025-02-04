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

from petaly.utils.file_handler import FileHandler
from petaly.core.f_loader import FLoader
from petaly.connectors.gcp.gs.gs_connector import GSConnector


class GSLoader(FLoader):
    def __init__(self, pipeline):
        self.gs_connector = GSConnector()
        self.f_handler = FileHandler()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.target_attr.get('gcp_bucket_name')
        self.cloud_bucket_path = self.gs_connector.bucket_prefix + self.cloud_bucket_name + '/'
        self.load_from_bucket = False if self.cloud_bucket_name is None else True
        self.cloud_region = self.pipeline.target_attr.get('gcp_region')
        self.cloud_project_id = self.pipeline.target_attr.get('gcp_project_id')

    def load_data(self):
        super().load_data(file_to_gzip=True)

    def load_from(self, loader_obj_conf):
        """ Load files to bucket
        """
        self.gs_connector.delete_object_in_bucket(self.cloud_bucket_name, loader_obj_conf.get('blob_prefix') )
        self.gs_connector.upload_files_to_bucket(self.cloud_bucket_name, loader_obj_conf.get('blob_prefix'),loader_obj_conf.get('file_list') )

