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

from petaly.connectors.gcp.bigquery.bq_connector import BQConnector
from petaly.connectors.gcp.gs.gs_connector import GSConnector
from petaly.core.db_extractor import DBExtractor
from petaly.utils.utils import FormatDict


class BQExtractor(DBExtractor):
    def __init__(self, pipeline):
        self.db_connector = BQConnector()
        self.gs_connector = GSConnector()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.source_attr.get('gcp_bucket_name')
        self.cloud_project_id = self.pipeline.source_attr.get('gcp_project_id')
        self.cloud_region = self.pipeline.source_attr.get('gcp_region')
        #self.cloud_service_account = self.pipeline.source_attr.get('gcp_service_account')

    def extract_data(self):
        super().extract_data()

    def get_query_result(self, meta_query):
        query_result = self.db_connector.get_metadata_result(meta_query)
        return query_result

    def extract_to(self, extractor_obj_conf):

        object_name = extractor_obj_conf.get('object_name')
        self.gs_connector.delete_gs_folder(self.cloud_bucket_name, object_name)

        # run export data
        extract_to_stmt = extractor_obj_conf.get('extract_to_stmt')
        extract_to_dict = self.f_handler.string_to_dict(extract_to_stmt)
        table_ref = extract_to_dict.get('table_ref')
        destination_uri = extract_to_dict.get('destination_uri')

        self.db_connector.extract_to(table_ref, destination_uri, self.cloud_region)

        output_file_dir = os.path.dirname(extractor_obj_conf.get('output_object_fpath'))

        blob_prefix = (self.pipeline.pipeline_name +'/'+ object_name).strip('/')
        # download export from bucket into local folder
        downloaded_file_list = self.gs_connector.download_files_from_bucket(
                                                    self.cloud_bucket_name,
                                                    blob_prefix,
                                                    specific_file_list=None,
                                                    destination_directory=output_file_dir)

        logging.debug(f"Following file list were downloaded from bucket:\n{downloaded_file_list}")

    def compose_extract_to_stmt(self, extract_to_stmt, extractor_obj_conf) -> dict:
        """ Its save copy statement into file
        """
        project_id = self.cloud_project_id
        object_name = extractor_obj_conf.get('object_name')
        dataset_id = extractor_obj_conf.get('source_schema_name')
        table_name = extractor_obj_conf.get('source_object_name')

        destination_blob_name = f"{self.pipeline.pipeline_name}/{object_name}/{object_name}_*.csv"
        destination_uri = f"{self.gs_connector.bucket_prefix }{self.cloud_bucket_name}/{destination_blob_name}"
        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        extract_to_stmt = extract_to_stmt.format_map(FormatDict(table_ref=table_ref, destination_uri=destination_uri))

        return extract_to_stmt
