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

import sys
from petaly.core.db_extractor import DBExtractor
from petaly.utils.utils import FormatDict

from petaly.connectors.aws.redshift.rs_connector import RSConnectorIAM, RSConnectorTCP
from petaly.connectors.aws.s3.s3_connector import S3Connector


class RSExtractor(DBExtractor):
    def __init__(self, pipeline):
        #connection_params = self.get_connection_params(pipeline.source_attr)

        if pipeline.source_attr.get('connection_method') == 'iam':
            self.db_connector = RSConnectorIAM(pipeline.source_attr)
            self.s3_connector = S3Connector(pipeline.source_attr, self.db_connector.aws_session)
        elif pipeline.source_attr.get('connection_method') == 'tcp':
            self.db_connector = RSConnectorTCP(pipeline.source_attr)
            self.s3_connector = S3Connector(pipeline.source_attr, aws_session=None)
        else:
            logger.error(f"The connection_method: {pipeline.source_attr.get('connection_method')} is not supported for AWS extraction.")
            sys.exit()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.source_attr.get('aws_bucket_name')
        self.cloud_bucket_path = self.s3_connector.bucket_prefix + self.cloud_bucket_name
        self.aws_iam_role = self.pipeline.source_attr.get('aws_iam_role')

    def extract_data(self):
        super().extract_data()

    def get_query_result(self, meta_query):
        return self.db_connector.get_metaquery_result(meta_query)

    def extract_to(self, extractor_obj_conf):
        object_name = extractor_obj_conf.get('object_name')
        extract_to_stmt = extractor_obj_conf.get('extract_to_stmt')
        logger.debug(f"Statement to execute:{extract_to_stmt}")

        blob_prefix = extractor_obj_conf.get('blob_prefix')
        # cleanup object from s3 bucket
        self.s3_connector.delete_object_in_bucket(self.cloud_bucket_name, blob_prefix)

        # extract data into s3 bucket
        self.db_connector.extract_to(extract_to_stmt)

        # download files from bucket into local folder
        output_data_object_dir=extractor_obj_conf.get('output_data_object_dir')

        self.s3_connector.download_files_from_bucket(bucket_name=self.cloud_bucket_name,
                                                      blob_prefix=blob_prefix,
                                                      file_names=None,
                                                      destination_dpath=output_data_object_dir)

    def compose_extract_to_stmt(self, extract_to_stmt, extractor_obj_conf) -> dict:
        """ Its save copy statement into file
        """
        extract_data_options = self.compose_extract_options(extractor_obj_conf)
        object_name = extractor_obj_conf.get('object_name')

        extract_to_fpath = self.cloud_bucket_path + '/' + extractor_obj_conf.get('blob_prefix').strip('/')  + '/' + object_name + '_'

        extract_to_stmt = extract_to_stmt.format_map(
        					FormatDict( column_list=extractor_obj_conf.get('column_list'),
                                        schema_name=extractor_obj_conf.get('source_schema_name'),
                                        table_name=extractor_obj_conf.get('source_object_name'),
                                        extract_to_fpath = extract_to_fpath,
                                        extract_to_options=extract_data_options,
                                        iam_role=self.aws_iam_role
                                       ))

        return extract_to_stmt

    def compose_extract_options(self, extractor_obj_conf):
        """ CSV
            DELIMITER AS ','
            GZIP
            HEADER
            PARALLEL OFF
            ALLOWOVERWRITE
            MAXFILESIZE 100 MB
        """

        object_settings = extractor_obj_conf.get('object_settings')
        extract_options = ""

        if object_settings.get('header'):
            extract_options += f"HEADER "

        columns_delimiter = object_settings.get('columns_delimiter')
        if columns_delimiter == '\t':
            extract_options += "DELIMITER '\\t' "
        else:
            extract_options += f"DELIMITER '{columns_delimiter}' "

        return extract_options