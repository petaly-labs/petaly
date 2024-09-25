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

from petaly.connectors.mysql.mysql_connector import MysqlConnector
from petaly.core.db_extractor import DBExtractor
from petaly.utils.utils import FormatDict


class MysqlExtractor(DBExtractor):
    def __init__(self, pipeline):
        self.db_connector = MysqlConnector(pipeline.source_attr)
        super().__init__(pipeline)

    def extract_data(self):
        super().extract_data()

    def get_query_result(self, meta_query):
        return self.db_connector.get_query_result(meta_query)

    def extract_to(self, extractor_obj_conf):
        output_fpath = extractor_obj_conf.get('output_fpath')
        extract_to_stmt = extractor_obj_conf.get('extract_to_stmt')
        logger.info(f"Target File: {output_fpath}")
        logger.info(f"Statement to execute: {extract_to_stmt}")

        self.db_connector.extract_to(extract_to_stmt, output_fpath)

    def compose_extract_to_stmt(self, extract_to_stmt, extractor_obj_conf) -> dict:
        """ Its save copy statement into file
        """
        column_list = extractor_obj_conf.get('column_list')
        schema_name = extractor_obj_conf.get('source_schema_name')
        table_name = extractor_obj_conf.get('source_object_name')

        extract_to_stmt = extract_to_stmt.format_map(
        					FormatDict(column_list=column_list, schema_name=schema_name, table_name=table_name, null_as=''))

        return extract_to_stmt
