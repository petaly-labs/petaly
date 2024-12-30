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

from petaly.connectors.postgres.psql_connector import PsqlConnector
from petaly.core.db_extractor import DBExtractor
from petaly.utils.utils import FormatDict


class PsqlExtractor(DBExtractor):
    def __init__(self, pipeline):
        self.db_connector = PsqlConnector(pipeline.source_attr)
        super().__init__(pipeline)

    def extract_data(self):
        super().extract_data()

    def get_query_result(self, meta_query):
        return self.db_connector.get_query_result(meta_query)

    def extract_to(self,extractor_obj_conf):
        output_object_fpath = extractor_obj_conf.get('output_object_fpath')
        extract_to_stmt = extractor_obj_conf.get('extract_to_stmt')
        logger.debug(f"Output file: {output_object_fpath}")
        logger.debug(f"Statement to execute:\n{extract_to_stmt}")

        self.db_connector.extract_to(extract_to_stmt, output_object_fpath)

    def compose_extract_options(self, extractor_obj_conf):
        "WITH (FORMAT CSV, DELIMITER ',', HEADER true, FORCE_QUOTE *, ENCODING 'UTF-8');"
        object_settings = extractor_obj_conf.get('object_settings')
        extract_options = ""

        extract_options += f", DELIMITER '{object_settings.get('columns_delimiter')}'"
        extract_options += f", HEADER {object_settings.get('header')}"

        # 3. OPTIONALLY ENCLOSED BY
        columns_quote = object_settings.get("columns_quote")
        if columns_quote in ('double','single'):
            if columns_quote == 'double':
                extract_options += f", QUOTE '\"'"
            elif columns_quote == 'single':
                extract_options += f", QUOTE \"'\""
            extract_options += f", FORCE_QUOTE *"

        return extract_options

    def compose_extract_to_stmt(self, extract_to_stmt, extractor_obj_conf) -> dict:
        """ Its save copy statement into file """
        column_list = extractor_obj_conf.get('column_list')
        schema_name = extractor_obj_conf.get('source_schema_name')
        table_name = extractor_obj_conf.get('source_object_name')
        copy_to_options = self.compose_extract_options(extractor_obj_conf)
        extract_to_stmt = extract_to_stmt.format_map(
        					FormatDict(column_list=column_list, schema_name=schema_name, table_name=table_name, copy_to_options=copy_to_options))

        return extract_to_stmt
