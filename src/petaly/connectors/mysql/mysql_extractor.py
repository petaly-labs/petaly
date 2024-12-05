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
import csv
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

    def compose_extract_options(self, extractor_obj_conf)->dict:
        """
        """
        object_default_settings = extractor_obj_conf.get("object_default_settings")
        extract_options = {}

        # 1. FIELDS TERMINATED BY (COLUMNS DELIMITER)
        columns_delimiter = object_default_settings.get("columns_delimiter")
        extract_options.update({"delimiter": columns_delimiter})
        if columns_delimiter == "\\t":
            extract_options.update({"delimiter": "\t"})

        # 2. OPTIONALLY ENCLOSED BY
        quote_char = object_default_settings.get("quote_char")
        if quote_char == 'double-quote':
            extract_options.update({"quotechar": "\""})
            extract_options.update({"quoting": csv.QUOTE_ALL})
        elif quote_char == 'single-quote':
            extract_options.update({"quotechar": "\'"})
            extract_options.update({"quoting": csv.QUOTE_ALL})
        else:
            extract_options.update({"quotechar": None})
            extract_options.update({"quoting": csv.QUOTE_NONE})

        # 3. ESCAPED BY
        extract_options.update({"escapechar": "\\"})

        # 5. LINES TERMINATED BY
        extract_options.update({"lineterminator": "\n"})

        # 6. Has Header
        has_header = True if object_default_settings.get("header") else False
        extract_options.update({"header": has_header})

        # 7. check cleanup_linebreak_in_fields
        cleanup_linebreak_in_fields = True if object_default_settings.get("cleanup_linebreak_in_fields") else False
        extract_options.update({"cleanup_linebreak_in_fields": cleanup_linebreak_in_fields})

        return extract_options

    def extract_to(self, extractor_obj_conf):
        output_fpath = extractor_obj_conf.get("output_fpath")
        extract_to_stmt = extractor_obj_conf.get("extract_to_stmt")

        extract_options = self.compose_extract_options(extractor_obj_conf)

        logger.info(f"Output File: {output_fpath}")
        logger.info(f"Statement to execute: {extract_to_stmt}")

        self.db_connector.extract_to(extract_to_stmt, output_fpath, extract_options)

    def compose_extract_to_stmt(self, extract_to_stmt, extractor_obj_conf) -> dict:
        """ Its save copy statement into file
        """
        column_list = extractor_obj_conf.get("column_list")
        schema_name = extractor_obj_conf.get("source_schema_name")
        table_name = extractor_obj_conf.get("source_object_name")

        extract_to_stmt = extract_to_stmt.format_map(
        					FormatDict(column_list=column_list, schema_name=schema_name, table_name=table_name, null_as=''))

        return extract_to_stmt
