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
from petaly.core.db_loader import DBLoader
from petaly.utils.utils import FormatDict


class PsqlLoader(DBLoader):

    def __init__(self, pipeline):
        self.db_connector = PsqlConnector(pipeline.target_attr)
        super().__init__(pipeline)

    def load_data(self):
        super().load_data()

    def drop_table(self, schema_table_name):
        self.db_connector.drop_table(schema_table_name)

    def execute_sql(self, create_table_stmt):
        self.db_connector.execute_sql(create_table_stmt)

    def load_from(self, loader_obj_conf):
        #object_name = loader_obj_conf.get('object_name')
        load_from_stmt = loader_obj_conf.get('load_from_stmt')

        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')

        # Unzip any compressed files that may be present.
        self.f_handler.gunzip_all_files(output_data_object_dir)

        # collect all csv file
        file_list = self.f_handler.get_specific_files(output_data_object_dir,'*.csv')

        for path_to_data_file in file_list:
            logger.info(f"Source file: {path_to_data_file}")
            logger.info(f"Statement to execute: {load_from_stmt}")

            self.db_connector.load_from(load_from_stmt, path_to_data_file)

    def compose_create_table_stmt(self, loader_obj_conf):

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        create_table_stmt = table_ddl_dict.get('create_table_stmt')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        column_datatype_list = table_ddl_dict.get('column_datatype_list')

        create_table_stmt = create_table_stmt.format_map(FormatDict(schema_table_name=schema_table_name,
                                                                    column_datatype_list=column_datatype_list,
                                                                    partition_by='',
                                                                    cluster_by='',
                                                                    table_options='',
                                                                    alter_table_primary_or_unique_key=''))
        loader_obj_conf.get('table_ddl_dict').update({'create_table_stmt': create_table_stmt})

        return loader_obj_conf

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        """ Its compose a copy from statement """

        load_from_stmt = self.f_handler.load_file(self.connector_load_from_stmt_fpath)
        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        column_list = '' if table_ddl_dict.get('column_list') == None else '(' + table_ddl_dict.get('column_list') + ')'

        has_header = True if data_object.skip_leading_rows > 0 else False

        load_from_stmt = load_from_stmt.format_map(FormatDict(schema_table_name=schema_table_name,
                                                              column_list=column_list,
                                                              has_header=has_header))
        load_from_file_fpath = loader_obj_conf.get('load_from_stmt_fpath')

        self.f_handler.save_file(load_from_file_fpath, load_from_stmt)
        return load_from_stmt


    def drop_table(self, loader_obj_conf: dict):

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        self.db_connector.drop_table(schema_table_name)

    def create_table(self, loader_obj_conf: dict):
        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        loader_obj_conf = self.compose_create_table_stmt(loader_obj_conf)
        self.f_handler.save_file(table_ddl_dict.get('create_table_stmt_fpath'),
                                 table_ddl_dict.get('create_table_stmt'))

        self.db_connector.execute_sql(loader_obj_conf.get('table_ddl_dict').get('create_table_stmt'))
