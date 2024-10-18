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

from abc import ABC, abstractmethod

from petaly.core.composer import Composer
from petaly.utils.utils import measure_time
from petaly.utils.file_handler import FileHandler
from petaly.core.type_mapping import TypeMapping
from petaly.core.object_metadata import ObjectMetadata
from petaly.core.data_object import DataObject


class DBLoader(ABC):

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.f_handler = FileHandler()
        self.composer = Composer(pipeline)
        self.m_conf = pipeline.m_conf
        self.object_metadata = ObjectMetadata(pipeline)

        self.type_mapping = TypeMapping(self.pipeline)

        if self.m_conf.set_loader_paths(self.pipeline.target_connector_id):
            self.connector_load_from_stmt_fpath = self.m_conf.connector_load_from_stmt_fpath
            self.connector_create_table_stmt_fpath = self.m_conf.connector_create_table_stmt_fpath


    @abstractmethod
    def execute_sql(self, create_table_stmt):
        pass

    @abstractmethod
    def load_from(self, object_load_conf):
        pass

    @abstractmethod
    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        pass

    @abstractmethod
    def drop_table(self, loader_obj_conf: dict):
        pass

    @abstractmethod
    def create_table(self, loader_obj_conf: dict):
        pass

    @measure_time
    def load_data(self):
        """  Load data into Postgres. Recreate table if parameter recreate_table=True. """

        logger.info(f"Start the load process to the target storage: {self.pipeline.target_connector_id}.")

        # 1. get and run all objects
        object_list = self.composer.get_object_list_from_output_dir()
        for object_name in object_list:
            loader_obj_conf = {}
            loader_obj_conf.update({'object_name': object_name})

            output_metadata_object_dir = self.pipeline.output_object_metadata_dpath.format(object_name=object_name)
            loader_obj_conf.update({'output_metadata_object_dir': output_metadata_object_dir})

            output_data_object_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
            loader_obj_conf.update({'output_data_object_dir': output_data_object_dir})

            # 2. search for metadata file and load table metadata
            metadata_file = self.pipeline.output_object_metadata_fpath.format(object_name=object_name)
            table_metadata = self.f_handler.load_file_as_dict(metadata_file, 'json')

            # 3. compose table DDL components
            data_object = DataObject(self.pipeline, object_name)
            table_ddl_dict = self.compose_table_ddl(data_object, table_metadata)
            loader_obj_conf.update({'table_ddl_dict': table_ddl_dict})

            # 4. drop and recreate table
            if data_object.recreate_target_object == True:
                self.drop_table(loader_obj_conf)

            self.create_table(loader_obj_conf)

            # 5. compose statement load_from
            output_load_from_stmt_fpath = self.pipeline.output_load_from_stmt_fpath.format(object_name=object_name)
            loader_obj_conf.update({'load_from_stmt_fpath': output_load_from_stmt_fpath})

            load_from_stmt = self.compose_load_from_stmt(data_object, loader_obj_conf)
            loader_obj_conf.update({'load_from_stmt': load_from_stmt})

            # 6. load data into table
            self.load_from(loader_obj_conf)


    #def get_data_object(self, object_name):
    #    return self.composer.get_data_object(object_name)

    def get_data_object(self, object_name):
        return DataObject(self.pipeline, object_name)

    def compose_table_ddl(self, data_object, table_metadata: dict) -> (dict):
        """ Its composes statement for create table command by using table_metadata dictionary. """

        object_name = data_object.object_name
        table_name = data_object.target_object_name

        if table_name is None:
            # table_name = table_metadata.get('source_object_name')
            table_name = object_name

        schema_name = self.pipeline.target_attr.get('database_schema')

        if schema_name is not None:
            schema_table_name = f"{schema_name}.{table_name}"
        else:
            schema_table_name = table_name

        logger.info(f"Compose DDL table: {schema_table_name}")

        table_ddl_dict = {}
        table_ddl_dict.update({'table_name': table_name})
        table_ddl_dict.update({'schema_name': schema_name})
        columns_meta_arr = table_metadata.get('columns')

        connector_create_table_stmt_fpath = self.f_handler.load_file(self.connector_create_table_stmt_fpath)
        type_mapping = self.type_mapping.get_type_mapping()
        column_list = ""
        column_datatype_list = ""
        primary_key = ''

        # loop each line
        for i, column_meta in enumerate(columns_meta_arr):

            column_name = self.db_connector.column_quotes + self.composer.normalise_column_name(column_meta.get('column_name')) + self.db_connector.column_quotes
            column_list += column_name

            column_datatype_list += column_name
            column_type = type_mapping.get(column_meta.get('data_type'))

            if column_type is None:
                logger.error(f"Type mapping doesn't exists for source-connector-id: {self.pipeline.source_connector_id}, table: {table_name}, "
                              f"column: {column_name}, data-type: {column_meta.get('data_type')}, "
                              f"target_connector_id {self.pipeline.target_connector_id}."
                              )

            column_datatype_list += " " + column_type
            mode = ' NOT NULL' if column_meta.get('is_nullable') == 'NO' else ''
            column_datatype_list += mode

            if column_meta.get('primary_key') != None:
                primary_key += column_meta.get('primary_key') + ','

            column_datatype_list += ",\n"
            column_list  += ",\n"

        if primary_key != '':
            primary_key = primary_key.rstrip(',')
            # ToDo: recreate generic primary key by adding ALTER TABLE
            primary_key = f"PRIMARY KEY ({primary_key})"
            #column_datatype_list += primary_key

        column_datatype_list = column_datatype_list.rstrip(',\n')
        column_list = column_list.rstrip(',\n')

        create_table_stmt_fpath = self.pipeline.output_create_table_stmt_fpath.format(object_name=object_name)
        table_ddl_dict.update({'column_datatype_list':column_datatype_list, 'primary_key':primary_key, 'column_list':column_list})
        table_ddl_dict.update({'create_table_stmt_fpath': create_table_stmt_fpath, 'create_table_stmt': connector_create_table_stmt_fpath})

        return table_ddl_dict

    def get_column_type_with_precision(self, column_meta, type_mapping):
        """ This functions currently is not in use. """
        column_type = type_mapping.get(column_meta.get('data_type'))
        column_type_with_precision = column_type

        if column_type in ('character varying', 'varchar'):
            max_value = '21845'

            if column_meta.get('character_maximum_length') is not None:
                column_type_with_precision = column_type + '(' + column_meta.get('character_maximum_length') + ')'
            else:
                # ToDo to find a way to get max values for each database type
                column_type_with_precision = column_type + '(' + max_value + ')'

        return column_type_with_precision

    def compose_column_list(self, table_meta):
        """ It's a sub function to compose column list """
        columns_meta_arr = table_meta.get('columns')
        columns_list = ''
        for i, column_meta in enumerate(columns_meta_arr):
            column_name = self.composer.normalise_column_name(column_meta.get('column_name'))
            columns_list += self.db_connector.column_quotes + column_name + self.db_connector.column_quotes
            columns_list += ","

        columns_list = columns_list.rstrip(',')
        return columns_list

