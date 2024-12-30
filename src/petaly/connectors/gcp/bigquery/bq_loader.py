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
import sys
import logging

from petaly.utils.file_handler import FileHandler
from petaly.core.db_loader import DBLoader
from petaly.utils.utils import FormatDict
from petaly.connectors.gcp.bigquery.bq_connector import BQConnector
from petaly.connectors.gcp.gs.gs_connector import GSConnector


class BQLoader(DBLoader):
    def __init__(self, pipeline):
        self.db_connector = BQConnector()
        self.gs_connector = GSConnector()
        self.f_handler = FileHandler()
        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.target_attr.get('gcp_bucket_name')
        self.cloud_project_id = self.pipeline.target_attr.get('gcp_project_id')
        self.cloud_region = self.pipeline.target_attr.get('gcp_region')
        #self.cloud_service_account = self.pipeline.target_attr.get('gcp_service_account')

        self.cloud_bucket_path = self.gs_connector.bucket_prefix + self.cloud_bucket_name + '/'
        self.load_from_bucket = False if self.cloud_bucket_name is None else True


    #def execute_sql(self, create_table_stmt):
    #    self.db_connector.execute_sql(create_table_stmt)

    def load_data(self):
        super().load_data()

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')

        #bq_job_config = loader_obj_conf.get('load_from_stmt')

        #bq_job_config_dict = self.f_handler.string_to_dict(bq_job_config)

        table_id = self.get_table_id(loader_obj_conf.get('table_ddl_dict'))
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')

        if loader_obj_conf.get('recreate_destination_object') == True:
            self.drop_table(loader_obj_conf)
        self.create_table(loader_obj_conf)

        self.f_handler.gzip_csv_files(output_data_object_dir, cleanup_file=True)
        file_list = self.f_handler.get_specific_files(output_data_object_dir, '*.csv.gz')

        if self.load_from_bucket == True:
            self.gs_connector.delete_gs_folder(self.cloud_bucket_name, self.pipeline.pipeline_name + '/' + object_name)
            bucket_file_list = self.load_files_to_gs(file_list, self.cloud_bucket_name, self.pipeline.pipeline_name, object_name)

            if len(bucket_file_list) > 0:
                file_list = bucket_file_list
            else:
                logging.error(f"Files upload to bucket failed. Try upload from local path: ")

        bq_job_config_dict = loader_obj_conf.get('load_from_stmt')

        for path_to_data_file in file_list:
            self.db_connector.load_from(bq_job_config_dict, path_to_data_file, table_id, self.load_from_bucket, self.cloud_region)


    def compose_create_table_stmt(self, loader_obj_conf):

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        create_table_stmt = table_ddl_dict.get('create_table_stmt')
        create_table_stmt = create_table_stmt.format_map(FormatDict(schema_name=table_ddl_dict.get('schema_name'),
                                                                    table_name=table_ddl_dict.get('table_name'),
                                                                    column_datatype_list=table_ddl_dict.get('column_datatype_list'),
                                                                    partition_by='',
                                                                    cluster_by='',
                                                                    table_options=''))
        loader_obj_conf.get('table_ddl_dict').update({'create_table_stmt': create_table_stmt})

        return loader_obj_conf
    def get_table_id(self, table_ddl_dict):

        return (f"{self.cloud_project_id}."
                f"{table_ddl_dict.get('schema_name')}."
                f"{table_ddl_dict.get('table_name')}")

    def drop_table(self, loader_obj_conf: dict):

        table_id = self.get_table_id(loader_obj_conf.get('table_ddl_dict'))
        self.db_connector.drop_table(table_id)

    def create_table(self, loader_obj_conf: dict):
        loader_obj_conf = self.compose_create_table_stmt(loader_obj_conf)
        self.f_handler.save_file(loader_obj_conf.get('table_ddl_dict').get('create_table_stmt_fpath'),
                                 loader_obj_conf.get('table_ddl_dict').get('create_table_stmt'))

        self.db_connector.execute_sql(loader_obj_conf.get('table_ddl_dict').get('create_table_stmt'))

    def compose_from_options(self):
        """
        """
        load_options = {}

        object_default_settings = self.pipeline.data_attributes.get("object_default_settings")
        columns_delimiter = object_default_settings.get("columns_delimiter")

        load_options.update({'delimiter': columns_delimiter})
        if columns_delimiter == "\t":
            load_options.update({'delimiter': '\\t'})

        header = True if object_default_settings.get("header") is None or True else False
        load_options.update({'header': header})

        # 3. OPTIONALLY ENCLOSED BY
        columns_quote = object_default_settings.get("columns_quote")
        quote_char = None
        if columns_quote == 'double':
            quote_char = '"'
        elif columns_quote == 'single':
            quote_char = "'"
        load_options.update({'quote_char': quote_char})

        return load_options

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        """ Its compose a copy from statement """
        load_data_options = self.compose_from_options()
        bq_load_from_stmt_fpath = self.f_handler.replace_file_extension(self.connector_load_from_stmt_fpath,'.json')
        load_from_stmt = self.f_handler.load_json(bq_load_from_stmt_fpath)

        #column_list = loader_obj_conf.get('table_ddl_dict').get('column_list')
        max_bad_records = 0
        skip_leading_rows = 1 if load_data_options.get("header") is True else 0
        field_delimiter = load_data_options.get("delimiter")
        quote_char=load_data_options.get("quote_char")
        load_from_stmt.update({"source_format": self.db_connector.bq_source_format})
        load_from_stmt.update({"skip_leading_rows": skip_leading_rows})
        load_from_stmt.update({'autodetect': False})
        load_from_stmt.update({'max_bad_records': max_bad_records})
        load_from_stmt.update({'field_delimiter': field_delimiter})
        load_from_stmt.update({'quote_character': quote_char})

        load_from_file_fpath = loader_obj_conf.get('load_from_stmt_fpath')
        load_from_file_fpath = self.f_handler.replace_file_extension(load_from_file_fpath, '.json')
        loader_obj_conf.update({'load_from_stmt_fpath':load_from_file_fpath})
        self.f_handler.save_dict_to_json(load_from_file_fpath, load_from_stmt)

        return load_from_stmt

    def load_files_to_gs(self, local_file_list, cloud_bucket_name, pipeline_name, object_name):
        """ upload file to GS bucket
        """
        bucket_file_list = []
        for file_local_fpath in local_file_list:
            file_name = os.path.basename(file_local_fpath)

            blob_path = pipeline_name + '/' + object_name + '/' + file_name
            self.gs_connector.upload_blob(file_local_fpath, cloud_bucket_name, blob_path)

            full_blob_path = self.gs_connector.bucket_prefix + cloud_bucket_name + '/' + blob_path

            bucket_file_list.append(full_blob_path)

            logging.debug(f"File upload: {full_blob_path}")

        return bucket_file_list