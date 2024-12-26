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
from petaly.core.db_loader import DBLoader
from petaly.utils.utils import FormatDict
from petaly.connectors.bigquery.bq_connector import BQConnector


class BQLoader(DBLoader):
    def __init__(self, pipeline):
        self.db_connector = BQConnector()
        self.f_handler = FileHandler()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.target_attr.get('gcp_bucket_name')
        self.cloud_project_id = self.pipeline.target_attr.get('gcp_project_id')
        self.cloud_region = self.pipeline.target_attr.get('gcp_region')
        #self.cloud_service_account = self.pipeline.target_attr.get('gcp_service_account')

        self.cloud_bucket_path = self.db_connector.bucket_prefix + self.cloud_bucket_name + '/'
        self.load_from_bucket = False if self.cloud_bucket_name is None else True

    def load_data(self):
        super().load_data()

    def drop_table(self, table_id):
        self.db_connector.drop_table(table_id)

    def execute_sql(self, create_table_stmt):
        self.db_connector.execute_sql(create_table_stmt)

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')

        bq_job_config = loader_obj_conf.get('load_from_stmt')

        # ToDo: specify job_config.schema
        # example job_config.schema = [SchemaField('columnName', 'STRING', mode='nullable')]

        bq_job_config_dict = self.f_handler.string_to_dict(bq_job_config)

        table_id = self.get_table_id(loader_obj_conf.get('table_ddl_dict'))

        if self.load_from_bucket == True:
            # cleanup object from bucket
            self.db_connector.delete_gs_folder(self.cloud_bucket_name, object_name)

        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')
        self.f_handler.gzip_all_files(output_data_object_dir, cleanup_file=True)
        file_list = self.f_handler.get_specific_files(output_data_object_dir, '*.csv.gz')

        for path_to_data_file in file_list:
            if self.load_from_bucket == True:
                path_to_data_file = self.load_file_to_gs(path_to_data_file, object_name)
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

    def load_file_to_gs(self, file_local_fpath, object_name):
        """ upload file to GS bucket
        """
        file_name = os.path.basename(file_local_fpath)
        blob_dir = self.pipeline.pipeline_name + "/" + object_name

        blob_path = blob_dir + "/" + file_name
        self.db_connector.upload_blob(file_local_fpath, self.cloud_bucket_name, blob_path)
        return self.cloud_bucket_path + blob_path

    def cleanup_cloud(self):
        """ function to recursively search for files named object_metadata.yaml in the pipeline's output directory.
        """

        dir_files = self.f_handler.get_specific_files(self.pipeline.output_pipeline_dpath, self.pipeline.object_metadata_fname)

        for file in dir_files:

            table_metadata = self.f_handler.load_file_as_dict(file, 'json')
            logging.info(f"COMPOSE table ddl from {file}")
            table_ddl_dict = self.compose_table_ddl(table_metadata)

            # Conduct Paths and Names
            self.db_connector.delete_gs_folder(self.cloud_bucket_name, table_ddl_dict('object_name'))
            table_id = (f"{self.cloud_project_id}.{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}")
            self.db_connector.drop_bq_table(table_id)

        self.db_connector.delete_gs_folder(self.cloud_bucket_name, self.pipeline.pipeline_name)

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
            quote_char = "'\"'"
        elif columns_quote == 'single':
            quote_char = "\"'\""

        load_options.update({'quote_char': quote_char})

        return load_options

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        """ Its compose a copy from statement """

        load_data_options = self.compose_from_options()

        load_from_stmt = self.f_handler.load_file(self.connector_load_from_stmt_fpath)
        column_list = loader_obj_conf.get('table_ddl_dict').get('column_list')

        max_bad_records = 0
        skip_leading_rows = 1 if load_data_options.get("header") is None or True else 0
        load_from_stmt = load_from_stmt.format_map(FormatDict(skip_leading_rows=skip_leading_rows,
                                                              source_format=self.db_connector.bq_source_format,
                                                              autodetect=False,
                                                              max_bad_records=max_bad_records))
        load_from_file_fpath = loader_obj_conf.get('load_from_stmt_fpath')
        self.f_handler.save_file(load_from_file_fpath, load_from_stmt)

        return load_from_stmt