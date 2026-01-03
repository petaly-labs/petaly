# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

import sys

from petaly.utils.utils import FormatDict
from petaly.core.db_loader import DBLoader

from petaly.connectors.redshift.rs_connector import RSConnectorIAM, RSConnectorTCP
from petaly.connectors.s3.s3_connector import S3Connector


class RSLoader(DBLoader):

    def __init__(self, pipeline):
        #connection_params = self.get_connection_params(pipeline.target_attr)

        if pipeline.target_attr.get('connection_method') == 'iam':
            self.db_connector = RSConnectorIAM(pipeline.target_attr)
            self.s3_connector = S3Connector(pipeline.target_attr, aws_session=self.db_connector.aws_session)
        elif pipeline.target_attr.get('connection_method') == 'tcp':
            self.db_connector = RSConnectorTCP(pipeline.target_attr)
            self.s3_connector = S3Connector(pipeline.target_attr, aws_session=None)
        else:
            logger.error(f"The connection_method: {pipeline.source_attr.get('connection_method')} is not supported for AWS load.")
            sys.exit()

        super().__init__(pipeline)

        self.cloud_bucket_name = self.pipeline.target_attr.get('bucket_name')
        # bucket_name is required for Redshift (used for staging)
        if self.cloud_bucket_name:
            self.cloud_bucket_path = self.s3_connector.bucket_prefix + self.cloud_bucket_name
        else:
            logger.error(f"bucket_name is required for Redshift target but was not found in target_attributes")
            raise ValueError("bucket_name is required in target_attributes for Redshift connector")
        self.aws_iam_role = self.pipeline.target_attr.get('aws_iam_role')

    def load_data(self):
        super().load_data()

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

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')
        blob_prefix = loader_obj_conf.get('blob_prefix')
        # 1. cleanup object from bucket
        self.s3_connector.delete_object_in_bucket(self.cloud_bucket_name, blob_prefix)

        # 2. drop and recreate table
        if loader_obj_conf.get('recreate_destination_object') == True:
            self.drop_table(loader_obj_conf)

        self.create_table(loader_obj_conf)
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')

        # Detect file format in output directory
        # Redshift can load Parquet and JSON directly without conversion
        import glob
        import os
        
        # Check for Parquet files first (Redshift supports direct Parquet loading)
        parquet_files = glob.glob(os.path.join(output_data_object_dir, '*.parquet'))
        parquet_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.parquet.gz')))
        
        # Check for JSON files (Redshift supports direct JSON loading)
        json_files = glob.glob(os.path.join(output_data_object_dir, '*.json'))
        json_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.json.gz')))
        
        # If Parquet or JSON files exist, use them directly (no CSV conversion needed)
        if parquet_files:
            file_list = list(set([f for f in parquet_files if os.path.isfile(f)]))
            logger.info(f"Redshift will load Parquet files directly (no CSV conversion needed): {len(file_list)} files")
        elif json_files:
            file_list = list(set([f for f in json_files if os.path.isfile(f)]))
            logger.info(f"Redshift will load JSON files directly (no CSV conversion needed): {len(file_list)} files")
        else:
            # No Parquet/JSON files found, fall back to CSV
            # Gzip all CSV files (they will be parsed based on delimiter, not extension)
            self.f_handler.gzip_csv_files(output_data_object_dir, cleanup_file=True)
            
            # Collect all files (regardless of extension) - delimiter will determine how to parse them
            all_files = []
            for pattern in ['*', '*.gz', '*.csv', '*.tsv', '*.txt']:
                all_files.extend(glob.glob(os.path.join(output_data_object_dir, pattern)))
            file_list = list(set([f for f in all_files if os.path.isfile(f)]))

        self.s3_connector.upload_files_to_bucket(self.cloud_bucket_name, blob_prefix, file_list)

        s3_file_list = self.s3_connector.get_bucket_file_list(self.cloud_bucket_name, blob_prefix)
        load_from_stmt = loader_obj_conf.get('load_from_stmt')

        for data_fpath in s3_file_list:
            path_to_data_file = self.cloud_bucket_path + '/' + data_fpath
            load_from_stmt = load_from_stmt.format_map(
                FormatDict(path_to_data_file=path_to_data_file))

            self.db_connector.load_from(load_from_stmt)

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

    def compose_from_options(self, loader_obj_conf):
        """
        """
        load_options = ""
        object_settings = loader_obj_conf.get("object_settings")
        
        # Detect source file format to determine if we can load Parquet/JSON directly
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')
        import glob
        import os
        
        # Check for Parquet or JSON files
        parquet_files = glob.glob(os.path.join(output_data_object_dir, '*.parquet'))
        parquet_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.parquet.gz')))
        json_files = glob.glob(os.path.join(output_data_object_dir, '*.json'))
        json_files.extend(glob.glob(os.path.join(output_data_object_dir, '*.json.gz')))
        
        # Determine format
        if parquet_files:
            # Parquet format - Redshift COPY supports FORMAT AS PARQUET
            load_options += "FORMAT AS PARQUET "
            logger.info(f"Redshift will load Parquet format directly (no CSV conversion needed)")
        elif json_files:
            # JSON format - Redshift COPY supports FORMAT AS JSON
            load_options += "FORMAT AS JSON "
            logger.info(f"Redshift will load JSON format directly (no CSV conversion needed)")
        else:
            # CSV format - include delimiter, header, and quote options
            load_options += "FORMAT AS CSV "
            columns_delimiter = object_settings.get("columns_delimiter")

            if columns_delimiter == '\t':
                load_options += "DELIMITER '\\t' "
            else:
                load_options += f"DELIMITER '{columns_delimiter}' "

            skip_leading_rows = 1 if object_settings.get("header") is None or True else 0
            load_options += f"IGNOREHEADER {skip_leading_rows} "

            columns_quote = object_settings.get("columns_quote")
            if columns_quote not in ('double','single'):
                load_options += "REMOVEQUOTES "

            # Add NULL handling for \N (common null representation in TSV/CSV files)
            # Redshift COPY recognizes \N as NULL by default, but EMPTYASNULL ensures empty strings are also treated as NULL
            # This provides consistent NULL handling across all column types
            load_options += "EMPTYASNULL "

        # GZIP compression is supported for all formats
        load_options += "GZIP "

        return load_options

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        """ Its compose a copy from statement """
        load_data_options = self.compose_from_options(loader_obj_conf)
        load_from_stmt = self.f_handler.load_file(self.connector_load_from_stmt_fpath)

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        column_list = '' if table_ddl_dict.get('column_list') == None else '(' + table_ddl_dict.get('column_list') + ')'

        load_from_stmt = load_from_stmt.format_map(FormatDict(schema_table_name=schema_table_name,
                                                               column_list=column_list,
                                                              iam_role=self.aws_iam_role,
                                                              load_from_options=load_data_options))
        load_from_file_fpath = loader_obj_conf.get('load_from_stmt_fpath')
        self.f_handler.save_file(load_from_file_fpath, load_from_stmt)

        return load_from_stmt

