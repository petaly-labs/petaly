# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

import sys
from petaly.core.db_extractor import DBExtractor
from petaly.utils.utils import FormatDict

from petaly.connectors.redshift.rs_connector import RSConnectorIAM, RSConnectorTCP
from petaly.connectors.s3.s3_connector import S3Connector


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
        self.cloud_bucket_name = self.pipeline.source_attr.get('bucket_name')
        # bucket_name is required for Redshift extractor (used for staging)
        if self.cloud_bucket_name:
            self.cloud_bucket_path = self.s3_connector.bucket_prefix + self.cloud_bucket_name
        else:
            logger.error(f"bucket_name is required for Redshift source but was not found in source_attributes")
            raise ValueError("bucket_name is required in source_attributes for Redshift connector")
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

        # Determine format based on target connector
        # Redshift UNLOAD supports PARQUET and JSON formats
        target_connector_id = self.pipeline.target_connector_id
        target_category = self.m_conf.get_connector_category(target_connector_id)
        
        if target_category == 'file' and target_connector_id in ('parquet', 'json'):
            # Redshift UNLOAD format: FORMAT PARQUET or FORMAT JSON
            format_clause = f"FORMAT AS {target_connector_id.upper()}"
            # Update the SQL template to use the target format instead of CSV
            # Replace "FORMAT AS CSV" with the target format
            if 'FORMAT AS CSV' in extract_to_stmt:
                extract_to_stmt = extract_to_stmt.replace('FORMAT AS CSV', format_clause)
            elif 'FORMAT AS' not in extract_to_stmt:
                # If no format clause exists, add it before GZIP
                if 'GZIP' in extract_to_stmt:
                    extract_to_stmt = extract_to_stmt.replace('GZIP', f'{format_clause}\nGZIP')
                else:
                    # Add format clause before the options
                    extract_to_stmt = extract_to_stmt.replace('{extract_to_options}', f'{format_clause}\n{{extract_to_options}}')
            
            # Update extension for Parquet/JSON
            # Note: Redshift UNLOAD with GZIP will create .parquet.gz or .json.gz files
            if target_connector_id == 'parquet':
                if "EXTENSION 'csv.gz'" in extract_to_stmt:
                    extract_to_stmt = extract_to_stmt.replace("EXTENSION 'csv.gz'", "EXTENSION 'parquet'")
                elif "EXTENSION 'csv'" in extract_to_stmt:
                    extract_to_stmt = extract_to_stmt.replace("EXTENSION 'csv'", "EXTENSION 'parquet'")
            elif target_connector_id == 'json':
                if "EXTENSION 'csv.gz'" in extract_to_stmt:
                    extract_to_stmt = extract_to_stmt.replace("EXTENSION 'csv.gz'", "EXTENSION 'json.gz'")
                elif "EXTENSION 'csv'" in extract_to_stmt:
                    extract_to_stmt = extract_to_stmt.replace("EXTENSION 'csv'", "EXTENSION 'json.gz'")
            
            logger.info(f"Redshift will export directly to {target_connector_id} format (target connector: {target_connector_id})")

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