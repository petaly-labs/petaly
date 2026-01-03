# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

from petaly.connectors.bigquery.bq_connector import BQConnector
from petaly.connectors.gs.gs_connector import GSConnector
from petaly.core.db_extractor import DBExtractor
from petaly.utils.utils import FormatDict


class BQExtractor(DBExtractor):
    def __init__(self, pipeline):
        self.db_connector = BQConnector()
        self.gs_connector = GSConnector()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.source_attr.get('bucket_name')
        self.cloud_project_id = self.pipeline.source_attr.get('gcp_project_id')
        self.cloud_region = self.pipeline.source_attr.get('gcp_region')
        # bucket_name is optional for BigQuery extractor (can extract to local folder)
        
    def extract_data(self):
        super().extract_data()

    def get_query_result(self, meta_query):
        query_result = self.db_connector.get_metadata_result(meta_query)
        return query_result

    def extract_to(self, extractor_obj_conf):

        object_name = extractor_obj_conf.get('object_name')

        # run export data
        extract_to_stmt = extractor_obj_conf.get('extract_to_stmt')
        extract_to_dict = self.f_handler.string_to_dict(extract_to_stmt)
        table_ref = extract_to_dict.get('table_ref')
        destination_uri = extract_to_dict.get('destination_uri')
        output_data_object_dir = extractor_obj_conf.get('output_data_object_dir')
        blob_prefix = extractor_obj_conf.get('blob_prefix')
        
        # Determine destination format based on target connector
        # BigQuery supports direct export to PARQUET and JSON
        target_connector_id = self.pipeline.target_connector_id
        target_category = self.m_conf.get_connector_category(target_connector_id)
        destination_format = None
        
        if target_category == 'file' and target_connector_id in ('parquet', 'json'):
            destination_format = target_connector_id
            logger.info(f"BigQuery will export directly to {destination_format} format (target connector: {target_connector_id})")

        # cleanup object from GCS bucket
        self.gs_connector.delete_object_in_bucket(self.cloud_bucket_name, blob_prefix)

        # extract data into GCS bucket with specified format
        self.db_connector.extract_to(table_ref, destination_uri, self.cloud_region, destination_format=destination_format)
        # download files from bucket into local folder
        downloaded_file_list = self.gs_connector.download_files_from_bucket(
                                                    bucket_name=self.cloud_bucket_name,
                                                    blob_prefix=blob_prefix,
                                                    file_names=None,
                                                    destination_dpath=output_data_object_dir)

        logger.debug(f"Following file list were downloaded from bucket:\n{downloaded_file_list}")

    def compose_extract_to_stmt(self, extract_to_stmt, extractor_obj_conf) -> dict:
        """ Its save copy statement into file
        """
        project_id = self.cloud_project_id
        object_name = extractor_obj_conf.get('object_name')
        dataset_id = extractor_obj_conf.get('source_schema_name')
        table_name = extractor_obj_conf.get('source_object_name')

        # Determine file extension based on target connector
        # BigQuery supports direct export to PARQUET and JSON
        target_connector_id = self.pipeline.target_connector_id
        target_category = self.m_conf.get_connector_category(target_connector_id)
        
        if target_category == 'file' and target_connector_id in ('parquet', 'json'):
            file_extension = target_connector_id
        else:
            file_extension = 'csv'  # Default to CSV
        
        destination_blob_name = extractor_obj_conf.get('blob_prefix').strip('/') + '/' + object_name + f'_*.{file_extension}'

        destination_uri = f"{self.gs_connector.bucket_prefix }{self.cloud_bucket_name}/{destination_blob_name}"

        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        extract_to_stmt = extract_to_stmt.format_map(FormatDict(table_ref=table_ref, destination_uri=destination_uri))

        return extract_to_stmt
