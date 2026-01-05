# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

from petaly.connectors.gs.gs_connector import GSConnector
from petaly.core.f_extractor import FExtractor
from petaly.utils.file_handler import FileHandler


class GSExtractor(FExtractor):
    def __init__(self, pipeline):
        self.gs_connector = GSConnector()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.source_attr.get('bucket_name')
        self.cloud_project_id = self.pipeline.source_attr.get('gcp_project_id')
        # bucket_name is optional for GCS extractor (can extract from local folder)
        self.cloud_region = self.pipeline.source_attr.get('gcp_region')
        self.file_format = 'csv'
        self.f_handler = FileHandler()

    def extract_data(self):
        super().extract_data()

    def extract_to(self, extractor_obj_conf):
        """ Download export from bucket into local folder
        """
        output_data_object_dir = extractor_obj_conf.get('output_data_object_dir')
        self.f_handler.cleanup_dir(output_data_object_dir)

        file_list = self.gs_connector.download_files_from_bucket(
                                                    bucket_name=self.cloud_bucket_name,
                                                    blob_prefix=extractor_obj_conf.get('blob_prefix'),
                                                    file_names=extractor_obj_conf.get('file_names'),
                                                    destination_dpath=output_data_object_dir)

        logger.debug(f"The following file list were downloaded from bucket:\n{file_list}")

        return file_list
