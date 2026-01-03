# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

from petaly.utils.file_handler import FileHandler
from petaly.core.f_loader import FLoader
from petaly.connectors.s3.s3_connector import S3Connector


class S3Loader(FLoader):
    def __init__(self, pipeline):
        self.s3_connector = S3Connector(pipeline.target_attr, aws_session=None)
        self.f_handler = FileHandler()

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.target_attr.get('bucket_name')
        # bucket_name is required for S3
        if self.cloud_bucket_name:
            self.cloud_bucket_path = self.s3_connector.bucket_prefix + self.cloud_bucket_name + '/'
        else:
            logger.error(f"bucket_name is required for S3 target but was not found in target_attributes")
            raise ValueError("bucket_name is required in target_attributes for S3 connector")

    def load_data(self):
        super().load_data(file_to_gzip=True)

    def load_from(self, loader_obj_conf):
        """ Load files to bucket
        """
        self.s3_connector.delete_object_in_bucket(self.cloud_bucket_name, loader_obj_conf.get('blob_prefix'))
        self.s3_connector.upload_files_to_bucket(self.cloud_bucket_name, loader_obj_conf.get('blob_prefix'), loader_obj_conf.get('file_list'))
