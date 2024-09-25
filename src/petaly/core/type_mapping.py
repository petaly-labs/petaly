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
from petaly.utils.file_handler import FileHandler

class TypeMapping():
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.f_handler = FileHandler()
        self.m_conf = pipeline.m_conf

    def get_type_mapping(self):
        """ get target-source type mapping. Return the first founded.

        """
        # 1. load type_mapping from pipeline if exists and return
        pipeline_type_mapping_fpath = os.path.join(self.pipeline.pipeline_dpath, self.m_conf.type_mapping_fname)

        if self.f_handler.is_file(pipeline_type_mapping_fpath):
            logger.info(f"Load data type mapping from: {pipeline_type_mapping_fpath}")
            type_mapping_dict = self.f_handler.load_json(pipeline_type_mapping_fpath)
            return type_mapping_dict

        # 2. load type_mapping as configured in class_config paths
        type_mapping_path_list = self.m_conf.get_type_mapping_paths(self.pipeline.target_connector_id)

        for type_mapping_fpath in type_mapping_path_list:
            type_mapping_fpath = type_mapping_fpath.format(source_connector_id=self.pipeline.source_connector_id)

            if self.f_handler.is_file(type_mapping_fpath):
                logger.info(f"Load data type mapping from: {type_mapping_fpath}")
                type_mapping_dict = self.f_handler.load_json(type_mapping_fpath)
                return type_mapping_dict

    def get_extractor_type_transformer(self):

        extractor_type_transformer_fpath = self.m_conf.get_extractor_type_transformer_fpath(self.pipeline.source_connector_id)
        type_mapping_dict = self.f_handler.load_json(extractor_type_transformer_fpath)

        if self.f_handler.is_file(self.pipeline.pipeline_extract_type_transformer_fpath):
            type_mapping_dict = self.f_handler.load_json(self.pipeline.pipeline_extract_type_transformer_fpath)
        return type_mapping_dict


