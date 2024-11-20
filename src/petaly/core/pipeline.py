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

from petaly.utils.file_handler import FileHandler


class Pipeline:
    def __init__(self, pipeline_name, main_config):
        logger.info("Load main ConfigHandler")
        self.m_conf = main_config
        self.pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
        self.pipeline_fpath = os.path.join(self.pipeline_dpath, main_config.pipeline_fname)

        self.data_dname = 'data'
        self.metadata_dname = 'metadata'
        self.object_metadata_fname = 'object_meta.json'

        self.pipeline_type_mapping_fpath = os.path.join(self.pipeline_dpath, self.m_conf.type_mapping_fname)
        self.pipeline_extract_type_transformer_fpath = os.path.join(self.pipeline_dpath,self.m_conf.extractor_type_transformer_fname)

        self.output_pipeline_dpath = os.path.join(self.m_conf.output_base_dpath, pipeline_name)
        self.output_object_data_dpath = os.path.join(self.output_pipeline_dpath, '{object_name}', self.data_dname)
        self.output_object_metadata_dpath = os.path.join(self.output_pipeline_dpath, '{object_name}', self.metadata_dname)
        self.output_object_metadata_fpath = os.path.join(self.output_object_metadata_dpath, self.object_metadata_fname)
        self.output_extract_to_stmt_fpath = os.path.join(self.output_object_metadata_dpath, self.m_conf.extract_to_stmt_fname )
        self.output_load_from_stmt_fpath = os.path.join(self.output_object_metadata_dpath, self.m_conf.load_from_stmt_fname)
        self.output_create_table_stmt_fpath = os.path.join(self.output_object_metadata_dpath, self.m_conf.create_table_stmt_fname)

        self.pipeline_name = pipeline_name

        logger.info("Load Pipeline config")
        self.f_handler = FileHandler()

        pipeline_all_obj = self.get_pipeline_entire_config()
        pipeline_dict = pipeline_all_obj[0]

        if pipeline_dict == None:
            logger.warning(f"The pipeline: {pipeline_name} does not exists under: {self.pipeline_fpath}")
            sys.exit()

        pipeline_attr = pipeline_dict.get('pipeline').get('pipeline_attributes')
        self.source_attr = pipeline_dict.get('pipeline').get('source_attributes')
        self.target_attr = pipeline_dict.get('pipeline').get('target_attributes')

        if pipeline_attr.get('pipeline_name') != pipeline_name:
            logger.warning(f"The pass parameter for pipeline_name: {pipeline_name} does not match the pipeline_name {pipeline_attr.get('pipeline_name')} defined in the corresponding file:  {self.pipeline_fpath}")
            sys.exit()

        # PIPELINE ATTRIBUTE
        self.is_enabled = pipeline_attr.get('is_enabled')

        if self.is_enabled != True:
            logger.warning(f"The pipeline: {pipeline_name} is disabled. To enable pipeline {self.pipeline_dpath} set the parameter is_enabled: true ")
            #sys.exit()

        self.source_connector_id = self.source_attr.get('endpoint_type')
        self.target_connector_id = self.target_attr.get('endpoint_type')

        self.data_object_main_config = pipeline_dict.get('pipeline').get('data_object_main_config')
        #self.incremental_batch_size = pipeline_attr.get('incremental_batch_size')
        #self.preferred_load_type = self.data_object_main_config.get('preferred_load_type')
        #self.data_transition_format = self.data_object_main_config.get('data_transition_format')
        #self.use_data_objects_spec = self.data_object_main_config.get('use_data_objects_spec')

        self.data_objects_spec = pipeline_all_obj[1]
        if self.data_objects_spec is None:
            logger.warning(
                f"The pipeline: {pipeline_name} wasn't specify properly. It is missing following definition\n\n"
                f"---\n"
                f"data_objects_spec: []\n\n"
                f"It is recommended to reconfigure the pipeline using\n"
                f"python -m petaly init pipeline -p {pipeline_name}")
            sys.exit()

        self.data_objects = []
        if len(self.data_objects_spec) > 0:
            for obj in self.data_objects_spec.get('data_objects_spec'):
                if obj is not None:
                    self.data_objects.append(obj.get('object_name'))

    def get_pipeline_entire_config(self):
        pipeline_all_obj = self.f_handler.load_yaml_all(self.pipeline_fpath)
        return pipeline_all_obj

    def get_data_objects(self):
        data_objects = []
        if self.data_objects is not None:
            if self.data_objects[0] is not None:
                data_objects = self.data_objects

        return data_objects