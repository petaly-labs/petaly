# Copyright © 2024-2025 Pavel Rabaev
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
from typing import Dict, List, Any, Optional

from petaly.utils.file_handler import FileHandler

class Pipeline:
    """
    Manages pipeline configuration and execution in Petaly.
    Handles loading and parsing pipeline configuration files (YAML/JSON),
    managing pipeline attributes, source and target connectors, and data objects.
    Supports both YAML and JSON formats for pipeline configuration.
    """

    def __init__(self, pipeline_name, main_config):
        """
        Initializes a new pipeline instance.
        
        Logic:
        1. Set up pipeline paths and file names
        2. Load pipeline configuration
        3. Parse pipeline attributes and settings
        4. Initialize data objects and specifications
        """
        logger.debug("Load main ConfigHandler")

        self.m_conf = main_config
        self.pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
        
        # Set pipeline file extension based on configured format
        pipeline_format = self.m_conf.global_settings.get('pipeline_format', 'yaml')
        self.pipeline_fname = f'pipeline.{pipeline_format}'
        self.pipeline_fpath = os.path.join(self.pipeline_dpath, self.pipeline_fname)

        # Check if pipeline file exists, if not try the other format
        if not os.path.exists(self.pipeline_fpath):
            alt_format = 'json' if pipeline_format == 'yaml' else 'yaml'
            alt_fname = f'pipeline.{alt_format}'
            alt_fpath = os.path.join(self.pipeline_dpath, alt_fname)
            if os.path.exists(alt_fpath):
                self.pipeline_fname = alt_fname
                self.pipeline_fpath = alt_fpath
            else:
                logger.warning(f"Pipeline file not found at {self.pipeline_fpath} or {alt_fpath}")
                return

        
        logger.info(f"Pipeline file: {self.pipeline_fpath}")
                
        self.data_dname = 'data'
        self.metadata_dname = 'metadata'
        self.object_metadata_fname = 'object_meta.json'

        self.pipeline_type_mapping_fpath = os.path.join(self.pipeline_dpath, self.m_conf.type_mapping_fname)
        self.pipeline_extract_type_transformer_fpath = os.path.join(self.pipeline_dpath, self.m_conf.extractor_type_transformer_fname)

        self.output_pipeline_dpath = os.path.join(self.m_conf.output_base_dpath, pipeline_name)
        self.output_object_data_dpath = os.path.join(self.output_pipeline_dpath, '{object_name}', self.data_dname)
        self.output_object_metadata_dpath = os.path.join(self.output_pipeline_dpath, '{object_name}', self.metadata_dname)
        self.output_object_metadata_fpath = os.path.join(self.output_object_metadata_dpath, self.object_metadata_fname)
        self.output_extract_to_stmt_fpath = os.path.join(self.output_object_metadata_dpath, self.m_conf.extract_to_stmt_fname )
        self.output_load_from_stmt_fpath = os.path.join(self.output_object_metadata_dpath, self.m_conf.load_from_stmt_fname)
        self.output_create_table_stmt_fpath = os.path.join(self.output_object_metadata_dpath, self.m_conf.create_table_stmt_fname)

        self.pipeline_name = pipeline_name

        logger.debug("Load Pipeline config")
        self.f_handler = FileHandler()

        # Initialize attributes with default values
        self.source_attr = {}
        self.target_attr = {}
        self.data_attributes = {}
        self.data_objects_spec = []
        self.data_objects = []
        self.data_objects_from_cli = []
        self.is_enabled = False
        self.source_connector_id = None
        self.target_connector_id = None
        self.data_objects_spec_mode = None
        self.object_default_settings = {}

        try:
            pipeline_all_obj = self.get_pipeline_entire_config()
            if not pipeline_all_obj:
                logger.warning(f"Could not load pipeline configuration from {self.pipeline_fpath}")
                return

            # Handle both YAML and JSON formats
            if isinstance(pipeline_all_obj, list):
                pipeline_dict = pipeline_all_obj[0]
                data_objects_spec = pipeline_all_obj[1] if len(pipeline_all_obj) > 1 else {'data_objects_spec': []}
            else:
                pipeline_dict = {'pipeline': pipeline_all_obj.get('pipeline', {})}
                data_objects_spec = {'data_objects_spec': pipeline_all_obj.get('data_objects_spec', [])}

            
            if pipeline_dict is None:
                logger.warning(f"The pipeline: {pipeline_name} does not exist under: {self.pipeline_fpath}")
                return

            pipeline_attr = pipeline_dict.get('pipeline', {}).get('pipeline_attributes', {})
            self.source_attr = pipeline_dict.get('pipeline', {}).get('source_attributes', {})
            self.target_attr = pipeline_dict.get('pipeline', {}).get('target_attributes', {})

            # Only check outdated arguments if attributes exist
            if self.source_attr:
                self.check_pipeline_outdated_arguments(self.source_attr)
            if self.target_attr:
                self.check_pipeline_outdated_arguments(self.target_attr)

            if pipeline_attr.get('pipeline_name') != pipeline_name:
                logger.warning(f"The pass parameter for pipeline_name: {pipeline_name} does not match the pipeline_name {pipeline_attr.get('pipeline_name')} defined in the corresponding file: {self.pipeline_fpath}")
                return

            # PIPELINE ATTRIBUTE
            self.is_enabled = True if str(pipeline_attr.get('is_enabled', 'false')).lower() == 'true' else False

            if not self.is_enabled:
                logger.warning(f"The pipeline: {pipeline_name} is disabled. To enable pipeline {self.pipeline_dpath} set the parameter is_enabled: true")

            self.source_connector_id = self.source_attr.get('connector_type')
            self.target_connector_id = self.target_attr.get('connector_type')

            self.data_attributes = pipeline_dict.get('pipeline', {}).get('data_attributes', {})
            self.data_objects_spec_mode = self.data_attributes.get('data_objects_spec_mode')
            self.object_default_settings = self.get_object_default_settings()

            # Set data objects spec
            self.data_objects_spec = data_objects_spec.get('data_objects_spec', [])
                  
            if self.data_objects_spec:
                for obj in self.data_objects_spec:
                    if obj is not None:
                        self.data_objects.append(obj.get('object_spec', {}).get('object_name'))

        except Exception as e:
            logger.error(f"Error initializing pipeline: {e}", exc_info=True)
            return

    def _check_outdated_arguments(self, attributes: Dict[str, Any]) -> List[str]:
        """
        Checks for outdated arguments in the given attributes dictionary.
        
        Logic:
        1. Get list of outdated arguments from configuration
        2. Check each attribute against outdated list
        3. Return list of issues found
        """
        issues = []
        pipeline_outdated_arguments = self.m_conf.get_pipeline_outdated_arguments()

        for item_name in attributes:
            item_value = pipeline_outdated_arguments.get(item_name)
            if item_value is not None:
                issues.append(f"Outdated parameter '{item_name}': {item_value.get('message')}")

        return issues

    def get_pipeline_entire_config(self):
        """
        Gets the entire pipeline configuration.
        
        Logic:
        1. Determine file format (YAML/JSON)
        2. Load configuration based on format
        3. Handle multiple documents for YAML
        4. Split JSON into pipeline and data objects
        """
        try:
            file_extension = os.path.splitext(self.pipeline_fpath)[1].lower()
            
            if file_extension == '.yaml':
                pipeline_all_obj = self.f_handler.load_yaml(self.pipeline_fpath)
            elif file_extension == '.json':
                # For JSON, we need to split the single document into two parts to match YAML structure
                json_data = self.f_handler.load_json(self.pipeline_fpath)
                pipeline_all_obj = json_data
                #pipeline_all_obj = [
                #    {'pipeline': json_data.get('pipeline', {})},
                #    {'data_objects_spec': json_data.get('data_objects_spec', [])}
                #]
                pipeline_all_obj = json_data
            else:
                logger.error(f"Unsupported pipeline file format: {file_extension}")
                return None
                
            if not pipeline_all_obj or len(pipeline_all_obj) < 1:
                logger.warning(f"Pipeline configuration is empty in {self.pipeline_fpath}")
                return None
                
            return pipeline_all_obj
        except Exception as e:
            logger.error(f"Error loading pipeline configuration from {self.pipeline_fpath}: {e}")
            return None

    def get_object_default_settings(self):
        """
        Gets default settings for data objects.
        
        Logic:
        1. Copy default settings from data attributes
        2. Check for outdated arguments
        3. Process column delimiter settings
        4. Process header settings
        5. Process column quote settings
        """

        object_default_settings = self.data_attributes.get('object_default_settings').copy()
        self.check_pipeline_outdated_arguments(object_default_settings)

        columns_delimiter = object_default_settings.get('columns_delimiter')
        object_default_settings.update({'columns_delimiter': columns_delimiter})
        if columns_delimiter == "\t":
            object_default_settings.update({'columns_delimiter': '\\t'})

        header = object_default_settings.get('header')
        if header is not True:
            header = False

        object_default_settings.update({'header': header})

        # 3. OPTIONALLY ENCLOSED BY
        columns_quote = object_default_settings.get('columns_quote')

        if columns_quote in ('double', 'double-quote'):
            columns_quote = 'double'
        elif columns_quote in ('single', 'single-quote'):
            columns_quote = 'single'
        else:
            columns_quote = 'none'

        object_default_settings.update({'columns_quote': columns_quote})

        return object_default_settings

    def check_pipeline_outdated_arguments(self, dict_to_check):
        """
        Checks for outdated parameters in the given dictionary.
        
        Logic:
        1. Get list of outdated arguments
        2. Check each parameter
        3. Log issues based on severity
        4. Exit if required by configuration
        """
        pipeline_outdated_arguments = self.m_conf.get_pipeline_outdated_arguments()

        for item_name in dict_to_check:
            item_value = pipeline_outdated_arguments.get(item_name)
            if item_value is not None:
                log_status = item_value.get('log_status').upper()
                item_message = f"The params {item_name} is outdated. {item_value.get('message')} For further information regarding this parameter, review the documentation."

                if log_status == 'CRITICAL':
                    logger.critical(item_message)
                elif log_status == 'ERROR':
                    logger.error(item_message)
                elif log_status == 'WARNING':
                    logger.warning(item_message)
                else:
                    logger.info(item_message)

                if item_value.get('action').lower() == 'exit':
                    sys.exit()

    def get_config(self) -> Dict[str, Any]:
        """
        Gets the complete pipeline configuration.
        
        Logic:
        1. Load entire pipeline configuration
        2. Return pipeline and data objects spec
        """
        try:
            pipeline_all_obj = self.get_pipeline_entire_config()
            if not pipeline_all_obj or len(pipeline_all_obj) < 2:
                return None
                
            return {
                "pipeline": pipeline_all_obj[0],
                "data_objects_spec": pipeline_all_obj[1]
            }
        except Exception as e:
            logger.error(f"Error getting pipeline config: {e}", exc_info=True)
            return None
