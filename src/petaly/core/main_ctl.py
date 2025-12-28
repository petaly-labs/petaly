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
import json
from petaly.utils.utils import sanitize_sensitive_data

logger = logging.getLogger(__name__)


class MainCtl():
    """Main controller class for pipeline execution.
    
    This class orchestrates the execution of data pipelines, managing both source
    extraction and target loading operations.
    
    Key responsibilities:
    - Initializes and manages pipeline configuration
    - Coordinates source data extraction
    - Coordinates target data loading
    - Handles pipeline execution flow
    
    Attributes:
        main_config: Main configuration instance containing pipeline settings
    """

    def __init__(self, main_config):
        self.m_conf = main_config
        # Logging is already set up in MainConfig.set_workspace_dpaths()
        # No need to set it up again here

    def run_pipeline(self, pipeline, run_endpoint, object_name_list):
        """ Call this function to run pipeline source and target
        """
        pipeline_name = pipeline.pipeline_name
        
        if pipeline.is_enabled is True:
            logger.info(f"[Start] Pipeline {pipeline_name}")

            # Log consolidated pipeline configuration (debug mode)
            self.save_execution_config(pipeline)

            if object_name_list is not None:
                pipeline.data_objects_from_cli = object_name_list.split(',')

            if run_endpoint is None or run_endpoint == 'source':
                self.run_source(pipeline)

            if run_endpoint is None or run_endpoint == 'target':
                self.run_target(pipeline)

            logger.info(f"[End] Pipeline {pipeline_name}")
        else:
            logger.info(f"The pipeline {pipeline_name} is disabled. Check the parameter is_enabled in pipeline.yaml file")
    
    def save_execution_config(self, pipeline):
        """
        Logs the consolidated pipeline configuration (pipeline.yaml + connections.yaml)
        as a debug message in the log file for execution tracking.
        
        Sensitive fields (passwords, secrets, keys) are masked before logging.
        
        Logic:
        1. Get consolidated configuration with resolved connections
        2. Sanitize sensitive fields (passwords, secrets, keys)
        3. Log as formatted JSON in debug mode
        """
        try:
            consolidated_config = pipeline.get_consolidated_config()
            if not consolidated_config:
                logger.warning(f"Could not create consolidated config for pipeline {pipeline.pipeline_name}")
                return
            
            # Sanitize sensitive fields before logging
            sanitized_config = sanitize_sensitive_data(consolidated_config)
            
            # Format consolidated configuration as JSON for logging
            config_json = json.dumps(sanitized_config, indent=2, default=str)
            
            # Log consolidated configuration as debug message
            logger.debug(f"Consolidated pipeline configuration for {pipeline.pipeline_name}:\n{config_json}")
        except Exception as e:
            logger.error(f"Error logging execution configuration: {e}", exc_info=True)

    ####################### run source ####################################

    def run_source(self, pipe):
        """Runs the source part of a pipeline.
        
        This method:
        1. Loads source configuration
        2. Gets the appropriate extractor class
        3. Initializes and runs the extractor
        
        If the extractor cannot be initialized, it raises a SystemExit error.
        """
        logger.debug("Load source config")
        
        if not pipe.source_connector_id:
            logger.error(f"Source connector type is not specified. Check source_attributes in pipeline configuration.")
            raise ValueError(f"Source connector type is missing. Ensure connector_type is specified in source_attributes or that the connection reference resolves correctly.")

        class_obj = self.m_conf.get_extractor_class(pipe.source_connector_id)
        # run extraction
        if class_obj is not None:
            extractor = class_obj(pipe)
            logger.debug(f"Extract connector-id: {pipe.source_connector_id}")
            extractor.extract_data()
        else:
            logger.error(f"Extractor with connector-id: {pipe.source_connector_id} can't initialized.")

    ####################### run targets ####################################

    def run_target(self, pipe):
        """Runs the target part of a pipeline.
        
        This method:
        1. Loads target configuration
        2. Gets the appropriate loader class
        3. Initializes and runs the loader
        
        If the loader cannot be initialized, it raises a SystemExit error.
        """
        logger.debug("Load target config")
        
        if not pipe.target_connector_id:
            logger.error(f"Target connector type is not specified. Check target_attributes in pipeline configuration.")
            raise ValueError(f"Target connector type is missing. Ensure connector_type is specified in target_attributes or that the connection reference resolves correctly.")
        
        class_obj = self.m_conf.get_loader_class(pipe.target_connector_id)
        logger.debug(f"Load class: {class_obj}")
        # run loader
        if class_obj is not None:
            loader = class_obj(pipe)
            logger.debug(f"Load target connector id: {pipe.target_connector_id}")
            loader.load_data()

        else:
            logger.error(f"Loader with connector-id: {pipe.target_connector_id} can't initialized.")