# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

from petaly.utils.file_handler import FileHandler


class TypeMapping:
    """
    Manages data type mappings between source and target connectors.
    Handles loading and applying type transformations for data extraction
    and loading operations. Supports custom type mappings at pipeline level.
    """

    def __init__(self, pipeline):
        """
        Initializes the TypeMapping instance.
        
        Logic:
        1. Store pipeline reference
        2. Initialize file handler
        3. Store main configuration reference
        """
        self.pipeline = pipeline
        self.f_handler = FileHandler()
        self.m_conf = pipeline.m_conf

    def get_type_mapping(self):
        """
        Gets target-source type mapping configuration.
        
        Logic:
        1. Try to load pipeline-specific type mapping from pipeline directory (e.g., pipeline_name/postgres.json)
        2. Fall back to default type mapping from connector if not found locally
        3. Load and return type mapping dictionary
        """
        # First, try to load from pipeline directory (local file takes precedence)
        # Only check local file if source_connector_id is set
        if self.pipeline.source_connector_id:
            pipeline_type_mapping_fpath = self.pipeline.pipeline_type_mapping_fpath.format(source_connector_id=self.pipeline.source_connector_id)
            
            logger.debug(f"Checking for local type mapping file: {pipeline_type_mapping_fpath}")
            if self.f_handler.is_file(pipeline_type_mapping_fpath):
                logger.debug(f"Load data type mapping from local pipeline file: {pipeline_type_mapping_fpath}")
                type_mapping_dict = self.f_handler.load_json(pipeline_type_mapping_fpath)
                return type_mapping_dict
            else:
                logger.debug(f"Local type mapping file not found: {pipeline_type_mapping_fpath}")
        
        # Fall back to connector's default type mapping
        connector_type_mapping_fpath = self.m_conf.compose_type_mapping_path(self.pipeline.target_connector_id, self.pipeline.source_connector_id)
        logger.debug(f"Loading from connector default: {connector_type_mapping_fpath}")
        type_mapping_dict = self.f_handler.load_json(connector_type_mapping_fpath)
        return type_mapping_dict

    def get_extractor_type_transformer(self):
        """
        Gets type transformer configuration for extractor.
        
        Logic:
        1. Load default type transformer
        2. Override with pipeline-specific transformer if exists
        """
        extractor_type_transformer_fpath = self.m_conf.get_extractor_type_transformer_fpath(self.pipeline.source_connector_id)
        type_mapping_dict = self.f_handler.load_json(extractor_type_transformer_fpath)
        if self.f_handler.is_file(self.pipeline.pipeline_extract_type_transformer_fpath):
            type_mapping_dict = self.f_handler.load_json(self.pipeline.pipeline_extract_type_transformer_fpath)
        return type_mapping_dict


