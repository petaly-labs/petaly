# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

import os
import sys
from typing import Dict, List, Any, Optional

from petaly.utils.file_handler import FileHandler
from petaly.core.connections import Connections

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
        pipeline_format = self.m_conf.global_settings.get('pipeline_file_format', 'yaml')
        self.pipeline_fname = f'pipeline.{pipeline_format}'
        self.pipeline_fpath = os.path.join(self.pipeline_dpath, self.pipeline_fname)
        
        # Set connections file path
        # Priority: 1) connections_file_path from config, 2) default: pipeline_dir_path/connections.yaml
        if hasattr(self.m_conf, 'connections_file_path') and self.m_conf.connections_file_path:
            # Use explicitly specified connections file path
            self.connections_fpath = self.m_conf.connections_file_path
        else:
            # Default: use pipeline_dir_path/connections.yaml
            # Use connections_file_format from config, fallback to pipeline_file_format for backward compatibility
            connection_format = self.m_conf.global_settings.get('connections_file_format', pipeline_format)
            self.connections_fname = f'connections.{connection_format}'
            self.connections_fpath = os.path.join(self.m_conf.pipeline_base_dpath, self.connections_fname)
        
        # Initialize connections handler
        self.connections = Connections(self.connections_fpath)
        
        # Initialize file handler early (needed for creating pipeline from skeleton)
        self.f_handler = FileHandler()

        # Check if pipeline file exists, if not try the other format or create it
        if not os.path.exists(self.pipeline_fpath):
            alt_format = 'json' if pipeline_format == 'yaml' else 'yaml'
            alt_fname = f'pipeline.{alt_format}'
            alt_fpath = os.path.join(self.pipeline_dpath, alt_fname)
            if os.path.exists(alt_fpath):
                self.pipeline_fname = alt_fname
                self.pipeline_fpath = alt_fpath
            else:
                # Pipeline file doesn't exist - create it from skeleton
                logger.info(f"Pipeline file not found at {self.pipeline_fpath}. Creating from skeleton...")
                self._create_pipeline_from_skeleton(pipeline_name, pipeline_format)

        
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

        # Initialize attributes with default values
        self.source_attr = {}
        self.target_attr = {}
        self.load_attributes = {}
        self.data_objects_spec = []
        self.data_objects = []
        self.data_objects_from_cli = []
        self.exclude_objects = []
        self.source_connector_id = None
        self.target_connector_id = None
        self.use_data_objects_spec = None
        self.csv_default_settings = {}

        try:
            pipeline_all_obj = self.get_pipeline_entire_config()
            if not pipeline_all_obj:
                logger.warning(f"Could not load pipeline configuration from {self.pipeline_fpath}")
                return

            # Handle both YAML and JSON formats (now both single document)
            if isinstance(pipeline_all_obj, list):
                # Legacy multi-document format support (backward compatibility)
                pipeline_dict = pipeline_all_obj[0]
                second_doc = pipeline_all_obj[1] if len(pipeline_all_obj) > 1 else {}
                if isinstance(second_doc, dict):
                    data_objects_spec_array = second_doc.get('data_objects_spec', [])
                elif isinstance(second_doc, list):
                    data_objects_spec_array = second_doc
                else:
                    data_objects_spec_array = []
            else:
                # Single document format (current format)
                pipeline_dict = {'pipeline': pipeline_all_obj.get('pipeline', {})}
                data_objects_spec_array = pipeline_all_obj.get('data_objects_spec', [])
            
            # Normalize to internal structure
            data_objects_spec = {
                'data_objects_spec': data_objects_spec_array if isinstance(data_objects_spec_array, list) else []
            }

            
            if pipeline_dict is None:
                logger.warning(f"The pipeline: {pipeline_name} does not exist under: {self.pipeline_fpath}")
                return

            # Get pipeline_name directly from pipeline (no longer nested in pipeline_attributes)
            pipeline_dict_data = pipeline_dict.get('pipeline', {})
            pipeline_name_from_config = pipeline_dict_data.get('pipeline_name')
            
            # Get source and target attributes sections
            source_attributes_section = pipeline_dict_data.get('source_attributes', {})
            target_attributes_section = pipeline_dict_data.get('target_attributes', {})
            
            # Resolve source attributes (may contain connection reference)
            self.source_attr = self.connections.resolve_attributes(source_attributes_section, 'source')
            if not self.source_attr:
                logger.error(f"Could not resolve source attributes")
                return
            
            # Resolve target attributes (may contain connection reference)
            self.target_attr = self.connections.resolve_attributes(target_attributes_section, 'target')
            if not self.target_attr:
                logger.error(f"Could not resolve target attributes")
                return

            # Only check outdated arguments if attributes exist
            if self.source_attr:
                self.check_pipeline_outdated_arguments(self.source_attr)
            if self.target_attr:
                self.check_pipeline_outdated_arguments(self.target_attr)

            # Backward compatibility: check pipeline_attributes if pipeline_name not found directly
            if not pipeline_name_from_config:
                pipeline_attr = pipeline_dict_data.get('pipeline_attributes', {})
                pipeline_name_from_config = pipeline_attr.get('pipeline_name')
                if pipeline_name_from_config:
                    logger.warning(f"Pipeline uses deprecated 'pipeline_attributes' section. Please move 'pipeline_name' directly under 'pipeline'.")

            if pipeline_name_from_config and pipeline_name_from_config != pipeline_name:
                logger.warning(f"The pass parameter for pipeline_name: {pipeline_name} does not match the pipeline_name {pipeline_name_from_config} defined in the corresponding file: {self.pipeline_fpath}")
                return

            # PIPELINE ATTRIBUTE
            self.source_connector_id = self.source_attr.get('connector_type')
            self.target_connector_id = self.target_attr.get('connector_type')

            self.load_attributes = pipeline_dict.get('pipeline', {}).get('load_attributes', {})
            
            # Incremental load parameters are now per-object in data_objects_spec
            # Default values for when not specified in object_spec
            self.load_mode = 'full'
            self.column_primary_key = ''
            self.column_last_modified = ''
            self.batch_size = None
            
            # Get use_data_objects_spec, with backward compatibility for old parameter names
            self.use_data_objects_spec = self.load_attributes.get('use_data_objects_spec', 'prefer')
            
            # Backward compatibility: check for old parameter names
            if self.use_data_objects_spec is None or self.use_data_objects_spec not in ('prefer', 'strict'):
                # Check for old include_data_objects parameter
                old_include_data_objects = self.load_attributes.get('include_data_objects')
                if old_include_data_objects is not None:
                    # Convert old include_data_objects to new use_data_objects_spec
                    if isinstance(old_include_data_objects, str):
                        old_include_data_objects = old_include_data_objects.lower()
                    # Map: 'all' -> 'prefer', 'spec' -> 'strict'
                    if old_include_data_objects == 'all':
                        self.use_data_objects_spec = 'prefer'
                    elif old_include_data_objects == 'spec':
                        self.use_data_objects_spec = 'strict'
                    else:
                        self.use_data_objects_spec = 'prefer'
                else:
                    # Check for old load_data_objects parameter
                    old_load_data_objects = self.load_attributes.get('load_data_objects')
                    if old_load_data_objects is not None:
                        if isinstance(old_load_data_objects, str):
                            old_load_data_objects = old_load_data_objects.lower()
                        # Map: 'all' -> 'prefer', 'spec' -> 'strict'
                        if old_load_data_objects == 'all':
                            self.use_data_objects_spec = 'prefer'
                        elif old_load_data_objects == 'spec':
                            self.use_data_objects_spec = 'strict'
                        else:
                            self.use_data_objects_spec = 'prefer'
                    else:
                        # Check for old load_all_from_schema parameter (boolean)
                        old_load_all = self.load_attributes.get('load_all_from_schema')
                        if old_load_all is not None:
                            if isinstance(old_load_all, str):
                                old_load_all = old_load_all.lower() == 'true'
                            self.use_data_objects_spec = 'prefer' if old_load_all else 'strict'
                        else:
                            # Check for old load_data_objects_spec_only parameter
                            old_load_spec_only = self.load_attributes.get('load_data_objects_spec_only')
                            if old_load_spec_only is not None:
                                if isinstance(old_load_spec_only, str):
                                    old_load_spec_only = old_load_spec_only.lower() == 'true'
                                self.use_data_objects_spec = 'strict' if old_load_spec_only else 'prefer'
                            else:
                                # Check for even older apply_data_objects_spec parameter
                                old_apply_spec = self.load_attributes.get('apply_data_objects_spec')
                                if old_apply_spec is not None:
                                    if isinstance(old_apply_spec, str):
                                        old_apply_spec = old_apply_spec.lower() == 'true'
                                    self.use_data_objects_spec = 'prefer' if old_apply_spec else 'strict'
            
            # Normalize to lowercase string
            if isinstance(self.use_data_objects_spec, str):
                self.use_data_objects_spec = self.use_data_objects_spec.lower()
                if self.use_data_objects_spec not in ('prefer', 'strict'):
                    logger.warning(f"Invalid value for use_data_objects_spec: {self.use_data_objects_spec}. Expected 'prefer' or 'strict'. Defaulting to 'prefer'.")
                    self.use_data_objects_spec = 'prefer'
            
            self.csv_default_settings = self.get_csv_default_settings()

            # Set data objects spec
            self.data_objects_spec = data_objects_spec.get('data_objects_spec', [])
                  
            if self.data_objects_spec:
                for obj in self.data_objects_spec:
                    if obj is not None:
                        self.data_objects.append(obj.get('object_spec', {}).get('object_name'))
            
            # Set exclude_objects array from load_attributes
            self.exclude_objects = self.load_attributes.get('exclude_objects', [])
            if not isinstance(self.exclude_objects, list):
                logger.warning(f"exclude_objects should be an array, got {type(self.exclude_objects)}. Converting to list.")
                self.exclude_objects = []
            
            # Remove excluded objects from data_objects list (applies regardless of use_data_objects_spec)
            # Even if objects are explicitly specified in data_objects_spec, they will be excluded
            if len(self.exclude_objects) > 0:
                original_count = len(self.data_objects)
                self.data_objects = [obj for obj in self.data_objects if obj not in self.exclude_objects]
                if len(self.data_objects) < original_count:
                    logger.info(f"Excluded {original_count - len(self.data_objects)} object(s) from data_objects based on exclude_objects: {self.exclude_objects}")
            
            # Warning: if use_data_objects_spec=strict and data_objects_spec[] is empty, no objects will be loaded
            if self.use_data_objects_spec == 'strict' and len(self.data_objects) == 0:
                logger.warning(
                    f"Pipeline {pipeline_name}: use_data_objects_spec='strict' and data_objects_spec[] is empty. "
                    f"No objects will be loaded. Either set use_data_objects_spec='prefer' to load all objects from schema, "
                    f"or add objects to data_objects_spec[] to load specific objects."
                )

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
                # Load YAML - try single document first (current format), then multi-document (legacy)
                try:
                    # Try single document format first (current format)
                    pipeline_all_obj = self.f_handler.load_yaml(self.pipeline_fpath)
                    # If it's a dict with 'pipeline', it's single document format
                    if isinstance(pipeline_all_obj, dict) and 'pipeline' in pipeline_all_obj:
                        # Single document format - return as-is
                        pass
                    elif isinstance(pipeline_all_obj, list):
                        # Already multi-document format (legacy) - return as-is
                        pass
                    else:
                        # Fallback: wrap in list for legacy compatibility
                        pipeline_all_obj = [pipeline_all_obj] if pipeline_all_obj else []
                except Exception:
                    # Fall back to multi-document loading for legacy files
                    try:
                        pipeline_all_obj = self.f_handler.load_yaml_all(self.pipeline_fpath)
                    except Exception:
                        pipeline_all_obj = None
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

    def get_csv_default_settings(self):
        """
        Gets default settings for data objects.
        
        Logic:
        1. Copy default settings from data attributes
        2. Check for outdated arguments
        3. Process column delimiter settings
        4. Process header settings
        5. Process column quote settings
        """

        # Backward compatibility: check for old 'object_default_settings' name
        csv_default_settings = self.load_attributes.get('csv_default_settings') or self.load_attributes.get('object_default_settings')
        if csv_default_settings is None:
            logger.error("Missing 'csv_default_settings' in load_attributes")
            return {}
        csv_default_settings = csv_default_settings.copy()
        
        # Warn if old name was used
        if 'object_default_settings' in self.load_attributes and 'csv_default_settings' not in self.load_attributes:
            logger.warning("Pipeline uses deprecated 'object_default_settings'. Please rename to 'csv_default_settings'.")
        
        self.check_pipeline_outdated_arguments(csv_default_settings)

        columns_delimiter = csv_default_settings.get('columns_delimiter')
        csv_default_settings.update({'columns_delimiter': columns_delimiter})
        
        # Remove the problematic conversion that was causing PyArrow to fail
        # The delimiter should remain as a single character for PyArrow compatibility
        #if columns_delimiter == "\t":

        #    csv_default_settings.update({'columns_delimiter': '\\t'})

        header = csv_default_settings.get('header')
        if header is not True:
            header = False

        csv_default_settings.update({'header': header})

        # 3. OPTIONALLY ENCLOSED BY
        columns_quote = csv_default_settings.get('columns_quote')

        if columns_quote in ('double', 'double-quote'):
            columns_quote = 'double'
        elif columns_quote in ('single', 'single-quote'):
            columns_quote = 'single'
        else:
            columns_quote = 'none'

        csv_default_settings.update({'columns_quote': columns_quote})

        # 4. TYPE AUTODETECTION (for CSV/TSV/TXT sources)
        type_autodetection = csv_default_settings.get('type_autodetection', True)
        # Convert string to boolean if needed (for backward compatibility)
        if isinstance(type_autodetection, str):
            type_autodetection = type_autodetection.lower() in ('true', '1', 'yes', 'on')
        elif type_autodetection is None:
            type_autodetection = True  # Default to true if not specified
        csv_default_settings.update({'type_autodetection': bool(type_autodetection)})

        # 5. NULL STRING (string representation of NULL values in CSV files)
        null_string = csv_default_settings.get('null_string', '')
        if null_string is None:
            null_string = ''
        csv_default_settings.update({'null_string': str(null_string)})

        # 6. FORCE NULL (force zero-length strings to be treated as NULL)
        force_null = csv_default_settings.get('force_null', False)
        # Convert string to boolean if needed (for backward compatibility)
        if isinstance(force_null, str):
            force_null = force_null.lower() in ('true', '1', 'yes', 'on')
        elif force_null is None:
            force_null = False  # Default to false if not specified
        csv_default_settings.update({'force_null': bool(force_null)})

        return csv_default_settings

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
    
    def get_consolidated_config(self) -> Dict[str, Any]:
        """
        Gets the consolidated pipeline configuration with resolved connections.
        This combines pipeline.yaml and connections.yaml into a single configuration
        with all connection references resolved to their full attributes.
        
        Logic:
        1. Load the original pipeline configuration
        2. Resolve connection references to full attributes
        3. Return consolidated configuration ready for execution logging
        
        Returns:
            Dictionary containing consolidated pipeline configuration with resolved connections
        """
        try:
            # Get the original pipeline configuration
            pipeline_all_obj = self.get_pipeline_entire_config()
            if not pipeline_all_obj:
                return None
            
            # Handle both YAML and JSON formats (now both single document)
            if isinstance(pipeline_all_obj, list):
                # Legacy multi-document format support (backward compatibility)
                pipeline_dict = pipeline_all_obj[0]
                second_doc = pipeline_all_obj[1] if len(pipeline_all_obj) > 1 else {}
                if isinstance(second_doc, dict):
                    data_objects_spec_array = second_doc.get('data_objects_spec', [])
                elif isinstance(second_doc, list):
                    data_objects_spec_array = second_doc
                else:
                    data_objects_spec_array = []
            else:
                # Single document format (current format)
                pipeline_dict = {'pipeline': pipeline_all_obj.get('pipeline', {})}
                data_objects_spec_array = pipeline_all_obj.get('data_objects_spec', [])
            
            # Normalize to internal structure
            data_objects_spec = {
                'data_objects_spec': data_objects_spec_array if isinstance(data_objects_spec_array, list) else []
            }
            
            # Create consolidated config
            consolidated_pipeline = pipeline_dict.get('pipeline', {}).copy()
            
            # Replace connection references with resolved attributes
            source_attributes_section = consolidated_pipeline.get('source_attributes', {})
            target_attributes_section = consolidated_pipeline.get('target_attributes', {})
            
            # Resolve source and target attributes (may contain connection references)
            consolidated_pipeline['source_attributes'] = self.connections.resolve_attributes(source_attributes_section, 'source')
            consolidated_pipeline['target_attributes'] = self.connections.resolve_attributes(target_attributes_section, 'target')
            
            return {
                "pipeline": consolidated_pipeline,
                "data_objects_spec": data_objects_spec.get('data_objects_spec', [])
            }
        except Exception as e:
            logger.error(f"Error creating consolidated pipeline config: {e}", exc_info=True)
            return None
    
    def _create_pipeline_from_skeleton(self, pipeline_name, pipeline_format):
        """
        Creates a pipeline.yaml/json file from skeleton if it doesn't exist.
        
        Args:
            pipeline_name: Name of the pipeline
            pipeline_format: Format of the pipeline file ('yaml' or 'json')
        """
        try:
            # Ensure pipeline directory exists
            if not os.path.exists(self.pipeline_dpath):
                os.makedirs(self.pipeline_dpath)
                logger.info(f"Created pipeline directory: {self.pipeline_dpath}")
            
            # Load skeleton
            skeleton_fpath = self.m_conf.pipeline_skeleton_fpath
            skeleton_config = self.f_handler.load_json(skeleton_fpath)
            
            # Set pipeline name in the skeleton
            if 'pipeline' in skeleton_config:
                skeleton_config['pipeline']['pipeline_name'] = pipeline_name
            
            # Save to pipeline file
            self.f_handler.save_dict_to_file(
                self.pipeline_fpath,
                skeleton_config,
                file_format=pipeline_format
            )
            
            logger.info(f"Created pipeline file from skeleton: {self.pipeline_fpath}")
        except Exception as e:
            logger.error(f"Error creating pipeline from skeleton: {e}", exc_info=True)
            raise
