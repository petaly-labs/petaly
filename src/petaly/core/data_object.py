# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import sys
import logging
logger = logging.getLogger(__name__)

class DataObject:
    """
    Represents a data object in a Petaly pipeline.
    Manages data object specifications, settings, and metadata.
    Handles both source and destination object configurations,
    including column mappings, file settings, and object properties.
    """

    def __init__(self, pipeline, object_name):
        """
        Initializes a new data object instance.
        
        Logic:
        1. Get data object specifications from pipeline
        2. Set up object paths and settings
        3. Load object-specific settings
        4. Handle different data object spec modes
        """
        data_objects = pipeline.data_objects_spec
        self.pipeline_data_object_dir = pipeline.output_object_data_dpath.format(object_name=object_name)
        self.use_data_objects_spec = pipeline.use_data_objects_spec
        self.object_settings = self.format_csv_default_settings(pipeline.csv_default_settings)
        self.object_settings.update({'cleanup_linebreak_in_fields': False})

        data_object_spec = self.get_object_spec(data_objects, object_name)
        logger.debug(f"Data object spec: {data_object_spec}")
        if not data_object_spec:
            # If use_data_objects_spec is 'strict' (only mode), spec is required
            if self.use_data_objects_spec == 'strict':
                logger.info(
                    f"For {pipeline.source_connector_id} extract the parameters use_data_objects_spec='strict' and specification in the data_objects_spec[] are required. Use python -m petaly init -p {pipeline.pipeline_name} --object_name table1,table2 -c your_config_dir/petaly.ini")
                sys.exit()

            # If use_data_objects_spec is 'prefer' (load all) or spec is empty, check file connector requirement
            if pipeline.source_connector_id in ('csv', 'parquet', 'json'):
                logger.info(
                    f"In case your source is a file connector ({pipeline.source_connector_id}), the parameters use_data_objects_spec should be set to 'strict' and require the specification in the data_objects_spec[]."
                    f"\nuse_data_objects_spec='strict'"
                    f"\nCheck pipeline under: {pipeline.pipeline_fpath}")

                sys.exit()

            return self.set_default_object_spec(pipeline, object_name)

        self.object_name = object_name
        self.destination_object_name = data_object_spec.get('destination_object_name')
        self.recreate_destination_object = True if data_object_spec.get('recreate_destination_object') is True else False
        self.exclude_columns = data_object_spec.get('exclude_columns')
        self.object_source_dir = data_object_spec.get('object_source_dir')
        self.file_names = data_object_spec.get('file_names')
        cleanup_linebreak_in_fields = data_object_spec.get('cleanup_linebreak_in_fields')
        self.object_settings.update({'cleanup_linebreak_in_fields': cleanup_linebreak_in_fields})
        
        # Incremental load parameters (per-object)
        # Note: Incremental load is only supported for MySQL and PostgreSQL sources
        self.load_mode = data_object_spec.get('load_mode', 'full')
        self.column_primary_key = data_object_spec.get('column_primary_key', '')
        self.column_last_modified = data_object_spec.get('column_last_modified', '')
        batch_size = data_object_spec.get('batch_size')
        self.batch_size = int(batch_size) if batch_size is not None and batch_size != '' else None
        
        # Validate incremental load is only used with supported sources
        if self.load_mode == 'incremental':
            if pipeline.source_connector_id not in ('mysql', 'postgres'):
                logger.error(
                    f"Incremental load mode is only supported for MySQL and PostgreSQL sources. "
                    f"Current source connector: {pipeline.source_connector_id}. "
                    f"Please set load_mode to 'full' or use MySQL/PostgreSQL as source."
                )
                sys.exit()

    def to_dict(self) -> dict:
        """
        Converts the data object to a dictionary.
        
        Logic:
        1. Convert all object attributes to dictionary format
        """
        return {key: value for key, value in self.__dict__.items()}

    def get_object_spec(self, data_objects, object_name):
        """
        Gets the specification for a specific data object.
    
        Logic:
        1. Search through data objects list
        2. Find matching object by name
        3. Return object specification
        """
        return_object_spec = {}
        for object_spec in data_objects:
            if object_spec is not None:
                if object_spec.get('object_spec', {}).get('object_name') == object_name:
                    return_object_spec = object_spec.get('object_spec', {})
                    break
        return return_object_spec

    def set_default_object_spec(self, pipeline, object_name):
        """
        Sets default specifications for a data object.
        
        Logic:
        1. Set basic object properties
        2. Initialize default values for all settings
        """
        self.object_name = object_name
        self.destination_object_name = None
        self.recreate_destination_object = False
        self.cleanup_linebreak_in_fields = False
        self.exclude_columns = [None]
        self.object_source_dir = None
        self.file_names = [None]
        
        # Incremental load parameters (defaults for objects without spec)
        self.load_mode = 'full'
        self.column_primary_key = ''
        self.column_last_modified = ''
        self.batch_size = None

    def format_csv_default_settings(self, csv_default_settings):
        """
        Formats default settings for a data object.
        
        Logic:
        1. Copy default settings
        2. Process header settings
        3. Process column delimiter settings
        """
        object_settings = csv_default_settings.copy()
        # 1. Header

        header = True if object_settings.get('header') is True else False
        object_settings.update({'header': header})

        # 2. Columns delimiter
        columns_delimiter = object_settings.get('columns_delimiter')
        #object_settings.update({'columns_delimiter_origin': columns_delimiter})
        if columns_delimiter == "\\t":
            object_settings.update({'columns_delimiter': '\t'})

        return object_settings
