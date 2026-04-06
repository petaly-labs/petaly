# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)

import os
import time
from abc import ABC, abstractmethod

from petaly.core.composer import Composer
from petaly.utils.file_handler import FileHandler

from petaly.core.data_object import DataObject

class FLoader(ABC):
    """Abstract base class for file loaders.
    
    This class provides the core functionality for loading data into file-based targets.
    It handles file writing, compression, and metadata management.
    
    Key responsibilities:
    - Loads data into various file formats
    - Manages file compression and decompression
    - Handles file output and metadata storage
    - Processes multiple files in a directory
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.composer = Composer()
        self.f_handler = FileHandler()
        pass

    @abstractmethod
    def load_from(self, loader_obj_conf):
        """Abstract method to be implemented by concrete loaders.
        
        This method should implement the specific logic for loading data
        into the target file format.
        """

    def load_data(self, file_to_gzip=False):
        """Loads data into file-based targets.
        
        This method orchestrates the entire loading process:
        1. Gets list of objects to load
        2. For each object:
           - Composes loader configuration
           - Optionally compresses files
           - Loads data to files
        
        The method handles timing and logging of the loading process.
        """

        logger.info(f"[--- Load into {self.pipeline.target_connector_id} ---]")
        start_total_time = time.time()

        if not self.pipeline.all_from_schema:
            object_list = self.pipeline.data_objects
        else:
            #object_list = self.f_handler.get_all_dir_names(self.pipeline.output_pipeline_dpath)
            object_list = self.composer.get_object_list_from_output_dir(self.pipeline)

        for object_name in object_list:
            self.load_per_object(object_name, file_to_gzip=file_to_gzip)

        end_total_time = time.time()
        logger.info(f"Load completed, duration: {round(end_total_time - start_total_time, 2)}s")

    def load_per_object(self, object_name, load_summary=None, file_to_gzip=False):
        """Load a single object into the file target.
        
        Args:
            object_name: Name of the object to load
            load_summary: Optional LoadSummary instance to track loading progress
            file_to_gzip: Whether to gzip files before loading
        """
        logger.info(f"Load object: {object_name} started...")
        start_time = time.time()
        load_status = 'success'
        rows_loaded = None

        try:
            loader_obj_conf = {}
            loader_obj_conf.update({'object_name': object_name})
            output_metadata_object_dir = self.pipeline.output_object_metadata_dpath.format(object_name=object_name)
            loader_obj_conf.update({'output_metadata_object_dir': output_metadata_object_dir})

            output_data_object_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
            loader_obj_conf.update({'output_data_object_dir': output_data_object_dir})

            output_load_from_stmt_fpath = self.pipeline.output_load_from_stmt_fpath.format(object_name=object_name)
            loader_obj_conf.update({'load_from_stmt_fpath': output_load_from_stmt_fpath})

            if file_to_gzip:
                self.f_handler.gzip_csv_files(output_data_object_dir, cleanup_file=True)

            file_list = self.f_handler.get_specific_files(output_data_object_dir, '*.*')
            loader_obj_conf.update({'file_list': file_list})
            
            # Try to count rows from CSV files for summary
            try:
                import os
                total_rows = 0
                for file_path in file_list:
                    if file_path.endswith('.csv') or file_path.endswith('.csv.gz'):
                        # For gzipped files, we can't easily count rows without decompressing
                        # For now, we'll skip row counting for file loaders
                        # This could be enhanced in the future
                        pass
                # For file loaders, rows_loaded will remain None
            except Exception:
                pass  # Row counting is optional for file loaders

            blob_prefix = self.composer.compose_bucket_object_path(self.pipeline.target_attr.get('bucket_pipeline_prefix'),
                                                                    self.pipeline.pipeline_name,
                                                                    object_name)
            loader_obj_conf.update({'blob_prefix': blob_prefix})

            self.load_from(loader_obj_conf)
            
        except Exception as e:
            load_status = 'failed'
            logger.error(f"Load object: {object_name} failed: {e}", exc_info=True)
            raise
        finally:
            end_time = time.time()
            duration_sec = round(end_time - start_time, 2)
            
            # Add to summary if provided
            if load_summary is not None:
                # Get source and target connection names
                source_connection_name = self.pipeline.source_attr.get('connection_name') if self.pipeline.source_attr else None
                if not source_connection_name:
                    source_connection_name = self.pipeline.source_connector_id if self.pipeline.source_connector_id else 'inline'
                
                target_connection_name = self.pipeline.target_attr.get('connection_name') if self.pipeline.target_attr else None
                if not target_connection_name:
                    target_connection_name = self.pipeline.target_connector_id if self.pipeline.target_connector_id else 'inline'
                
                # Source object name is the object_name (extracted from source)
                source_object_name = object_name
                
                # Target object name is the blob prefix or object name for file targets
                target_object_name = blob_prefix if 'blob_prefix' in locals() else object_name
                
                # Get recreate_destination_object from data object configuration
                try:
                    data_object = self.get_data_object(object_name)
                    destination_object_recreated = bool(data_object.recreate_destination_object)
                except Exception:
                    # If we can't get the data object, default to False
                    destination_object_recreated = False
                
                load_summary.add_entry(
                    source_connection=source_connection_name,
                    source_object=source_object_name,
                    target_connection=target_connection_name,
                    target_object=target_object_name,
                    recreated=destination_object_recreated,
                    rows_loaded=rows_loaded,
                    duration_sec=duration_sec,
                    status=load_status,
                    start_time=start_time,
                    end_time=end_time
                )
            
            if load_status == 'success':
                logger.info(f"Load object: {object_name} completed | time: {duration_sec}s")
            else:
                logger.error(f"Load object: {object_name} failed | time: {duration_sec}s")

    def get_data_object(self, object_name):
        """Gets a DataObject instance for the specified object.
        
        Creates and returns a DataObject instance containing the object's
        configuration and settings.
        """
        return DataObject(self.pipeline, object_name)

    def get_target_object_dir(self, object_name, data_object):
        """Composes the destination directory for file-based targets.

        Logic:
        1. Use target_base_dir from target_attributes when available
        2. If object_target_dir is provided, treat it as relative to target_base_dir
        3. Fall back to legacy destination_dir for backward compatibility
        4. If no base dir is configured, object_target_dir must be absolute
        """
        target_base_dir = self.pipeline.target_attr.get("target_base_dir") or self.pipeline.target_attr.get("destination_dir")
        object_target_dir = getattr(data_object, 'object_target_dir', None)
        dest_object_name = data_object.destination_object_name or object_name

        if target_base_dir:
            if object_target_dir:
                relative_object_target_dir = str(object_target_dir).lstrip('/\\')
                return os.path.join(target_base_dir, relative_object_target_dir)
            return os.path.join(target_base_dir, self.pipeline.pipeline_name, dest_object_name)

        if object_target_dir:
            if not os.path.isabs(object_target_dir):
                logger.error("For file targets, object_target_dir must be an absolute path when target_base_dir is not set.")
                raise SystemExit()
            return object_target_dir

        logger.warning("The pipeline->target_attribute->target_base_dir in pipeline.yaml is not specified.")
        raise SystemExit()
