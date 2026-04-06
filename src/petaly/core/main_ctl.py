# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
import json
import concurrent.futures
import threading
import time
from petaly.utils.utils import sanitize_sensitive_data
from petaly.core.data_object import DataObject

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
        
        Args:
            pipeline: Pipeline instance
            run_endpoint: 'source', 'target', or None for both
            object_name_list: Optional comma-separated list of object names to process
        """
        pipeline_name = pipeline.pipeline_name
        
        logger.info(f"[Start] Pipeline {pipeline_name}")

        # Log consolidated pipeline configuration (debug mode)
        self.save_execution_config(pipeline)

        if object_name_list is not None:
            pipeline.data_objects_from_cli = object_name_list.split(',')

        # Check dump mode: false (default) = object flow, true = dump flow.
        dump_mode = pipeline.load_attributes.get('dump_mode')
        if dump_mode is None:
            flow_mode = str(pipeline.load_attributes.get('flow_mode', 'object')).lower()
            dump_mode = flow_mode == 'dump'
        elif isinstance(dump_mode, str):
            dump_mode = dump_mode.lower() in ('true', '1', 'yes')
        else:
            dump_mode = bool(dump_mode)

        if not dump_mode:
            # Object flow: extract → load per object, with parallel processing support
            self.run_pipeline_end_to_end(pipeline, run_endpoint)
        else:
            # Dump flow: extract all → load all
            if run_endpoint is None or run_endpoint == 'source':
                self.run_source(pipeline)

            if run_endpoint is None or run_endpoint == 'target':
                self.run_target(pipeline)

        logger.info(f"[End] Pipeline {pipeline_name}")
    
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

    ####################### run pipeline object ####################################

    def run_pipeline_end_to_end(self, pipeline, run_endpoint):
        """
        Run pipeline in object mode: extract → load per object.

        This method processes objects one by one (extract then load) instead of
        extracting all objects first, then loading all objects. Supports parallel
        processing of multiple objects using max_workers parameter.

        Args:
            pipeline: Pipeline instance
            run_endpoint: 'source', 'target', or None for both
        """
        logger.info("[Object Flow Mode] Processing objects: extract → load per object")

        # Get source and target extractors/loaders
        source_extractor = None
        target_loader = None

        if run_endpoint is None or run_endpoint == 'source':
            if not pipeline.source_connector_id:
                logger.error(
                    f"Pipeline {pipeline.pipeline_name}: source_connector_id is None. "
                    f"Please configure source_attributes in the pipeline file: {pipeline.pipeline_fpath}"
                )
                return
            source_class = self.m_conf.get_extractor_class(pipeline.source_connector_id)
            if source_class:
                source_extractor = source_class(pipeline)
            else:
                logger.error(f"Extractor with connector-id: {pipeline.source_connector_id} can't be initialized.")

        if run_endpoint is None or run_endpoint == 'target':
            if not pipeline.target_connector_id:
                logger.error(
                    f"Pipeline {pipeline.pipeline_name}: target_connector_id is None. "
                    f"Please configure target_attributes in the pipeline file: {pipeline.pipeline_fpath}"
                )
                return
            target_class = self.m_conf.get_loader_class(pipeline.target_connector_id)
            if target_class:
                target_loader = target_class(pipeline)
            else:
                logger.error(f"Loader with connector-id: {pipeline.target_connector_id} can't be initialized.")

        if not source_extractor and not target_loader:
            logger.error("Neither source extractor nor target loader could be initialized")
            return

        # Get list of objects to process
        if source_extractor:
            # Cleanup output directory once at the start (for object mode)
            source_extractor.f_handler.cleanup_dir(pipeline.output_pipeline_dpath)
            
            # Get object list based on extractor type
            # Database extractors: process metadata to get object list
            # File extractors: use pipeline.data_objects
            if hasattr(source_extractor, 'compose_meta_query'):
                # Database extractor - process metadata once to get object list and set up metadata
                # This is required before extract_per_object can work
                meta_query = source_extractor.compose_meta_query()
                meta_result = source_extractor.execute_meta_query(meta_query)
                object_list = source_extractor.object_metadata.process_metadata(meta_result)
            else:
                # File extractor - use pipeline.data_objects
                object_list = pipeline.data_objects
        else:
            # If only loading, get objects from output directory
            from petaly.core.composer import Composer
            composer = Composer()
            object_list = composer.get_object_list_from_output_dir(pipeline)

        if not object_list:
            logger.warning("No objects found to process")
            return

        # Get max_workers for parallel processing
        max_workers = int(pipeline.load_attributes.get('max_workers', 1))
        logger.info(f"[Object] Processing {len(object_list)} objects with max_workers={max_workers}")

        # Initialize load summary if loading
        if target_loader:
            from petaly.core.load_summary import LoadSummary
            load_summary = LoadSummary()
            # Ensure pipeline-level and object-level state entries exist for all objects in scope.
            for obj_name in object_list:
                data_object = DataObject(pipeline, obj_name)
                object_mode = 'incremental' if data_object.extract_load_mode == 'incremental' else 'full'
                target_loader.load_state.ensure_object_state(obj_name, load_mode=object_mode)
        else:
            load_summary = None

        def process_object_end_to_end(obj_name: str):
            """Process a single object: extract → load (object)"""
            thread_id = threading.current_thread().ident
            thread_name = threading.current_thread().name
            start_time = time.time()
            
            if max_workers > 1:
                logger.info(f"[Object] [Thread-{thread_id}] Processing object: {obj_name} (PARALLEL MODE)")
            else:
                logger.info(f"[Object] Processing object: {obj_name} (SEQUENTIAL MODE)")
            
            try:
                # For parallel processing, create thread-local extractors/loaders to avoid connection sharing
                # Database connections are not thread-safe, so each thread needs its own instances
                thread_source_extractor = None
                thread_target_loader = None
                
                if max_workers > 1:
                    logger.debug(f"[Object] [Thread-{thread_id}] Creating thread-local extractor/loader instances")
                    # Create new extractor/loader instances for this thread
                    if source_extractor and (run_endpoint is None or run_endpoint == 'source'):
                        source_class = self.m_conf.get_extractor_class(pipeline.source_connector_id)
                        if source_class:
                            thread_source_extractor = source_class(pipeline)
                    
                    if target_loader and (run_endpoint is None or run_endpoint == 'target'):
                        target_class = self.m_conf.get_loader_class(pipeline.target_connector_id)
                        if target_class:
                            thread_target_loader = target_class(pipeline)
                else:
                    # Sequential processing - reuse shared instances
                    thread_source_extractor = source_extractor
                    thread_target_loader = target_loader

                data_object = DataObject(pipeline, obj_name)
                run_incremental_batches = (
                    run_endpoint is None
                    and thread_source_extractor is not None
                    and thread_target_loader is not None
                    and data_object.extract_load_mode == 'incremental'
                    and data_object.batch_size is not None
                )

                if run_incremental_batches:
                    batch_number = 1
                    while True:
                        logger.info(
                            f"[Object] Processing incremental batch {batch_number} for object: {obj_name}"
                        )
                        thread_source_extractor.extract_per_object(obj_name)

                        loader_obj_conf = thread_target_loader.get_loader_obj_conf(obj_name)
                        extracted_rows = thread_target_loader.count_rows_in_csv_files(
                            loader_obj_conf.get('output_data_object_dir'),
                            loader_obj_conf
                        )

                        if extracted_rows == 0:
                            logger.info(
                                f"[Object] No more incremental rows to process for object: {obj_name}. "
                                f"Stopping after {batch_number - 1} completed batches."
                            )
                            break

                        thread_target_loader.load_per_object(obj_name, load_summary)

                        if extracted_rows < data_object.batch_size:
                            logger.info(
                                f"[Object] Final incremental batch detected for object: {obj_name} "
                                f"({extracted_rows} rows < batch_size {data_object.batch_size})."
                            )
                            break

                        batch_number += 1
                else:
                    # Extract object (if source processing enabled)
                    if thread_source_extractor and (run_endpoint is None or run_endpoint == 'source'):
                        thread_source_extractor.extract_per_object(obj_name)

                    # Load object (if target processing enabled)
                    if thread_target_loader and (run_endpoint is None or run_endpoint == 'target'):
                        thread_target_loader.load_per_object(obj_name, load_summary)

                elapsed_time = time.time() - start_time
                if max_workers > 1:
                    logger.info(f"[Object] [Thread-{thread_id}] Successfully processed object: {obj_name} in {elapsed_time:.2f}s (PARALLEL)")
                else:
                    logger.info(f"[Object] Successfully processed object: {obj_name} in {elapsed_time:.2f}s")
                
                # Close thread-local connections if they were created
                if max_workers > 1:
                    logger.debug(f"[Object] [Thread-{thread_id}] Closing thread-local database connections")
                    if thread_source_extractor and hasattr(thread_source_extractor, 'db_connector'):
                        if hasattr(thread_source_extractor.db_connector, 'conn') and thread_source_extractor.db_connector.conn:
                            try:
                                thread_source_extractor.db_connector.conn.close()
                            except Exception:
                                pass
                    if thread_target_loader and hasattr(thread_target_loader, 'db_connector'):
                        if hasattr(thread_target_loader.db_connector, 'conn') and thread_target_loader.db_connector.conn:
                            try:
                                thread_target_loader.db_connector.conn.close()
                            except Exception:
                                pass

            except Exception as exc:
                logger.error(f"[Object] Failed to process object {obj_name}: {exc}", exc_info=True)
                raise

        # Process objects in parallel batches limited by max_workers
        if max_workers == 1:
            # Sequential processing (no threading overhead)
            logger.info(f"[Object] Running in SEQUENTIAL mode (max_workers=1)")
            for obj_name in object_list:
                process_object_end_to_end(obj_name)
        else:
            # Parallel processing
            logger.info(f"[Object] Running in PARALLEL mode (max_workers={max_workers})")
            logger.info(f"[Object] Objects will be processed concurrently across {max_workers} worker threads")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_obj = {executor.submit(process_object_end_to_end, obj): obj for obj in object_list}
                for future in concurrent.futures.as_completed(future_to_obj):
                    obj = future_to_obj[future]
                    try:
                        future.result()
                    except Exception as exc:
                        logger.error(f"[Object] Failed to process object {obj}: {exc}")
                        # Continue with next object instead of failing the entire pipeline

        # Display load summary if loading was performed
        if target_loader and load_summary:
            load_summary.display()
