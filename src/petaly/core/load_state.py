# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
import os
import json
import tempfile
from datetime import datetime
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)


class LoadState:
    """
    Manages load state persistence for both full and incremental loads.
    
    Saves the last loaded timestamp and load mode for each object to enable resumable loads.
    State is persisted to a JSON file per-object in the object's metadata directory.
    
    Key responsibilities:
    - Load state from file (per-object)
    - Save state after each batch
    - Track last loaded timestamp per object (for incremental loads)
    - Track load mode (full or incremental)
    - Handle state file creation and updates atomically
    """
    UNIX_EPOCH_TIMESTAMP = "1970-01-01T00:00:00Z"
    
    def __init__(self, pipeline):
        """
        Initialize load state manager.
        
        Args:
            pipeline: Pipeline instance containing pipeline_name and paths
        """
        self.pipeline = pipeline
        self.f_handler = pipeline.m_conf.f_handler if hasattr(pipeline.m_conf, 'f_handler') else None
        
        # State file is per-object, stored in object's metadata directory
        # Path structure: {output_pipeline_dpath}/{object_name}/metadata/load_state.json
        # We'll set the actual path when we know the object_name
        self._state_file_cache = {}  # Cache of state file paths per object
        self._aggregate_state_file_path = None
        
        # In-memory state cache per object
        self._state_cache = {}
    
    def _get_aggregate_state_file_path(self) -> Optional[str]:
        """Get pipeline-level incremental state file path."""
        if self._aggregate_state_file_path is None:
            pipeline_dpath = getattr(self.pipeline, 'pipeline_dpath', None)
            if pipeline_dpath:
                self._aggregate_state_file_path = os.path.join(pipeline_dpath, 'load_state.json')
        return self._aggregate_state_file_path

    def _get_legacy_aggregate_state_file_path(self) -> Optional[str]:
        """Get legacy pipeline-level incremental state file path."""
        pipeline_dpath = getattr(self.pipeline, 'pipeline_dpath', None)
        if pipeline_dpath:
            return os.path.join(pipeline_dpath, 'incremental_load.json')
        return None

    def _load_aggregate_state(self, refresh: bool = False) -> Dict[str, Any]:
        """Load pipeline-level incremental state file."""
        aggregate_state_path = self._get_aggregate_state_file_path()
        legacy_path = self._get_legacy_aggregate_state_file_path()
        if not aggregate_state_path:
            return {}

        cache_key = '__aggregate__'
        if not refresh and cache_key in self._state_cache:
            cached = self._state_cache.get(cache_key, {})
            if isinstance(cached, dict):
                return cached

        source_path = None
        if os.path.exists(aggregate_state_path):
            source_path = aggregate_state_path
        elif legacy_path and os.path.exists(legacy_path):
            source_path = legacy_path
        else:
            return {}

        try:
            with open(source_path, 'r', encoding='utf-8') as f:
                aggregate_state = json.load(f)
                if not isinstance(aggregate_state, dict):
                    aggregate_state = {}
                self._state_cache[cache_key] = aggregate_state
                # Migrate legacy file content to the new file name.
                if source_path != aggregate_state_path:
                    self._save_aggregate_state(aggregate_state)
                return aggregate_state
        except Exception as e:
            logger.error(f"Error loading pipeline load state from {source_path}: {e}")
            return {}

    def _save_aggregate_state(self, aggregate_state: Dict[str, Any]):
        """Save pipeline-level incremental state file atomically."""
        aggregate_state_path = self._get_aggregate_state_file_path()
        if not aggregate_state_path:
            return

        aggregate_dir = os.path.dirname(aggregate_state_path)
        if aggregate_dir and not os.path.exists(aggregate_dir):
            os.makedirs(aggregate_dir, exist_ok=True)

        temp_file = aggregate_state_path + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(aggregate_state, f, indent=2, ensure_ascii=False)

        if os.name == 'nt' and os.path.exists(aggregate_state_path):
            os.remove(aggregate_state_path)
        os.rename(temp_file, aggregate_state_path)
        self._state_cache['__aggregate__'] = aggregate_state
    
    def _get_state_file_path(self, object_name: str) -> str:
        """
        Get the state file path for a specific object.
        
        Path: {output_pipeline_dpath}/{object_name}/metadata/load_state.json
        
        Args:
            object_name: Name of the object
            
        Returns:
            Full path to the state file for this object
        """
        if object_name not in self._state_file_cache:
            # Build path: output_pipeline_dpath/{object_name}/metadata/load_state.json
            object_metadata_dir = self.pipeline.output_object_metadata_dpath.format(object_name=object_name)
            state_file_path = os.path.join(object_metadata_dir, 'load_state.json')
            self._state_file_cache[object_name] = state_file_path
        return self._state_file_cache[object_name]
    
    def load_state(self, object_name: str, refresh: bool = False) -> Dict[str, Any]:
        """
        Load load state from file for a specific object.
        
        Args:
            object_name: Name of the object
            refresh: When True, bypass the in-memory cache and reread the state file
            
        Returns:
            Dictionary containing state for the object, or default state if file doesn't exist
        """
        # Check cache first
        if not refresh and object_name in self._state_cache:
            return self._state_cache[object_name]
        
        aggregate_state = self._load_aggregate_state(refresh=refresh)
        if object_name in aggregate_state:
            state = aggregate_state.get(object_name, {})
            if isinstance(state, dict):
                self._state_cache[object_name] = state
                return state

        state_file_path = self._get_state_file_path(object_name)
        
        if not os.path.exists(state_file_path):
            logger.debug(f"Load state file not found at {state_file_path}. Starting fresh for object '{object_name}'.")
            default_state = {
                "object_name": object_name,
                "load_mode": "full",
                "object_loaded_timestamp": None,
                "object_loaded_rows": 0,
                "last_run": None
            }
            self._state_cache[object_name] = default_state
            return default_state
        
        try:
            with open(state_file_path, 'r', encoding='utf-8') as f:
                state = json.load(f)
                logger.debug(f"Loaded load state from {state_file_path} for object '{object_name}'")
                self._state_cache[object_name] = state
                return state
        except Exception as e:
            logger.error(f"Error loading load state from {state_file_path}: {e}")
            # Return default state on error
            default_state = {
                "object_name": object_name,
                "load_mode": "full",
                "object_loaded_timestamp": None,
                "object_loaded_rows": 0,
                "last_run": None
            }
            self._state_cache[object_name] = default_state
            return default_state
    
    def initialize_first_batch_state(self, object_name: str, schema_table_name: str, column_last_modified: str, db_connector, table_exists: bool) -> Optional[str]:
        """
        Initialize state for the first batch in each incremental load process.
        
        Logic for first batch initialization:
        1. If target table doesn't exist:
           - Table will be created by loader
           - Create state file with load_mode="full" and object_loaded_timestamp=None
           - Extract ALL rows (not batch_size) - full load on first run
        2. If target table exists and state file doesn't exist:
           - Read MAX(column_last_modified) from target table
           - Create state file with load_mode="incremental" and that timestamp if table is not empty
           - If table is empty, initialize incremental state from Unix epoch
        3. If target table exists and state file exists:
           - Use object_loaded_timestamp from JSON file
           - This continues from last successful batch
        
        Args:
            object_name: Name of the object
            schema_table_name: Full table name (schema.table)
            column_last_modified: Column name containing timestamp
            db_connector: Database connector instance
            table_exists: Whether the target table exists
            
        Returns:
            Timestamp string (ISO format) for incremental load, or None for full load (extract all rows)
        """
        state_file_path = self._get_state_file_path(object_name)
        state_file_exists = os.path.exists(state_file_path)
        
        if not table_exists:
            # Case 1: Target table doesn't exist
            # Table will be created by loader, initialize state for full load
            # Extract ALL rows (not batch_size) - full load on first run
            logger.info(f"Target table '{schema_table_name}' doesn't exist. Initializing state file for full load for object '{object_name}'. Will extract ALL rows.")
            self._create_initial_state_file(object_name, load_mode="full", timestamp=None)
            return None  # None means extract all rows (full load)
        
        elif table_exists and not state_file_exists:
            # Case 2: Target table exists but state file doesn't exist
            # Read MAX(column_last_modified) from target table
            # Only create state file if table is NOT empty
            logger.info(f"Target table '{schema_table_name}' exists but state file doesn't. Querying database for last modified timestamp for object '{object_name}'.")
            max_timestamp = self._get_max_timestamp_from_db(schema_table_name, column_last_modified, db_connector)
            if max_timestamp:
                # Table is not empty, create state file with database timestamp
                logger.info(f"Found last modified timestamp in database: {max_timestamp}. Table is not empty. Initializing state file for object '{object_name}'.")
                self._create_initial_state_file(object_name, load_mode="incremental", timestamp=max_timestamp)
                return max_timestamp
            else:
                # Table exists but is empty or column is NULL.
                # Keep incremental semantics and start from Unix epoch.
                logger.info(
                    f"Table '{schema_table_name}' exists but is empty or '{column_last_modified}' is NULL. "
                    f"Initializing incremental state from Unix epoch for object '{object_name}'."
                )
                self._create_initial_state_file(
                    object_name,
                    load_mode="incremental",
                    timestamp=self.UNIX_EPOCH_TIMESTAMP
                )
                return self.UNIX_EPOCH_TIMESTAMP
        
        else:
            # Case 3: Target table exists and state file exists
            # Use timestamp from state file
            state = self.load_state(object_name, refresh=True)
            timestamp = state.get('object_loaded_timestamp')
            if timestamp:
                logger.debug(f"Using existing state file timestamp for object '{object_name}': {timestamp}")
                return timestamp
            else:
                # State file exists but timestamp is None, query database
                logger.warning(f"State file exists for object '{object_name}' but timestamp is None. Querying database...")
                max_timestamp = self._get_max_timestamp_from_db(schema_table_name, column_last_modified, db_connector)
                if max_timestamp:
                    # Update state file with database timestamp
                    state['object_loaded_timestamp'] = max_timestamp
                    state['last_run'] = datetime.utcnow().isoformat() + 'Z'
                    self._save_state_file(object_name, state)
                    return max_timestamp
                else:
                    # Keep incremental semantics and start from Unix epoch.
                    state['load_mode'] = 'incremental'
                    state['object_loaded_timestamp'] = self.UNIX_EPOCH_TIMESTAMP
                    state['last_run'] = datetime.utcnow().isoformat() + 'Z'
                    self._save_state_file(object_name, state)
                    return self.UNIX_EPOCH_TIMESTAMP
    
    def _create_initial_state_file(self, object_name: str, load_mode: str, timestamp: Optional[str] = None):
        """
        Create initial state file for an object.
        
        Args:
            object_name: Name of the object
            load_mode: Load mode ("full" or "incremental")
            timestamp: Initial timestamp to set (ISO format) or None for full load
        """
        state = {
            "object_name": object_name,
            "load_mode": load_mode,
            "object_loaded_timestamp": timestamp,
            "object_loaded_rows": 0,
            "last_run": datetime.utcnow().isoformat() + 'Z'
        }
        self._save_state_file(object_name, state)
        if timestamp:
            logger.debug(f"Created initial state file for object '{object_name}' with load_mode='{load_mode}' and timestamp: {timestamp}")
        else:
            logger.debug(f"Created initial state file for object '{object_name}' with load_mode='{load_mode}' (full load, no timestamp)")
    
    def _save_state_file(self, object_name: str, state: Dict[str, Any]):
        """
        Save state file for an object (internal helper method).
        
        Args:
            object_name: Name of the object
            state: State dictionary to save
        """
        # Update cache
        self._state_cache[object_name] = state
        aggregate_state = self._load_aggregate_state(refresh=True)
        aggregate_state[object_name] = state
        self._save_aggregate_state(aggregate_state)
        
        # Get state file path for this object
        state_file_path = self._get_state_file_path(object_name)
        
        # Ensure metadata directory exists
        metadata_dir = os.path.dirname(state_file_path)
        if not os.path.exists(metadata_dir):
            os.makedirs(metadata_dir, exist_ok=True)
        
        # Atomic write: write to temp file, then rename
        temp_file = state_file_path + '.tmp'
        
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        
        # Atomic rename (works on Unix and Windows)
        if os.name == 'nt':  # Windows
            if os.path.exists(state_file_path):
                os.remove(state_file_path)

            aggregate_state = self._load_aggregate_state(refresh=True)
            if object_name in aggregate_state:
                del aggregate_state[object_name]
                self._save_aggregate_state(aggregate_state)
        os.rename(temp_file, state_file_path)
    
    def get_last_loaded_timestamp(self, object_name: str, schema_table_name: str = None, column_last_modified: str = None, db_connector=None) -> Optional[str]:
        """
        Get the last loaded timestamp for a specific object.
        
        Strategy (hybrid approach):
        1. Primary: Read from state file (fast, no DB query needed)
        2. Fallback: If state file doesn't exist or timestamp not found, query target database
           for MAX(column_last_modified) to get the actual last timestamp
        3. This ensures reliability even if state file is missing/corrupted
        
        Args:
            object_name: Name of the object
            schema_table_name: Optional full table name (schema.table) for database fallback query
            column_last_modified: Optional column name for timestamp (e.g., 'modified_at')
            db_connector: Optional database connector instance for fallback query
            
        Returns:
            Last loaded timestamp string (ISO format) or None if not found
        """
        # Strategy 1: Try to read from state file first (fast, no DB query)
        state = self.load_state(object_name, refresh=bool(schema_table_name and column_last_modified and db_connector))
        timestamp = state.get('object_loaded_timestamp')
        if timestamp:
            logger.debug(f"Found last loaded timestamp for '{object_name}' in state file: {timestamp}")
            return timestamp
        
        # Strategy 2: Fallback to database query if state file doesn't have it
        # This handles cases where:
        # - State file doesn't exist (first run)
        # - Object not in state file yet
        # - State file was deleted/corrupted
        if schema_table_name and column_last_modified and db_connector:
            try:
                logger.debug(f"State file doesn't have timestamp for '{object_name}', querying database...")
                max_timestamp = self._get_max_timestamp_from_db(schema_table_name, column_last_modified, db_connector)
                if max_timestamp:
                    logger.info(f"Found last modified timestamp in database for '{object_name}': {max_timestamp}")
                    # Optionally save this to state file for next time
                    # But don't do it here to avoid side effects - let the loader save it after first batch
                    return max_timestamp
                else:
                    logger.debug(f"No rows found in '{schema_table_name}' or column '{column_last_modified}' is NULL. Starting from beginning.")
                    return None
            except Exception as e:
                logger.warning(f"Could not query database for last timestamp: {e}. Starting from beginning.")
                return None
        
        # No state file entry and no database fallback available
        logger.debug(f"No last loaded timestamp found for '{object_name}'. Starting from beginning.")
        return None
    
    def _get_max_timestamp_from_db(self, schema_table_name: str, column_last_modified: str, db_connector) -> Optional[str]:
        """
        Query database for MAX(column_last_modified) from target table.
        
        This is used as a fallback when state file doesn't have the timestamp.
        
        Args:
            schema_table_name: Full table name (schema.table)
            column_last_modified: Column name containing timestamp
            db_connector: Database connector instance
            
        Returns:
            Maximum timestamp value as ISO string, or None if table is empty or column is NULL
        """
        try:
            # Build query: SELECT MAX(column_last_modified) FROM schema_table_name
            # Handle different database quote styles
            if hasattr(db_connector, 'metaquery_quote'):
                quote = db_connector.metaquery_quote
            else:
                quote = '"'  # Default to double quote
            
            # Handle BigQuery backticks
            if hasattr(db_connector, 'target_connector_id') and db_connector.target_connector_id == 'bigquery':
                table_name = f"`{schema_table_name}`"
                column_name = f"`{column_last_modified}`"
            else:
                table_name = schema_table_name
                column_name = f"{quote}{column_last_modified}{quote}"
            
            max_query = f"SELECT MAX({column_name}) as max_timestamp FROM {table_name}"
            logger.debug(f"Executing query: {max_query}")
            
            # Execute query based on connector type
            if hasattr(db_connector, 'target_connector_id') and db_connector.target_connector_id == 'bigquery':
                result = db_connector.get_metadata_result(max_query)
                if result and len(result) > 0:
                    max_timestamp = result[0].get('max_timestamp')
            elif hasattr(db_connector, 'execute_sql') and hasattr(db_connector, 'is_serverless'):
                # Redshift IAM
                result_data, request_id = db_connector.execute_sql(max_query, sleep_sec=1)
                if result_data and result_data.get('Records'):
                    records = result_data.get('Records', [])
                    if records and len(records) > 0:
                        first_record = records[0]
                        if first_record and len(first_record) > 0:
                            value_dict = first_record[0]
                            max_timestamp = value_dict.get('stringValue') or value_dict.get('timestampValue')
            else:
                # PostgreSQL, MySQL, Redshift TCP
                result = db_connector.get_query_result(max_query)
                if result and len(result) > 0:
                    first_row = result[0]
                    if isinstance(first_row, dict):
                        max_timestamp = first_row.get('max_timestamp')
                    elif isinstance(first_row, (list, tuple)):
                        max_timestamp = first_row[0]
                    else:
                        max_timestamp = first_row
            
            # Convert timestamp to ISO format string if needed
            if max_timestamp is None:
                return None
            
            # Handle different timestamp formats
            if isinstance(max_timestamp, str):
                # Already a string, return as-is (assuming ISO format)
                return max_timestamp
            elif hasattr(max_timestamp, 'isoformat'):
                # datetime object
                return max_timestamp.isoformat()
            else:
                # Convert to string
                return str(max_timestamp)
                
        except Exception as e:
            logger.debug(f"Error querying MAX timestamp from database: {e}")
            return None
    
    def save_batch_state(self, object_name: str, batch_end_timestamp: Optional[str] = None, rows_loaded: int = 0, load_mode: str = "incremental"):
        """
        Save state after a batch is successfully loaded.
        
        This method atomically updates the state file to ensure consistency
        even if the process is killed during the write.
        
        Args:
            object_name: Name of the object
            batch_end_timestamp: Timestamp of the last row in the batch (ISO format) or None for full load
            rows_loaded: Number of rows loaded in this batch (optional, for tracking)
            load_mode: Load mode ("full" or "incremental"). If "full", object_loaded_timestamp will be None
        """
        try:
            # Load current state for this object
            state = self.load_state(object_name, refresh=True)
            
            # Update state
            state['load_mode'] = load_mode
            # For full load, keep object_loaded_timestamp empty (None)
            # For incremental load, set it to batch_end_timestamp
            if load_mode == "full":
                state['object_loaded_timestamp'] = None
            else:
                state['object_loaded_timestamp'] = batch_end_timestamp
            state['object_loaded_rows'] = state.get('object_loaded_rows', 0) + rows_loaded
            state['last_run'] = datetime.utcnow().isoformat() + 'Z'
            state['object_name'] = object_name  # Ensure object_name is set
            
            self._save_state_file(object_name, state)
            
            logger.debug(f"Saved load state for object '{object_name}': last_timestamp={batch_end_timestamp}, rows_loaded={rows_loaded}")
            
        except Exception as e:
            logger.error(f"Error saving load state for object '{object_name}': {e}", exc_info=True)
            # Don't raise - we don't want to fail the entire pipeline if state save fails
            # The next run will start from the last successfully saved timestamp

    def ensure_object_state(self, object_name: str, load_mode: str = "full"):
        """
        Ensure an object entry exists in state files.

        Creates default state for the object if it does not exist yet.
        """
        state_file_path = self._get_state_file_path(object_name)
        if os.path.exists(state_file_path):
            # Ensure aggregate file has the same object as well.
            state = self.load_state(object_name, refresh=True)
            self._save_state_file(object_name, state)
            return

        self._create_initial_state_file(
            object_name=object_name,
            load_mode=load_mode,
            timestamp=None
        )
    
    def reset_object_state(self, object_name: str):
        """
        Reset state for a specific object (e.g., when doing full load).
        
        This deletes the state file for the object.
        
        Args:
            object_name: Name of the object to reset
        """
        try:
            state_file_path = self._get_state_file_path(object_name)
            
            # Delete state file if it exists
            if os.path.exists(state_file_path):
                os.remove(state_file_path)
                logger.debug(f"Deleted load state file for object '{object_name}': {state_file_path}")
            
            # Remove from cache
            if object_name in self._state_cache:
                del self._state_cache[object_name]
                
            logger.debug(f"Reset load state for object '{object_name}'")
        except Exception as e:
            logger.error(f"Error resetting load state for object '{object_name}': {e}", exc_info=True)
