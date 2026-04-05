# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
import os
from typing import Dict, Any, Optional

from petaly.utils.file_handler import FileHandler

logger = logging.getLogger(__name__)


class Connections:
    """
    Manages connection configuration loading and resolution.
    
    Handles loading connections from connections.yaml/json files and resolving
    connection references to their full attribute configurations.
    
    Key responsibilities:
    - Load connections configuration from file
    - Resolve connection references to full attributes
    - Merge connection attributes with inline overrides
    """
    
    def __init__(self, connections_fpath: str):
        """
        Initializes the Connections instance.
        
        Args:
            connections_fpath: Path to the connections.yaml/json file
        """
        self.connections_fpath = connections_fpath
        self.f_handler = FileHandler()
        self._connections_config = None
        self._connections_dict = None
    
    def load_config(self) -> Optional[Dict[str, Any]]:
        """
        Loads the connections configuration from connections.yaml/json.
        
        Logic:
        1. Check if connections file exists
        2. Load based on file format (YAML/JSON)
        3. Cache the configuration for subsequent calls
        4. Return connections configuration or None if file doesn't exist
        
        Note: connections.yaml should be created during workspace initialization.
        
        Returns:
            Dictionary containing connections configuration or None if file doesn't exist
        """
        # Return cached config if already loaded
        if self._connections_config is not None:
            return self._connections_config
        
        if not os.path.exists(self.connections_fpath):
            # Connections file should be created during workspace init
            # If it doesn't exist here, it means workspace wasn't initialized properly
            logger.debug(f"Connections file not found at {self.connections_fpath}, using inline attributes")
            self._connections_config = None
            self._connections_dict = None
            return None
        
        try:
            file_extension = os.path.splitext(self.connections_fpath)[1].lower()
            
            if file_extension == '.yaml':
                self._connections_config = self.f_handler.load_yaml(self.connections_fpath)
            elif file_extension == '.json':
                self._connections_config = self.f_handler.load_json(self.connections_fpath)
            else:
                logger.error(f"Unsupported connections file format: {file_extension}")
                self._connections_config = None
                self._connections_dict = None
                return None
            
            # Extract connections dictionary
            self._connections_dict = self._connections_config.get('connections', self._connections_config)
            
            logger.debug(f"Loaded connections configuration from {self.connections_fpath}")
            return self._connections_config
        except Exception as e:
            logger.error(f"Error loading connections configuration from {self.connections_fpath}: {e}")
            self._connections_config = None
            self._connections_dict = None
            return None
    
    def resolve_connection(self, connection_name: str, connection_type: str = '') -> Optional[Dict[str, Any]]:
        """
        Resolves a connection reference to its full configuration.
        
        Logic:
        1. Load connections configuration if not already loaded
        2. Look up connection by name in connections dictionary
        3. Return connection attributes
        
        Args:
            connection_name: Name of the connection to resolve
            connection_type: Type of connection ('source' or 'target') - used for logging only
        
        Returns:
            Dictionary containing connection attributes or None if not found
        """
        # Load config if not already loaded
        if self._connections_dict is None:
            self.load_config()
        
        if not self._connections_dict:
            return None
        
        # Look for connection in flat connections structure
        connection = self._connections_dict.get(connection_name)
        
        if not connection:
            logger.error(f"Connection '{connection_name}' not found in connections")
            return None
        
        log_msg = f"Resolved {connection_type} connection '{connection_name}'" if connection_type else f"Resolved connection '{connection_name}'"
        logger.debug(log_msg)
        return connection
    
    def resolve_attributes(self, attributes: Dict[str, Any], connection_type: str = '') -> Dict[str, Any]:
        """
        Resolves attributes that may contain a connection reference.
        
        If attributes contain a 'connection_name' key, resolves it to full attributes
        and merges with any additional inline attributes (inline attributes override connection).
        
        Logic:
        1. Check if attributes contain 'connection_name' reference
        2. If yes, resolve connection and merge with inline attributes
        3. If no, return attributes as-is
        
        Args:
            attributes: Dictionary that may contain 'connection_name' key
            connection_type: Type of connection ('source' or 'target') - used for logging
        
        Returns:
            Dictionary with resolved attributes (connection reference replaced with full attributes)
        """
        connection_ref = attributes.get('connection_name')
        
        if not connection_ref:
            # No connection reference, return attributes as-is
            return attributes.copy()
        
        # Resolve connection
        resolved_connection = self.resolve_connection(connection_ref, connection_type)
        
        if not resolved_connection:
            # Could not resolve connection - this is an error if only connection reference is provided
            # If there are other attributes, we can try to use them, but warn about missing connection
            if len(attributes) == 1 and 'connection_name' in attributes:
                # Only connection reference, no other attributes - this is an error
                logger.error(f"Could not resolve {connection_type} connection '{connection_ref}'. The connection does not exist in connections.yaml. Please create it or use inline attributes.")
                raise ValueError(f"Connection '{connection_ref}' not found. Create it using 'petaly init -e {connection_ref}' or use inline attributes in pipeline configuration.")
            else:
                # Other attributes present - keep them but warn
                logger.warning(f"Could not resolve {connection_type} connection '{connection_ref}', using inline attributes only")
                return {k: v for k, v in attributes.items() if k != 'connection_name'}

        endpoint_type = resolved_connection.get('endpoint_type')
        if endpoint_type is not None:
            endpoint_type = str(endpoint_type).lower()

        allowed_types = {'source', 'target'}
        if endpoint_type is not None and endpoint_type not in allowed_types:
            raise ValueError(
                f"Connection '{connection_ref}' has invalid endpoint_type '{endpoint_type}'. "
                f"Expected one of: source, target."
            )

        if connection_type == 'source' and endpoint_type == 'target':
            raise ValueError(
                f"Connection '{connection_ref}' is defined as target-only and cannot be used as a source."
            )
        if connection_type == 'target' and endpoint_type == 'source':
            raise ValueError(
                f"Connection '{connection_ref}' is defined as source-only and cannot be used as a target. "
                f"This protects source endpoints from write operations."
            )
        
        # Merge resolved connection with any additional inline attributes (inline overrides connection)
        # Preserve connection_name so it can be used for display purposes (e.g., in load summary)
        merged_attributes = {**resolved_connection, **{k: v for k, v in attributes.items() if k != 'connection_name'}}
        merged_attributes['connection_name'] = connection_ref  # Preserve connection name for reference
        
        return merged_attributes
    
    def get_all_connections(self) -> Dict[str, Any]:
        """
        Gets all connections from the configuration.
        
        Returns:
            Dictionary containing all connections or empty dict if not loaded
        """
        if self._connections_dict is None:
            self.load_config()
        
        return self._connections_dict if self._connections_dict else {}
