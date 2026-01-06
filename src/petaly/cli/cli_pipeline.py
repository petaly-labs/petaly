# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import os
import logging
import rich.prompt as prompt
from rich.console import Console

from petaly.utils.file_handler import FileHandler

logger = logging.getLogger(__name__)


class CliPipeline:
    """
    CLI handler for managing pipeline configuration.
    
    Provides functionality to:
    - Compose pipeline configurations
    - Handle connection selection/creation during pipeline setup
    - Configure pipeline attributes, data attributes, and object specs
    """
    
    def __init__(self, main_config, cli_menu):
        """
        Initialize CliPipeline instance.
        
        Args:
            main_config: MainConfig instance
            cli_menu: CliMenu instance with common utilities
        """
        self.m_conf = main_config
        self.cli_menu = cli_menu
        self.console = cli_menu.console
        self.f_handler = FileHandler()
        self.break_line = cli_menu.break_line
        
        # Load pipeline meta config and skeleton
        self.pipeline_meta_config = self.f_handler.load_json(self.m_conf.pipeline_meta_config_fpath)
        self.composed_pipeline_config = self.f_handler.load_json(self.m_conf.pipeline_skeleton_fpath)
    
    def compose_pipeline(self, pipeline_name):
        """
        Composes a pipeline configuration with improved connection flow.
        
        Flow:
        1. Compose pipeline attributes
        2. Check for connections.yaml and guide user through connection selection
        3. Allow creating connections on-the-fly if needed
        4. Compose data attributes
        
        Args:
            pipeline_name: Name of the pipeline to compose
        """
        # Set pipeline_name directly (no longer using pipeline_attributes section)
        self.composed_pipeline_config['pipeline']['pipeline_name'] = pipeline_name
        
        # Check if connections.yaml exists
        # Priority: 1) connections_file_path from config, 2) default: pipeline_dir_path/connections.yaml
        if hasattr(self.m_conf, 'connections_file_path') and self.m_conf.connections_file_path:
            connections_fpath = self.m_conf.connections_file_path
        else:
            connection_format = self.m_conf.global_settings.get('connections_file_format', 'yaml')
            connections_fname = f'connections.{connection_format}'
            connections_fpath = os.path.join(self.m_conf.pipeline_base_dpath, connections_fname)
        
        connections_exist = self.f_handler.is_file(connections_fpath)
        
        if connections_exist:
            # connections file exists - use it
            use_connections = True
        else:
            # No connections file - ask if user wants to create one
            self.console.print(f"\n[bold]{self.break_line}[/bold]")
            self.console.print(f"[bold]connection Configuration[/bold]\n")
            self.console.print(f"No {connections_fname} file found.")
            create_connections = prompt.Confirm.ask(
                "Would you like to create connections.yaml and use connections? (Recommended for reusable configurations)",
                default=True
            )
            
            if create_connections:
                # Initialize connections file
                from petaly.cli.cli_connections import CliConnections
                connections_cli = CliConnections(self.m_conf, self.cli_menu)
                connections_cli.init_connections()
                use_connections = True
            else:
                use_connections = False
        
        if use_connections:
            # Use connections - ask once if user wants to use existing connections
            # Load connections to check if any exist
            existing_connection_names = []
            if self.f_handler.is_file(connections_fpath):
                file_extension = os.path.splitext(connections_fpath)[1].lower()
                if file_extension == '.yaml':
                    connections_config = self.f_handler.load_yaml(connections_fpath)
                else:
                    connections_config = self.f_handler.load_json(connections_fpath)
                
                connections_dict = connections_config.get('connections', {})
                existing_connection_names = list(connections_dict.keys())
            
            if existing_connection_names:
                # Show list of existing connections directly
                self.console.print(f"\nExisting connections:")
                self.console.print(f"  0. [bold cyan]Create new connection[/bold cyan]")
                for idx, connection_name in enumerate(existing_connection_names, 1):
                    self.console.print(f"  {idx}. {connection_name}")
                
                # Select source connection
                source_connection_name = self.cli_menu._select_connection_from_list('source', existing_connection_names, len(existing_connection_names), connections_fpath)
                
                # Select target connection (don't show list again)
                target_connection_name = self.cli_menu._select_connection_from_list('target', existing_connection_names, len(existing_connection_names), connections_fpath)
            else:
                # No existing connections, create new ones
                source_connection_name = self.cli_menu.select_or_create_connection('source', connections_fpath, skip_list=True)
                target_connection_name = self.cli_menu.select_or_create_connection('target', connections_fpath, skip_list=True)
            
            # Set connection references inside source_attributes and target_attributes
            self.composed_pipeline_config['pipeline']['source_attributes']['connection_name'] = source_connection_name
            self.composed_pipeline_config['pipeline']['target_attributes']['connection_name'] = target_connection_name
            
            # Prompt for schema/dataset if connector supports it (pipeline-specific, not connection)
            self._prompt_for_schema_if_needed('source_attributes', source_connection_name)
            self._prompt_for_schema_if_needed('target_attributes', target_connection_name)
            
            # Prompt for bucket_pipeline_prefix if connector needs it
            self._prompt_for_bucket_prefix_if_needed('source_attributes', source_connection_name, pipeline_name)
            self._prompt_for_bucket_prefix_if_needed('target_attributes', target_connection_name, pipeline_name)
            
            self.console.print(f"\n[green]✓[/green] Pipeline will use connections:")
            self.console.print(f"  Source: {source_connection_name}")
            self.console.print(f"  Target: {target_connection_name}")
        else:
            # Use inline attributes (backward compatibility)
            self.console.print(f"\n[bold]{self.break_line}[/bold]")
            self.console.print(f"[bold]Using Inline Attributes[/bold]\n")
            self.console.print("You'll configure source and target attributes directly in the pipeline.")
            self.compose_connection_attributes('source_attributes')
            self.compose_connection_attributes('target_attributes')
            
            # Prompt for file connector-specific attributes if connector is CSV, Parquet, or JSON (for inline attributes)
            source_connector_type = self.composed_pipeline_config['pipeline']['source_attributes'].get('connector_type')
            target_connector_type = self.composed_pipeline_config['pipeline']['target_attributes'].get('connector_type')
            
            if source_connector_type in ('csv', 'parquet', 'json'):
                connector_name = source_connector_type.upper()
                self.console.print(f"\n[bold]Configure {connector_name} Source Directory[/bold]")
                source_dir = prompt.Prompt.ask(
                    f"Enter [bold yellow]source_dir[/bold yellow] (absolute path to source {connector_name} files directory)",
                    default=None
                )
                if source_dir:
                    self.composed_pipeline_config['pipeline']['source_attributes']['source_dir'] = source_dir
            
            if target_connector_type in ('csv', 'parquet', 'json'):
                connector_name = target_connector_type.upper()
                self.console.print(f"\n[bold]Configure {connector_name} Target Directory[/bold]")
                destination_dir = prompt.Prompt.ask(
                    f"Enter [bold yellow]destination_dir[/bold yellow] (absolute path to destination directory)",
                    default=None
                )
                if destination_dir:
                    self.composed_pipeline_config['pipeline']['target_attributes']['destination_dir'] = destination_dir
            
            # Prompt for bucket_pipeline_prefix if connector needs it (for inline attributes)
            self._prompt_for_bucket_prefix_if_needed('source_attributes', None, pipeline_name)
            self._prompt_for_bucket_prefix_if_needed('target_attributes', None, pipeline_name)
        
        self.compose_load_attributes()
    

    def compose_connection_attributes(self, connection_attributes_name):
        """
        Composes inline connection attributes (for backward compatibility when not using connections.yaml).
        
        Args:
            connection_attributes_name: 'source_attributes' or 'target_attributes'
        """
        self.console.print(f"\n[bold]{self.break_line}[/bold]")
        self.console.print(f"Specify [bold]{connection_attributes_name}[/bold]\n")

        connection_attributes_dict = {}
        predefined_values = {}

        # step 1. specify connection type
        available_connectors = self.m_conf.get_available_connectors()
        connector_type = prompt.Prompt.ask(f"Specify [bold yellow]{connection_attributes_name}[/bold yellow] connector type", choices=available_connectors)
        predefined_values.update({'connector_type': connector_type})

        assigned_connection_attributes = self.cli_menu.assign_attributes(connection_attributes_dict, predefined_values=predefined_values)
        self.composed_pipeline_config['pipeline'][connection_attributes_name].update(assigned_connection_attributes)

        # step 2. get connector category
        connector_category = self.m_conf.get_connector_class_config(connector_type).get('connector_category')

        # step 3. based on connector category define database or file parameters
        connector_attributes = self.m_conf.get_connector_attributes(connector_type)

        # Exclude attributes based on connector category and connection type
        exclude_key_list = [None]
        if connector_type == 'csv':
            # CSV connector: source_dir for source, destination_dir for target
            if connection_attributes_name == 'source_attributes':
                exclude_key_list = ['destination_dir']
            elif connection_attributes_name == 'target_attributes':
                exclude_key_list = ['source_dir']
        elif connector_category in ('file','storage') and connection_attributes_name == 'source_attributes':
            exclude_key_list = ['destination_dir']
        
        # Exclude bucket_pipeline_prefix from assign_attributes - it will be prompted separately
        # This ensures consistent prompting for both connection-based and inline attribute flows
        exclude_key_list.append('bucket_pipeline_prefix')
        
        # Note: database_schema is included here (not excluded) because it's pipeline-specific
        # When using connections, database_schema will be added to pipeline attributes
        # and merged with connection attributes during resolution

        assigned_connector_attributes = self.cli_menu.assign_attributes(connector_attributes, exclude_key_list=exclude_key_list, predefined_values=None)
        self.composed_pipeline_config['pipeline'][connection_attributes_name].update(assigned_connector_attributes)

        # step 4. get and define platform type
        platform_type_list = self.m_conf.get_supported_platforms(connector_type)

        if len(platform_type_list) == 1:
            platform_type = platform_type_list[0]
        else:
            platform_type = prompt.Prompt.ask(f"Specify [bold green]platform_type[/bold green]",
                                              choices=platform_type_list)

        # step 5. assign platform type and platform attributes
        if platform_type != 'local':
            predefined_values.update({'platform_type': platform_type})
            platform_attributes = self.m_conf.get_platform_attributes(platform_id=platform_type)
            assigned_platform_attributes = self.cli_menu.assign_attributes(platform_attributes, predefined_values=predefined_values, exclude_key_list=['connector_type'])
            self.composed_pipeline_config['pipeline'][connection_attributes_name].update(assigned_platform_attributes)

    def _prompt_for_schema_if_needed(self, connection_attributes_name, connection_name):
        """
        Prompts for database_schema/dataset if the connector supports it.
        Also prompts for file connector-specific attributes (source_dir/destination_dir).
        Schema/dataset and file directories are pipeline-specific and should be configured in pipeline.yaml,
        not in connections.yaml.
        
        Args:
            connection_attributes_name: 'source_attributes' or 'target_attributes'
            connection_name: Name of the connection to check
        """
        # Load connection to check connector type
        # Priority: 1) connections_file_path from config, 2) default: pipeline_dir_path/connections.yaml
        if hasattr(self.m_conf, 'connections_file_path') and self.m_conf.connections_file_path:
            connections_fpath = self.m_conf.connections_file_path
        else:
            connection_format = self.m_conf.global_settings.get('connections_file_format', 'yaml')
            connections_fname = f'connections.{connection_format}'
            connections_fpath = os.path.join(self.m_conf.pipeline_base_dpath, connections_fname)
        
        if not self.f_handler.is_file(connections_fpath):
            return
        
        # Load connection configuration
        file_extension = os.path.splitext(connections_fpath)[1].lower()
        if file_extension == '.yaml':
            connections_config = self.f_handler.load_yaml(connections_fpath)
        else:
            connections_config = self.f_handler.load_json(connections_fpath)
        
        connections_dict = connections_config.get('connections', {})
        connection_config = connections_dict.get(connection_name)
        
        if not connection_config:
            return
        
        connector_type = connection_config.get('connector_type')
        if not connector_type:
            return
        
        # Prompt for file connector-specific attributes if connector is CSV, Parquet, or JSON
        # This should be done BEFORE checking for database_schema, as file connectors don't have schema
        if connector_type in ('csv', 'parquet', 'json'):
            self.console.print(f"\n[bold]Configure {connection_attributes_name.replace('_', ' ').title()}[/bold]")
            if connection_attributes_name == 'source_attributes':
                # Prompt for source_dir when file connector is source
                connector_name = connector_type.upper()
                self.console.print(f"\n[bold]Configure {connector_name} Source Directory[/bold]")
                source_dir = prompt.Prompt.ask(
                    f"Enter [bold yellow]source_dir[/bold yellow] (absolute path to source {connector_name} files directory)",
                    default=None
                )
                if source_dir:
                    self.composed_pipeline_config['pipeline'][connection_attributes_name]['source_dir'] = source_dir
            elif connection_attributes_name == 'target_attributes':
                # Prompt for destination_dir when file connector is target
                connector_name = connector_type.upper()
                self.console.print(f"\n[bold]Configure {connector_name} Target Directory[/bold]")
                destination_dir = prompt.Prompt.ask(
                    f"Enter [bold yellow]destination_dir[/bold yellow] (absolute path to destination directory)",
                    default=None
                )
                if destination_dir:
                    self.composed_pipeline_config['pipeline'][connection_attributes_name]['destination_dir'] = destination_dir
            return  # File connectors don't have database_schema, so return after prompting for directories
        
        # Check if connector supports database_schema (for database connectors)
        connector_attributes = self.m_conf.get_connector_attributes(connector_type)
        if 'database_schema' not in connector_attributes:
            # Connector doesn't support schema (e.g., MySQL)
            return
        
        # Prompt for schema/dataset with connector-specific messaging
        self.console.print(f"\n[bold]Configure {connection_attributes_name.replace('_', ' ').title()}[/bold]")
        
        # Determine the appropriate prompt message based on connector type
        if connector_type == 'bigquery':
            prompt_message = f"Enter [bold yellow]database_schema[/bold yellow] (dataset name) for {connection_attributes_name}"
        elif connector_type in ['postgres', 'redshift']:
            prompt_message = f"Enter [bold yellow]database_schema[/bold yellow] for {connection_attributes_name}"
        else:
            # For other connectors that support schema
            prompt_message = f"Enter [bold yellow]database_schema[/bold yellow] for {connection_attributes_name}"
        
        schema = prompt.Prompt.ask(
            prompt_message,
            default=None
        )
        
        if schema:
            self.composed_pipeline_config['pipeline'][connection_attributes_name]['database_schema'] = schema
    
    def _prompt_for_bucket_prefix_if_needed(self, connection_attributes_name, connection_name, pipeline_name):
        """
        Prompts for bucket_pipeline_prefix if the connector needs it.
        bucket_pipeline_prefix is pipeline-specific and should be configured in pipeline.yaml,
        not in connections.yaml.
        
        Args:
            connection_attributes_name: 'source_attributes' or 'target_attributes'
            connection_name: Name of the connection to check (None if using inline attributes)
            pipeline_name: Name of the pipeline (for default value)
        """
        connector_type = None
        
        if connection_name:
            # Using connections - load connection config to get connector type
            # Priority: 1) connections_file_path from config, 2) default: pipeline_dir_path/connections.yaml
            if hasattr(self.m_conf, 'connections_file_path') and self.m_conf.connections_file_path:
                connections_fpath = self.m_conf.connections_file_path
            else:
                connection_format = self.m_conf.global_settings.get('connections_file_format', 'yaml')
                connections_fname = f'connections.{connection_format}'
                connections_fpath = os.path.join(self.m_conf.pipeline_base_dpath, connections_fname)
            
            if not self.f_handler.is_file(connections_fpath):
                return
            
            # Load connection configuration
            file_extension = os.path.splitext(connections_fpath)[1].lower()
            if file_extension == '.yaml':
                connections_config = self.f_handler.load_yaml(connections_fpath)
            else:
                connections_config = self.f_handler.load_json(connections_fpath)
            
            connections_dict = connections_config.get('connections', {})
            connection_config = connections_dict.get(connection_name)
            
            if connection_config:
                connector_type = connection_config.get('connector_type')
        else:
            # Using inline attributes - get connector type from composed config
            connector_type = self.composed_pipeline_config['pipeline'][connection_attributes_name].get('connector_type')
        
        if not connector_type:
            return
        
        # Check if connector needs bucket_pipeline_prefix (Redshift, BigQuery, S3, GCS)
        connectors_needing_bucket_prefix = ['redshift', 'bigquery', 's3', 'gs']
        if connector_type not in connectors_needing_bucket_prefix:
            return
        
        # Check if connector has bucket_pipeline_prefix in its attributes
        connector_attributes = self.m_conf.get_connector_attributes(connector_type)
        if 'bucket_pipeline_prefix' not in connector_attributes:
            return
        
        # Get default value from connector attributes
        bucket_prefix_config = connector_attributes.get('bucket_pipeline_prefix', {})
        default_value = bucket_prefix_config.get('default_value', 'petaly/{pipeline_name}')
        # Keep {pipeline_name} placeholder as-is - it will be replaced automatically during execution
        
        # Prompt for bucket_pipeline_prefix
        self.console.print(f"\n[bold]Configure Bucket Pipeline Prefix for {connection_attributes_name.replace('_', ' ').title()}[/bold]")
        bucket_prefix = prompt.Prompt.ask(
            f"Enter [bold yellow]bucket_pipeline_prefix[/bold yellow] (path prefix in bucket for pipeline objects, use {{pipeline_name}} placeholder)",
            default=default_value
        )
        
        # Always save the value (use default if user didn't provide one)
        # This ensures bucket_pipeline_prefix exists in both source_attributes and target_attributes for connectors that need it
        # Keep {pipeline_name} placeholder as-is - it will be replaced automatically during execution
        self.composed_pipeline_config['pipeline'][connection_attributes_name]['bucket_pipeline_prefix'] = bucket_prefix if bucket_prefix else default_value

    def compose_load_attributes(self):
        """
        Composes load attributes including default object settings and data objects spec mode.
        """
        load_attributes = self.pipeline_meta_config.get('load_attributes')

        # Use default csv_default_settings from skeleton (no user prompts)
        # Defaults: header: true, columns_delimiter: ',', columns_quote: 'double', type_autodetection: true, null_string: '', force_null: false
        default_csv_settings = {
            "header": True,
            "columns_delimiter": ",",
            "columns_quote": "double",
            "type_autodetection": True,
            "null_string": "",
            "force_null": False
        }
        self.composed_pipeline_config['pipeline']['load_attributes'].update({"csv_default_settings": default_csv_settings})

        self.console.print(f"\n[bold]{self.break_line}[/bold]")
        self.console.print(f"[bold]Specify load attributes[/bold]")

        assigned_load_attributes = self.cli_menu.assign_attributes(load_attributes, predefined_values=None, exclude_key_list=[None])
        self.composed_pipeline_config['pipeline']['load_attributes'].update(assigned_load_attributes)

    def compose_object_spec(self, pipeline, object_name, use_pipeline_wizard):
        """
        Composes object specification for data objects.
        
        Args:
            pipeline: Pipeline instance
            object_name: Name of the object
            use_pipeline_wizard: Whether to use wizard mode
        
        Returns:
            Dictionary with object_spec containing the composed attributes
        """
        self.cli_menu.use_pipeline_wizard = use_pipeline_wizard
        data_objects_spec = self.pipeline_meta_config.get('data_objects_spec')
        exclude_key_list = []
        
        # Get source connector type and category
        source_connector_type = pipeline.source_attr.get('connector_type')
        if not source_connector_type:
            logger.warning("Source connector type is not specified in the pipeline")
            return None
            
        connector_category = self.m_conf.get_connector_category(source_connector_type)
        if not connector_category:
            logger.warning(f"Could not determine connector category for source connector type: {source_connector_type}")
            return None

        # exclude params for file load (csv, etc..)
        if connector_category == 'database':
            exclude_key_list.append('object_source_dir')
            exclude_key_list.append('file_names')

        if object_name is None:
            object_name = self.cli_menu.force_assign_value(
                key='object_name',
                message=data_objects_spec.get('object_name').get('key_comment')
            )

        exclude_key_list.append('object_name')
        predefined_values = {'object_name': object_name}

        assigned_attributes = self.cli_menu.assign_attributes(data_objects_spec, predefined_values=predefined_values, exclude_key_list=exclude_key_list)
        tmp_assigned_attributes = {}
        tmp_assigned_attributes.update({'object_spec': assigned_attributes})

        return tmp_assigned_attributes
    
    def get_composed_config(self):
        """
        Returns the composed pipeline configuration.
        
        Returns:
            Dictionary containing the composed pipeline configuration
        """
        return self.composed_pipeline_config

