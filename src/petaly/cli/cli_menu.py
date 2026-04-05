# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import rich.prompt as prompt
from rich.console import Console
import logging
import os

from petaly.utils.file_handler import FileHandler
from collections import Counter, OrderedDict

logger = logging.getLogger(__name__)

class OrderedCounter(Counter, OrderedDict):
    'Counter that remembers the order elements are first seen'
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__,
                           OrderedDict(self))

    def __reduce__(self):
        return self.__class__, (OrderedDict(self),)

class CliMenu():
    """
    CLI Composer/Controller - orchestrates pipeline and connection CLI operations.
    
    Acts as a facade that composes and coordinates:
    - CliPipeline: Pipeline configuration composition
    - CliConnections: Connection management
    - Common utilities: Value assignment, attribute assignment, dependency checking
    
    This class serves as the main entry point for CLI composition operations.
    """

    def __init__(self, main_config):
        # Set use_pipeline_wizard based on full_pipeline_wizard from config
        full_pipeline_wizard = main_config.global_settings.get('full_pipeline_wizard', 'true')
        self.use_pipeline_wizard = full_pipeline_wizard.lower() == 'true'
        self.console = Console()
        self.prompt = prompt
        self.m_conf = main_config
        self.f_handler = FileHandler()
        self.break_line = '------------------------------------------------------------------'
        
        # Compose CLI components
        from petaly.cli.cli_pipeline import CliPipeline
        from petaly.cli.cli_connections import CliConnections
        
        self.cli_pipeline = CliPipeline(main_config, self)
        self.cli_connections = CliConnections(main_config, self)
        
    def force_assign_value(self, key, message):
        while True:
            self.console.print(message)
            value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]")
            message = f"[red]{message}[/red]"
            if value.strip() != '':

                break
        return value

    def assign_attributes(self, spec_attributes, predefined_values=None, exclude_key_list=None) -> dict:
        assigned_attributes = {}

        if predefined_values is not None:
            assigned_attributes.update(predefined_values)

        # cleanup dict from exclude keys
        if exclude_key_list is not None:
            for key in exclude_key_list:
                if self.f_handler.check_dict_key_exist(spec_attributes, key):
                    spec_attributes.pop(key)

        for key, value in spec_attributes.items():

            in_use = False if value.get('in_use') is None else value.get('in_use')

            if in_use:
                # 1. handle predefined_values
                if predefined_values is not None:
                    if self.f_handler.check_dict_key_exist(predefined_values, key):
                        assigned_value = predefined_values.get(key)
                        assigned_attributes.update({key: assigned_value})
                        continue

                # 2. define default and preassigned_values
                preassigned_values = value.get('preassigned_values')
                default_value =  None if value.get('default_value') is None else value.get('default_value')
                preassigned_values = None if preassigned_values[0] is None else preassigned_values
                assigned_value = default_value

                # 3. check dependency
                if not self.include_based_on_dependency(assigned_attributes, value.get('dependency')):
                    continue

                # 4. Check wizard_required: if use_pipeline_wizard is False, only ask if wizard_required is True
                wizard_required = value.get('wizard_required', False)
                if not self.use_pipeline_wizard and not wizard_required:
                    # Short form mode and this field is not required - use default value
                    assigned_attributes.update({key: assigned_value})
                    continue

                # 5. compose key comment and default value
                console_message = "\n"

                console_message += f"{value.get('key_comment')}"
                if default_value is not None:
                    console_message += f"Default: [bold blue]{default_value}[/bold blue]"

                self.console.print(console_message)

                if key == 'database_password':
                    if self.use_pipeline_wizard or wizard_required:
                        assigned_value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]", password=True)

                elif value.get('key_type') == 'Integer':
                    if self.use_pipeline_wizard or wizard_required:
                        assigned_value = prompt.IntPrompt.ask(f"[bold green]{key}[/bold green]", default=default_value,
                                                              show_default=False)

                elif value.get('key_type') == 'Array':

                    if self.use_pipeline_wizard or wizard_required:
                        assigned_value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]", default=default_value,
                                                           show_default=False)

                        if type(assigned_value) == str:
                            assigned_value = [item.strip() for item in assigned_value.split(',')]

                    if assigned_value is None:
                        assigned_value = [None]
                else:
                    if self.use_pipeline_wizard or wizard_required:
                        # For String types, only pass choices if preassigned_values is not None
                        # If preassigned_values is None, prompt without choices (free text input)
                        if preassigned_values is not None:
                            assigned_value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]", choices=preassigned_values,
                                                               default=default_value, show_default=False)
                        else:
                            assigned_value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]",
                                                               default=default_value, show_default=False)

                    if value.get('key_type') == 'Boolean':
                        assigned_value = True if assigned_value == 'true' else False

                assigned_attributes.update({key: assigned_value})

        return assigned_attributes

    def include_based_on_dependency(self, assigned_attributes, dependency_dict):

        include = True
        if dependency_dict is None or dependency_dict == {}:
            return include

        for key, value in dependency_dict.items():
            if assigned_attributes.get(key) == value:
                include = True
            else:
                return False

        return include
    
    def _select_connection_from_list(self, connection_type, existing_connection_names, max_num, connections_fpath):
        """
        Selects an connection from the already-displayed list.
        
        Args:
            connection_type: 'source' or 'target'
            existing_connection_names: List of existing connection names
            max_num: Maximum number in the list
            connections_fpath: Path to connections.yaml/json file
        
        Returns:
            Name of the selected/created connection
        """
        # Allow selection by number or name (without showing list again)
        while True:
            selection = prompt.Prompt.ask(
                f"\nSelect connection for [bold]{connection_type}[/bold] (enter number 0-{max_num} or connection name)"
            )
            
            # Check for option 0 (create new connection)
            if selection == '0':
                # Create new connection
                connection_name = self.force_assign_value(
                    key=f'{connection_type}_connection_name',
                    message=f"Enter a unique name for the {connection_type} connection"
                )
                
                # Compose connection attributes
                connection_attributes = self.compose_connection_attributes_dict()
                                # Save connection to connections.yaml
                self.save_connection_to_file(connections_fpath, connection_name, connection_attributes)
                
                return connection_name
            
            # Try to parse as number
            try:
                num = int(selection)
                if 1 <= num <= max_num:
                    connection_name = existing_connection_names[num - 1]
                    return connection_name
                else:
                    self.console.print(f"[red]Invalid number. Please enter a number between 0 and {max_num}[/red]")
            except ValueError:
                # Not a number, try as connection name
                if selection in existing_connection_names:
                    return selection
                else:
                    self.console.print(f"[red]connection '{selection}' not found. Please enter a valid number or connection name.[/red]")
    
    def select_or_create_connection(self, connection_type, connections_fpath, skip_list=False):
        """
        Allows user to select an existing connection or create a new one.
        
        Args:
            connection_type: 'source' or 'target'
            connections_fpath: Path to connections.yaml/json file
            skip_list: If True, skip showing the list (used when list was already shown)
        
        Returns:
            Name of the selected/created connection
        """
        # Load existing connections
        connections_config = self.f_handler.load_json(self.m_conf.connections_skeleton_fpath)
        if self.f_handler.is_file(connections_fpath):
            file_extension = os.path.splitext(connections_fpath)[1].lower()
            if file_extension == '.yaml':
                existing_connections = self.f_handler.load_yaml(connections_fpath)
            else:
                existing_connections = self.f_handler.load_json(connections_fpath)
            
            if existing_connections:
                connections_config = existing_connections
        
        # Get all existing connections (flat structure)
        if 'connections' in connections_config:
            connections_dict = connections_config['connections']
        else:
            connections_dict = connections_config
        existing_connection_names = list(connections_dict.keys())
        
        self.console.print(f"\n[bold]{self.break_line}[/bold]")
        self.console.print(f"Configure [bold]{connection_type}[/bold] connection\n")
        
        # First ask if user wants to use an existing connection
        if existing_connection_names:
            use_existing = prompt.Confirm.ask(f"Do you want to use an existing connection for [bold]{connection_type}[/bold]?", default=True)
            
            if use_existing:
                # Show all existing connections (one per line) with option 0 to create new
                self.console.print(f"\nExisting connections:")
                self.console.print(f"  0. [bold cyan]Create new connection[/bold cyan]")
                for idx, connection_name in enumerate(existing_connection_names, 1):
                    self.console.print(f"  {idx}. {connection_name}")
                
                # Allow selection by number or name (without showing all choices in one line)
                while True:
                    selection = prompt.Prompt.ask(
                        f"\nSelect connection for [bold]{connection_type}[/bold] (enter number 0-{len(existing_connection_names)} or connection name)"
                    )
                    
                    # Check for option 0 (create new connection)
                    if selection == '0':
                        # Break out to create new connection
                        break
                    
                    # Try to parse as number
                    try:
                        num = int(selection)
                        if 1 <= num <= len(existing_connection_names):
                            connection_name = existing_connection_names[num - 1]
                            return connection_name
                        else:
                            self.console.print(f"[red]Invalid number. Please enter a number between 0 and {len(existing_connection_names)}[/red]")
                    except ValueError:
                        # Not a number, try as connection name
                        if selection in existing_connection_names:
                            return selection
                        else:
                            self.console.print(f"[red]connection '{selection}' not found. Please enter a valid number or connection name.[/red]")
        
        # User wants to create a new connection (or no existing connections, or selected 0)
        if existing_connection_names:
            self.console.print(f"\nCreating new {connection_type} connection...")
        else:
            self.console.print(f"\nNo existing connections found. Creating new {connection_type} connection...")
        
        connection_name = self.force_assign_value(
            key=f'{connection_type}_connection_name',
            message=f"Enter a unique name for the {connection_type} connection"
        )
        
        # Compose connection attributes (similar to compose_connection_attributes but return dict)
        connection_attributes = self.compose_connection_attributes_dict()
        
        # Save connection to connections.yaml
        self.save_connection_to_file(connections_fpath, connection_name, connection_attributes)
        
        return connection_name
    
    def compose_connection_attributes_dict(self):
        """
        Composes connection attributes and returns as dictionary (for saving to connections.yaml).
        connections are generic and can be used as both source and target.
        Similar to compose_connection_attributes but returns dict instead of updating pipeline config.
        """
        connection_attributes_dict = {}
        predefined_values = {}
        
        # step 1. specify connector type
        available_connectors = self.m_conf.get_available_connectors()
        connector_type = prompt.Prompt.ask(
            f"Specify [bold yellow]connector type[/bold yellow]",
            choices=available_connectors
        )
        predefined_values.update({'connector_type': connector_type})
        
        # step 2. get connector category
        connector_category = self.m_conf.get_connector_class_config(connector_type).get('connector_category')
        
        # step 3. based on connector category define database or file parameters
        connector_attributes = self.m_conf.get_connector_attributes(connector_type)
        
        # Exclude pipeline-specific attributes from connections
        # connections should only contain connection details, not schema/dataset names, CSV directories, or bucket pipeline prefix
        # Schema/dataset, CSV source_dir/destination_dir, and bucket_pipeline_prefix are pipeline-specific and will be configured in pipeline.yaml
        # bucket_name stays in connections as it's part of the connection configuration
        exclude_key_list = ['database_schema', 'source_dir', 'destination_dir', 'bucket_pipeline_prefix']  # These belong to pipeline, not connection
        
        assigned_connector_attributes = self.assign_attributes(
            connector_attributes,
            exclude_key_list=exclude_key_list,
            predefined_values=predefined_values
        )
        connection_attributes_dict.update(assigned_connector_attributes)
        
        # step 4. get and define platform type
        platform_type_list = self.m_conf.get_supported_platforms(connector_type)
        
        if len(platform_type_list) == 1:
            platform_type = platform_type_list[0]
        else:
            platform_type = prompt.Prompt.ask(
                f"Specify [bold green]platform_type[/bold green]",
                choices=platform_type_list
            )
        
        # step 5. assign platform type and platform attributes
        if platform_type != 'local':
            connection_attributes_dict.update({'platform_type': platform_type})
            platform_attributes = self.m_conf.get_platform_attributes(platform_id=platform_type)
            assigned_platform_attributes = self.assign_attributes(
                platform_attributes,
                predefined_values={'platform_type': platform_type},
                exclude_key_list=['connector_type']
            )
            connection_attributes_dict.update(assigned_platform_attributes)

        endpoint_type = prompt.Prompt.ask(
            "Specify [bold green]endpoint_type[/bold green]",
            choices=['source', 'target'],
            default='source',
            show_default=False
        )
        connection_attributes_dict.update({'endpoint_type': endpoint_type})
        
        return connection_attributes_dict
    
    def save_connection_to_file(self, connections_fpath, connection_name, connection_attributes):
        """
        Saves an connection to the connections.yaml/json file.
        """
        # Load existing connections or create new structure
        connection_format = self.m_conf.global_settings.get('connections_file_format', 'yaml')
        
        if self.f_handler.is_file(connections_fpath):
            file_extension = os.path.splitext(connections_fpath)[1].lower()
            if file_extension == '.yaml':
                connections_config = self.f_handler.load_yaml(connections_fpath)
            else:
                connections_config = self.f_handler.load_json(connections_fpath)
        else:
            connections_config = self.f_handler.load_json(self.m_conf.connections_skeleton_fpath)
        
        # Ensure connections structure exists
        if 'connections' not in connections_config:
            connections_config = {'connections': connections_config}
        
        # Initialize connections dict if it doesn't exist
        if not isinstance(connections_config['connections'], dict):
            connections_config['connections'] = {}
        
        # Add or update connection (flat structure)
        connections_config['connections'][connection_name] = connection_attributes
        
        # Save back to file
        self.f_handler.save_dict_to_file(connections_fpath, connections_config, file_format=connection_format)
        self.console.print(f"\nConnection '{connection_name}' saved to {connections_fpath}")
