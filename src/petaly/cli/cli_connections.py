# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import os
import sys
import logging
from rich.console import Console
from rich.table import Table
import rich.prompt as prompt

from petaly.utils.file_handler import FileHandler

logger = logging.getLogger(__name__)


class CliConnections:
    """
    CLI handler for managing connections configuration.
    
    Provides functionality to:
    - Initialize/create connections.yaml file
    - List all connections
    - Create new connections
    - Show connection details
    """
    
    def __init__(self, main_config, cli_menu):
        """
        Initialize CliConnections instance.
        
        Args:
            main_config: MainConfig instance
            cli_menu: CliMenu instance (composer) with common utilities
        """
        self.m_conf = main_config
        self.cli_menu = cli_menu
        self.console = cli_menu.console
        self.f_handler = FileHandler()
        self.break_line = cli_menu.break_line
        
        # Get connections file path
        # Priority: 1) connections_file_path from config, 2) default: pipeline_dir_path/connections.yaml
        if hasattr(self.m_conf, 'connections_file_path') and self.m_conf.connections_file_path:
            # Use explicitly specified connections file path
            self.connections_fpath = self.m_conf.connections_file_path
        else:
            # Default: use pipeline_dir_path/connections.yaml
            connection_format = self.m_conf.global_settings.get('connections_file_format', 'yaml')
            self.connections_fname = f'connections.{connection_format}'
            self.connections_fpath = os.path.join(self.m_conf.pipeline_base_dpath, self.connections_fname)
    
    def init_connections(self):
        """
        Initialize or create connections.yaml file.
        """
        self.console.print(f"\n[bold]{self.break_line}[/bold]")
        self.console.print(f"[bold]Initialize Connections Configuration[/bold]\n")
        
        # Check if connections file already exists
        if self.f_handler.is_file(self.connections_fpath):
            self.console.print(f"[yellow]File exists:[/yellow] {self.connections_fpath}")
            connections = self.get_all_connections()
            if connections:
                self.console.print(f"\nExisting connections:")
                for connection_name in sorted(connections.keys()):
                    self.console.print(f"  • {connection_name}")
            self.console.print("\nTo modify or create a specific connection, use:")
            self.console.print("  [bold]petaly init -n <connection_name>[/bold]")
            return
        
        # Create connections file from skeleton
        connection_format = self.m_conf.global_settings.get('connections_file_format', 'yaml')
        connections_skeleton = self.f_handler.load_json(self.m_conf.connections_skeleton_fpath)
        self.f_handler.save_dict_to_file(
            self.connections_fpath,
            connections_skeleton,
            file_format=connection_format
        )
        
        self.console.print(f"\n[green]✓[/green] Created connections file at: {self.connections_fpath}")
        self.console.print("\nYou can now add connections using: [bold]petaly init -n <name>[/bold]")
    
    def create_connection(self, connection_name=None):
        """
        Create or modify an connection interactively.
        
        Connections can be used as both source and target - the type is determined
        when the connection is used in a pipeline configuration.
        
        Args:
            connection_name: Optional name for the connection (if None, will prompt)
        """
        self.console.print(f"\n[bold]{self.break_line}[/bold]")
        
        # Ensure connections file exists
        if not self.f_handler.is_file(self.connections_fpath):
            self.console.print(f"[yellow]Connections file not found. Creating it first...[/yellow]")
            self.init_connections()
        
        # Get connection name
        if not connection_name:
            connection_name = self.cli_menu.force_assign_value(
                key='connection_name',
                message="Enter a name for this connection"
            )
        
        # Check if connection already exists
        existing_connections = self.get_all_connections()
        if connection_name in existing_connections:
            self.console.print(f"[bold]Modify Connection: {connection_name}[/bold]\n")
            self.console.print("Current configuration:")
            current_connection = existing_connections[connection_name]
            for key, value in sorted(current_connection.items()):
                if key == 'database_password':
                    display_value = "***"
                else:
                    display_value = value if value is not None else "None"
                self.console.print(f"  [cyan]{key}:[/cyan] {display_value}")
            self.console.print("")  # Empty line before modification
        else:
            self.console.print(f"[bold]Create New Connection: {connection_name}[/bold]\n")
        
        # Compose connection attributes (no type needed - connections are generic)
        connection_attributes = self.cli_menu.compose_connection_attributes_dict()
        
        # Save connection
        self.cli_menu.save_connection_to_file(
            self.connections_fpath,
            connection_name,
            connection_attributes
        )
        
        action = "modified" if connection_name in existing_connections else "created"
        self.console.print(f"\n[green]✓[/green] Connection '{connection_name}' {action} successfully!")
    
    def list_connections(self):
        """
        List all available connections in a formatted table.
        """
        connections = self.get_all_connections()
        
        if not connections:
            self.console.print(f"\n[yellow]No connections found in {self.connections_fpath}[/yellow]")
            self.console.print("Create connections using: [bold]petaly init -n <name>[/bold]")
            return
        
        self.console.print(f"\n[bold]{self.break_line}[/bold]")
        self.console.print(f"[bold]Available Connections[/bold]\n")
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Connection Name", style="cyan", no_wrap=True)
        table.add_column("Connector Type", style="green")
        table.add_column("Platform", style="yellow")
        table.add_column("Key Details", style="white")
        
        for name, attrs in sorted(connections.items()):
            connector_type = attrs.get('connector_type', 'N/A')
            platform = attrs.get('platform_type', 'local')
            
            # Build key details string
            details = []
            if connector_type in ['postgres', 'mysql', 'redshift']:
                host = attrs.get('database_host', '')
                db = attrs.get('database_name', '')
                if host:
                    details.append(f"host: {host}")
                if db:
                    details.append(f"db: {db}")
            elif connector_type == 'bigquery':
                project = attrs.get('gcp_project_id', '')
                if project:
                    details.append(f"project: {project}")
            elif connector_type in ['gcs', 's3']:
                bucket = attrs.get('bucket_name', '')
                if bucket:
                    details.append(f"bucket: {bucket}")
            
            details_str = ", ".join(details[:2]) if details else "N/A"
            
            table.add_row(name, connector_type, platform, details_str)
        
        self.console.print(table)
        self.console.print(f"\nTotal connections: {len(connections)}")
    
    def show_connection(self, connection_name):
        """
        Show detailed information about a specific connection.
        
        Args:
            connection_name: Name of the connection to show
        """
        connections = self.get_all_connections()
        
        if connection_name not in connections:
            self.console.print(f"[red]Error:[/red] Connection '{connection_name}' not found")
            
            if not self.f_handler.is_file(self.connections_fpath):
                self.console.print(f"\n[yellow]Note:[/yellow] Connections file not found at: {self.connections_fpath}")
                self.console.print("Create connections using: [bold]petaly init -n <connection_name>[/bold]")
            elif not connections:
                self.console.print(f"\n[yellow]Note:[/yellow] No connections found in {self.connections_fpath}")
                self.console.print("Create connections using: [bold]petaly init -n <connection_name>[/bold]")
            else:
                self.console.print("\nAvailable connections:")
                for name in sorted(connections.keys()):
                    self.console.print(f"  - {name}")
            return
        
        connection = connections[connection_name]
        
        self.console.print(f"\n[bold]{self.break_line}[/bold]")
        self.console.print(f"[bold]Connection: {connection_name}[/bold]\n")
        
        # Display connection attributes
        for key, value in sorted(connection.items()):
            if key == 'database_password':
                display_value = "***" if value else "None"
            else:
                display_value = value if value is not None else "None"
            self.console.print(f"  [cyan]{key}:[/cyan] {display_value}")
    
    def get_all_connections(self):
        """
        Get all connections from the configuration file.
        
        Returns:
            Dictionary of connection names to attributes
        """
        if not self.f_handler.is_file(self.connections_fpath):
            logger.debug(f"Connections file not found at {self.connections_fpath}")
            return {}
        
        try:
            connection_format = self.m_conf.global_settings.get('connections_file_format', 'yaml')
            file_extension = os.path.splitext(self.connections_fpath)[1].lower()
            
            if file_extension == '.yaml':
                connections_config = self.f_handler.load_yaml(self.connections_fpath)
            elif file_extension == '.json':
                connections_config = self.f_handler.load_json(self.connections_fpath)
            else:
                logger.warning(f"Unsupported connections file format: {file_extension}")
                return {}
            
            if not connections_config:
                logger.debug(f"Connections config is empty in {self.connections_fpath}")
                return {}
            
            # Handle both flat structure and nested structure
            if isinstance(connections_config, dict):
                connections_dict = connections_config.get('connections', connections_config)
                # If connections_dict is still a dict with 'connections' key, extract it
                if isinstance(connections_dict, dict) and 'connections' in connections_dict:
                    connections_dict = connections_dict['connections']
                return connections_dict if isinstance(connections_dict, dict) else {}
            
            return {}
        except Exception as e:
            logger.error(f"Error loading connections from {self.connections_fpath}: {e}", exc_info=True)
            return {}

