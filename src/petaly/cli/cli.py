# Copyright © 2024-2025 Pavel Rabaev
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
import sys
import os
from typing import Optional

#from petaly.core.logger import setup_logging

from rich.console import Console

from petaly.cli.cli_initializer import CliInitializer
from petaly.cli.cli_visualizer import CliVisualizer
from petaly.cli.cli_cleanup import CliCleanup
from petaly.core.main_ctl import MainCtl
from petaly.core.pipeline import Pipeline
from petaly.sysconfig.main_config import MainConfig

logger = logging.getLogger(__name__)

class Cli():

    def __init__(self, main_config: Optional[MainConfig] = None) -> None:
        """
        Initialize the CLI interface.
        
        Args:
            main_config: Optional main configuration instance
        """
        self.main_config = main_config
        self.console = Console()
        self.mode_message = (
            f"Type one of the following top level positional arguments: show, init, run, cleanup; followed by options below."
            f"\nUse -h for help"
        )

        self.main_config_file_message = (
            f"To initialize config file for the first time, provide the absolute path to petaly config file: init -c /ABSOLUTE_PATH_TO_PETALY_CONFIG_DIR/petaly.ini\n"
            f"Or simply run 'init' to create it in ~/.petaly/petaly.ini\n"
            f"To skip '-c' argument at runtime, set an environment variable: export PETALY_CONFIG_DIR=/ABSOLUTE_PATH_TO_PETALY_CONFIG_DIR\n"
        )

        self._setup_parser()

    def _setup_parser(self) -> None:
        """Setup the argument parser with all commands and options."""
        self.parser = argparse.ArgumentParser(
            description="Petaly CLI - Data Pipeline Management System",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Available commands:
  show        Show pipelines and workspace
  init        Initialize workspace or pipeline
  run         Run pipeline operations
  cleanup     Cleanup pipeline objects
            """
        )

        # Required arguments
        self.parser.add_argument(
            'command',
            choices=['show', 'init', 'run', 'cleanup'],
            help='Command to execute'
        )

        # Optional arguments
        self.parser.add_argument(
            '-w', '--workspace',
            action="store_true",
            help='Initialize workspace (required once after installation)'
        )
        self.parser.add_argument(
            '-p', '--pipeline_name',
            help='Pipeline name to operate on'
        )
        self.parser.add_argument(
            '-o', '--object_name',
            help='Object name or comma-separated list (requires -p)'
        )
        self.parser.add_argument(
            '-c', '--config_file_path',
            nargs='?',
            type=str,
            help=self.main_config_file_message
        )
        self.parser.add_argument(
            '-s', '--source_only',
            action='store_true',
            help='Extract data from source only'
        )
        self.parser.add_argument(
            '-t', '--target_only',
            action='store_true',
            help='Load data to target only'
        )

        self.parser.set_defaults(func=self.process_p)

    def process_p(self, args: argparse.Namespace) -> None:
        """
        Process pipeline commands.
        
        Args:
            args: Parsed command line arguments
            
        Raises:
            SystemExit: If command execution fails
        """

        if args.command == 'show':
            self.show_p(args)
        elif args.command == 'init':
            self.init_p(args)
        elif args.command == 'run':
            self.run_p(args)
        elif args.command == 'cleanup':
            self.cleanup_p(args)
        else:
            self.parser.print_help()
            sys.exit(1)

    def init_p(self, args):
        """
        """
        m_conf = MainConfig() if self.main_config == None else self.main_config
        m_conf.set_main_config_fpath(args.config_file_path, init_main_config = True)
        self.console.print(f"The following main config file is used: {m_conf.main_config_fpath}")
        m_conf.set_global_settings()
        m_conf.set_workspace_dpaths()

        initialize = CliInitializer(m_conf)

        if args.workspace:
            initialize.init_workspace()
        elif args.pipeline_name:
            if args.object_name:
                initialize.init_data_objects(args.pipeline_name, args.object_name)
            else:
                initialize.init_pipeline(args.pipeline_name)
        else:
            self.console.print('Provide one of the following sub arguments: --workspace, --pipeline_name, --pipeline_name + --object_name. Use -h for help')

    def show_p(self, args):
        """
        """
        m_conf = MainConfig() if self.main_config == None else self.main_config
        m_conf.set_main_config_fpath(args.config_file_path)
        m_conf.set_global_settings()
        m_conf.set_workspace_dpaths()

        visualize = CliVisualizer(m_conf)

        if args.workspace:
            visualize.show_workspace()
        elif args.pipeline_name:
            if args.pipeline_name is None or args.pipeline_name == '':
                self.console.print("Attribute pipeline name has to be specified: --pipeline_name your_pipeline_name")
                sys.exit()
            visualize.show_pipeline(args.pipeline_name)
        else:
            visualize.show_pipelines()


    def run_p(self, args):
        """
        """
        m_conf = MainConfig() if self.main_config == None else self.main_config

        m_conf.set_main_config_fpath(args.config_file_path)
        m_conf.set_global_settings()
        m_conf.set_workspace_dpaths()
        
        initialize = CliInitializer(m_conf)
        initialize.init_workspace(skip_message_if_exist=True)

        if args.pipeline_name:

            main_ctl = MainCtl(m_conf)
            pipeline = Pipeline(args.pipeline_name, m_conf)

            if self.are_endpoints_identical(pipeline):
                self.console.print(f"In the pipeline {args.pipeline_name} source_attributes and target_attributes are exactly the same. To avoid accidentally recreating the same tables, specify at least a different schema or database name.")
                sys.exit()

            if pipeline:
                run_endpoint = None
                if args.source_only:
                    run_endpoint = 'source'
                    self.console.print(f"Run source only")
                elif args.target_only:
                    run_endpoint = 'target'
                    self.console.print(f"Run target only")
                main_ctl.run_pipeline(pipeline, run_endpoint, args.object_name)

        else:

            self.console.print('Provide -p pipeline name. Check exiting pipelines below')
            sys.exit()

    def are_endpoints_identical(self, pipeline):
        """
        """
        identical_attributes = False
        m_conf = MainConfig() if self.main_config == None else self.main_config

        source_category = m_conf.get_connector_class_config(pipeline.source_attr.get('connector_type')).get('connector_category')
        target_category = m_conf.get_connector_class_config(pipeline.target_attr.get('connector_type')).get('connector_category')

        if source_category == 'database':
            if source_category == target_category:
                for key in pipeline.source_attr.keys():
                    if key not in ('platform_type','database_password'):
                        if str(pipeline.target_attr.get(key)) == str(pipeline.source_attr.get(key)):
                            identical_attributes = True
                        else:
                            return False

        return identical_attributes

    def cleanup_p(self, args):
        """
        """
        m_conf = MainConfig() if self.main_config == None else self.main_config

        m_conf.set_main_config_fpath(args.config_file_path)
        m_conf.set_global_settings()
        m_conf.set_workspace_dpaths()

        cleanup = CliCleanup(m_conf)

        if args.pipeline_name:
            if args.object_name:
                cleanup.cleanup_data_objects(args.pipeline_name, args.object_name)
            else:
                self.exit_with_help(f"Cleanup of pipeline is not supported yet. "
                                    f"To remove an object from specific pipeline provide additionaly --object_name: object_name.")

    def exit_with_help(self, message):
        """
        """
        self.parser.print_help()
        self.console.print('\n'+message+'\n')
        m_conf = MainConfig() if self.main_config == None else self.main_config
        visualize = CliVisualizer(m_conf)
        visualize.show_pipelines()
        sys.exit()

    def start(self) -> None:
        """Start the CLI interface."""
        try:
            # If no arguments provided, show full help
            if len(sys.argv) == 1:
                self.parser.print_help()
                return

            # Normal CLI mode
            args = self.parser.parse_args()
            logger.debug(f"Executing command with args: {args}")
            self._validate_args(args)
            args.func(args)
        except Exception as e:
            logger.error(f"Error executing command: {e}", exc_info=True)
            raise

    def _validate_args(self, args: argparse.Namespace) -> None:
        """Validate command line arguments."""

        #if args.command == 'init' and not args.workspace and not args.pipeline_name:
        #    self.parser.error("init requires either --workspace or --pipeline_name")
        
        if args.object_name and not args.pipeline_name:
            self.parser.error("--object_name requires --pipeline_name")
        
        if args.source_only and args.target_only:
            self.parser.error("Cannot specify both --source_only and --target_only")

