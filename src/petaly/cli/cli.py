# Copyright © 2024 Pavel Rabaev
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

#from petaly.core.logger import setup_logging

from rich.console import Console

from petaly.cli.cli_initializer import CliInitializer
from petaly.cli.cli_visualizer import CliVisualizer
from petaly.cli.cli_cleanup import CliCleanup
from petaly.core.main_ctl import MainCtl
from petaly.core.pipeline import Pipeline
from petaly.sysconfig.main_config import MainConfig


class Cli():

    def __init__(self, main_config=None):
        """
        """
        self.m_conf = MainConfig() if main_config == None else main_config
        self.console = Console()
        self.top_level_argument_message = (
                                f"Type one of the following top level positional arguments followed by optional argument: show, init, run, cleanup."
                                f"\nUse -h for help"
        )

        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('top_level_argument', choices=['show', 'init', 'run', 'cleanup'], help=self.top_level_argument_message)
        self.parser.add_argument('-w', '--workspace', action="store_true", help='Provide attribute --workspace. Check exiting workspace_name by show workspace_name')
        self.parser.add_argument('-p', '--pipeline_name', help='Provide pipeline name. Check exiting pipelines by show pipelines')
        self.parser.add_argument('-o', '--object_name', help='Provide object name or a comma-separated list without empty space. The pipeline name should be specified with -p paramater too.')
        self.parser.add_argument('-c', '--config_file_path', nargs='?', type=str, help=self.m_conf.get_petaly_config_path_message())
        self.parser.add_argument('-s', '--source_only', action='store_true', help='This parameter allow to run the extract from the source only.')
        self.parser.add_argument('-t', '--target_only', action='store_true', help='This parameter allow to run the load to the target only.')
        self.parser.set_defaults(func=self.process_p)


    def process_p(self, args):
        """
        """
        if args.top_level_argument == 'show':
            self.show_p(args)
        elif args.top_level_argument == 'init':
            self.init_p(args)
        elif args.top_level_argument == 'run':
            self.run_p(args)
        elif args.top_level_argument == 'cleanup':
            self.cleanup_p(args)
        else:
            #self.exit_with_help(args.config_file_path, self.top_level_argument_message)
            pass

    def init_p(self, args):
        """
        """
        self.m_conf.set_main_config_fpath(args.config_file_path)
        self.m_conf.set_base_dpaths()
        initialize = CliInitializer(self.m_conf)

        if args.workspace:
            initialize.init_workspace()
        elif args.pipeline_name:
            if args.object_name:
                initialize.init_data_objects(args.pipeline_name, args.object_name)
            else:
                initialize.init_pipeline(args.pipeline_name)
        else:
            self.console.print('Provide one of the following sub arguments: --workspace, --pipeline_name, --pipeline_name + --object_name. Use -h for help')
            #self.parser.print_help()

    def show_p(self, args):
        """
        """
        self.m_conf.set_main_config_fpath(args.config_file_path)
        self.m_conf.set_base_dpaths()

        visualize = CliVisualizer(self.m_conf)

        if args.workspace:
            visualize.show_workspace()
        elif args.pipeline_name:
            if args.pipeline_name is None or args.pipeline_name == '':
                self.console.print("Attribute pipeline name has to be specified: --pipeline_name your_pipeline_name")
                sys.exit()
            visualize.show_pipeline(args.pipeline_name)
        else:
            visualize.show_pipelines()
            self.parser.print_help()

    def run_p(self, args):
        """
        """

        self.m_conf.set_main_config_fpath(args.config_file_path)
        self.m_conf.set_base_dpaths()

        if args.pipeline_name:

            main_ctl = MainCtl(self.m_conf)
            pipeline = Pipeline(args.pipeline_name, self.m_conf)

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
            self.parser.print_help()
            self.console.print('Provide -p pipeline name. Check exiting pipelines below')
            sys.exit()

    def are_endpoints_identical(self, pipeline):
        """
        """
        identical_attributes = False

        source_category = self.m_conf.get_connector_class_config(pipeline.source_attr.get('endpoint_type')).get('connector_category')
        target_category = self.m_conf.get_connector_class_config(pipeline.target_attr.get('endpoint_type')).get('connector_category')

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
        self.m_conf.set_main_config_fpath(args.config_file_path)
        self.m_conf.set_base_dpaths()

        cleanup = CliCleanup(self.m_conf)

        if args.pipeline_name:
            if args.object_name:
                cleanup.cleanup_data_objects(args.pipeline_name, args.object_name)
            else:
                self.exit_with_help(args.config_file_path,
                                    f"Cleanup of pipeline is not supported yet. "
                                    f"To remove an object from specific pipeline provide additionaly --object_name: object_name.")
        else:
            self.exit_with_help(args.config_file_path,
                'Provide one of the following arguments: --objects. Check below if the pipeline name already exists.')

    def exit_with_help(self, config_file_path, message):
        """
        """
        self.parser.print_help()
        self.console.print('\n'+message+'\n')
        visualize = CliVisualizer(self.m_conf)
        visualize.show_pipelines()
        sys.exit()

    def start(self):
        """
        """
        try:
            args = self.parser.parse_args()
            args.func(args)
        except:

            if 'args' not in locals():
                self.console.print(
                    "----------------------------------------------------------------------------------------")
                self.console.print(self.top_level_argument_message)
                self.console.print(
                    "----------------------------------------------------------------------------------------")
                sys.exit(0)

