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

import logging
logger = logging.getLogger(__name__)

import os
import sys
import importlib
import importlib.abc
from rich.console import Console

from configparser import ConfigParser, ExtendedInterpolation
from petaly.utils.file_handler import FileHandler
from petaly.sysconfig.load_class import load_class_obj

class MainConfig:

    def __init__(self):

        self.console = Console()
        self.pipeline_fname = 'pipeline.yaml'
        self.env_config_dpath = os.getenv('PETALY_CONFIG_DIR')
        self.main_config_fname = 'petaly.ini'
        self.logging_config_fname = 'logging_config.json'

        self.class_config_fname = 'class_config.json'
        self.pipeline_meta_config_fname = 'pipeline_meta_config.json'
        self.extractor_type_transformer_fname = 'extractor_type_transformer.json'
        self.type_mapping_fname = '{source_connector_id}.json'
        self.metadata_sql_fname = 'metadata.sql'
        self.extract_to_stmt_fname = 'extract_to_stmt.sql'
        self.load_from_stmt_fname = 'load_from_stmt.sql'
        self.create_table_stmt_fname = 'create_table_stmt.sql'
        self.database_attributes_fname = 'database_attributes.json'

        self.base_dpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

        self.src_dpath = os.path.dirname(self.base_dpath)
        self.root_dpath = os.path.dirname(self.src_dpath)
        self.main_config_fpath = None

        self.sysconfig = os.path.join(self.base_dpath, 'sysconfig')
        self.sysconfig_config = os.path.join(self.sysconfig, 'config')

        self.templates_main_config_fpath = os.path.join(self.sysconfig_config, 'template_' + self.main_config_fname)
        self.logging_config_fpath = os.path.join(self.sysconfig_config, self.logging_config_fname)
        self.pipeline_meta_config_fpath = os.path.join(self.sysconfig_config, self.pipeline_meta_config_fname)
        self.class_sysconfig_fpath = os.path.join(self.sysconfig_config, self.class_config_fname)

        self.f_handler = FileHandler()

    def set_main_config_fpath(self, config_file_path):
        """
        """
        if config_file_path is None:
            config_file_path = self.main_config_fname

        elif self.f_handler.check_file_extension(config_file_path, 'ini') is False:
            config_file_path = os.path.join(config_file_path, self.main_config_fname)

        if os.path.isabs(config_file_path) is False:
            if self.env_config_dpath:
                config_file_path = os.path.join(self.env_config_dpath, config_file_path)

            else:
                self.console.print(self.get_petaly_config_path_message())
                sys.exit()


        # if config file doesn't exists
        if self.f_handler.is_file(config_file_path) is False:

            if self.f_handler.is_dir(config_file_path):
                config_file_name = self.main_config_fname
            else:
                config_file_name = os.path.basename(config_file_path)

            self.f_handler.cp_file(self.templates_main_config_fpath, os.path.dirname(config_file_path), os.path.basename(config_file_name))

            self.console.print(f"The main config file was created: {config_file_path}\n"
                  f"Open it with an editor and provide absolute paths for the following parameters: \nlogs_dir_path= \npipeline_dir_path= \noutput_dir_path=\n")

        self.main_config_fpath = config_file_path

    def set_base_dpaths(self):
        """
        """
        if self.f_handler.is_file(self.main_config_fpath) is False:

            logger.warning(f"Check and modify petaly config file: {self.main_config_fpath}\n"
                            f"If file doesn't exists run command with default parameters: "
                            f"init -c\n"
                            f"or specify custom config file: "
                            f"init -c {os.path.dirname(self.main_config_fpath)}")
            sys.exit()

        else:
            process_exit = False
            conf_parser = ConfigParser(interpolation=ExtendedInterpolation())
            conf_parser.read(self.main_config_fpath)

            self.logs_base_dpath = conf_parser['workspace_config']['logs_dir_path']
            self.pipeline_base_dpath = conf_parser['workspace_config']['pipeline_dir_path']
            self.output_base_dpath = conf_parser['workspace_config']['output_dir_path']

            if os.path.isabs(self.logs_base_dpath) is False:
                process_exit = True
                self.console.print(f"The parameter logs_dir_path is not specified.")

            if os.path.isabs(self.pipeline_base_dpath) is False:
                process_exit = True
                self.console.print(f"The parameter pipeline_dir_path is not specified.")

            if os.path.isabs(self.output_base_dpath) is False:
                process_exit = True
                self.console.print(f"The parameter output_dir_path is not specified.")

            if process_exit:
                self.console.print(f"Check petaly config file: {self.main_config_fpath}")
                sys.exit()

    def get_petaly_config_path_message(self):
        return    (f"To initialize config file for the first time, provide the absolute path to petaly config file: -c /ABSOLUTE_PATH_TO_PETALY_CONFIG_DIR/{self.main_config_fname}\n"
                   f"To skip '-c' argument at runtime, set an environment variable: export PETALY_CONFIG_DIR=/ABSOLUTE_PATH_TO_PETALY_CONFIG_DIR\n")


    def get_platform_attributes(self, platform_id):
        platforms_cl_config = self.f_handler.load_json(self.class_sysconfig_fpath).get('platforms')
        platform_config = platforms_cl_config.get(platform_id)

        if platform_config is None:
            logger.warning(f"The platform {platform_id} in class_config.json is not specified")
            sys.exit()

        return platform_config

    def get_supported_platforms(self, connector_id):
        platforms_cl_config = self.f_handler.load_json(self.class_sysconfig_fpath).get("connectors")
        platform_type_list = platforms_cl_config.get(connector_id).get('supported_platforms')
        if platform_type_list is None or len(platform_type_list)==0:
            platform_type_list = ['local']
        return platform_type_list

    def set_extractor_paths(self, connector_id):
        connector_dpath = self.get_connector_dpath(connector_id)
        self.connector_metadata_sql_fpath = os.path.join(connector_dpath, self.metadata_sql_fname)
        self.connector_extract_to_stmt_fpath = os.path.join(connector_dpath, 'config', self.extract_to_stmt_fname)
        return True

    def get_available_connectors(self):
        endpoint_types = (self.f_handler.load_json(self.class_sysconfig_fpath).get("connectors").keys())
        return endpoint_types

    def set_loader_paths(self, connector_id):
        connector_dpath = self.get_connector_dpath(connector_id)
        self.connector_load_from_stmt_fpath = os.path.join(connector_dpath, 'config', self.load_from_stmt_fname)
        self.connector_create_table_stmt_fpath = os.path.join(connector_dpath, 'config', self.create_table_stmt_fname)

        return True

    def get_connector_dpath(self, connector_id):
        """ The connector_id_dpath has a dot as path delimiter in class_config.json, e.g. connector_id_dpath: "connectors.mysql".
            To make it cross-platform compatible the split('.') and replace with directory delimiter is required.
        """
        connector_class_config = self.get_connector_class_config(connector_id)
        connector_id_dpath = connector_class_config.get('connector_dpath')
        connector_id_dpath_list = connector_id_dpath.split('.')
        connector_dpath = os.path.join(self.src_dpath, *connector_id_dpath_list)
        return connector_dpath

    def get_connector_class_config(self, connector_id):
        connectors_cl_config = self.f_handler.load_json(self.class_sysconfig_fpath).get("connectors")
        connector_class_config = connectors_cl_config.get(connector_id)

        if not connector_class_config:
            logger.warning(f"The connector_id {connector_id} in class_config.json is not specified")
            sys.exit()

        return connector_class_config

    def get_database_attributes(self, connector_id):

        #connector_class_config = self.get_connector_class_config(connector_id)
        connector_dpath = self.get_connector_dpath(connector_id)
        database_attributes_fpath = os.path.join(connector_dpath, 'config', self.database_attributes_fname)

        return self.f_handler.load_json(database_attributes_fpath)

    def get_type_mapping_paths(self, connector_id):

        connector_class_config = self.get_connector_class_config(connector_id)

        type_mapping_path_list = []
        for type_mapping_path in connector_class_config.get("type_mapping"):
            type_mapping_dpath = os.path.join(self.src_dpath, *type_mapping_path.split('.'))
            type_mapping_path_list.append(os.path.join(type_mapping_dpath, self.type_mapping_fname))
        return type_mapping_path_list

    def get_extractor_type_transformer_fpath(self, connector_id):

        connector_dpath = self.get_connector_dpath(connector_id)
        return os.path.join(connector_dpath, 'config', self.extractor_type_transformer_fname)

    def get_extractor_class(self, connector_id):

        connector_class_config = self.get_connector_class_config(connector_id)
        return self.get_class_obj(connector_class_config, 'extractor')

    def get_loader_class(self, connector_id):

        connector_class_config = self.get_connector_class_config(connector_id)
        return self.get_class_obj(connector_class_config,'loader')

    def get_class_obj(self, connector_class_config, class_type):
        module_path = connector_class_config.get('connector_dpath') + '.' + connector_class_config.get(class_type).get('module_path')
        class_name = connector_class_config.get(class_type).get('class_name')
        class_object = load_class_obj(module_path, class_name)
        return class_object
