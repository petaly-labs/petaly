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

from petaly.utils.file_handler import FileHandler
from petaly.core.pipeline import Pipeline
from petaly.cli.cli_menu import CliMenu


class CliCleanup():

    def __init__(self, main_config):
        self.m_conf = main_config
        self.cli_menu = CliMenu(main_config)
        self.console = self.cli_menu.console
        self.f_handler = FileHandler()

    def cleanup_data_objects(self, pipeline_name, object_names):

        if pipeline_name is None:
            pipeline_name = self.cli_menu.force_assign_value(key='pipeline_name', message="Provide pipeline name. Pipeline with this name should already exists.")

        pipe = Pipeline(pipeline_name, self.m_conf)
        pipeline_all_obj = pipe.get_pipeline_entire_config()

        if object_names is None:
            object_names = self.cli_menu.force_assign_value(key='objects', message="Provide object name or comma separated list.")

        if type(object_names) == str:
            object_name_arr = [item.strip() for item in object_names.split(',')]

        if len(object_name_arr) > 0:
            self.remove_data_objects(pipeline_all_obj=pipeline_all_obj, object_name_arr=object_name_arr, pipeline_fpath=pipe.pipeline_fpath)
            self.console.print(f"Data-Objects were removed for pipeline {pipe.pipeline_name}. For further configuration review the yaml file: {pipe.pipeline_fpath} ")
        else:
            self.console.print(f"Data-Objects weren't specified for pipeline {pipe.pipeline_name}. For further configuration review the yaml file: {pipe.pipeline_fpath} ")

    def remove_data_objects(self, pipeline_all_obj, object_name_arr, pipeline_fpath):
        data_objects_spec = pipeline_all_obj[1].get('data_objects_spec')
        for object_name in object_name_arr:
            for idx, object in enumerate(data_objects_spec):
                if object.get('object_name') == object_name:
                    data_objects_spec.pop(idx)

        pipeline_all_obj[1]['data_objects_spec'] = data_objects_spec

        self.f_handler.backup_file(pipeline_fpath)
        self.f_handler.save_dict_to_yaml(pipeline_fpath, pipeline_all_obj, dump_all=True)