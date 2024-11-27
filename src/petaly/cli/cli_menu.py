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

import rich.prompt as prompt
from rich.console import Console

from petaly.utils.file_handler import FileHandler
from collections import Counter, OrderedDict


class OrderedCounter(Counter, OrderedDict):
    'Counter that remembers the order elements are first seen'
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__,
                           OrderedDict(self))

    def __reduce__(self):
        return self.__class__, (OrderedDict(self),)

class CliMenu():

    def __init__(self, main_config):
        self.use_long_form_wizard = True
        self.console = Console()
        self.prompt = prompt
        self.m_conf = main_config
        self.f_handler = FileHandler()

        self.pipeline_meta_config = self.f_handler.load_json(self.m_conf.pipeline_meta_config_fpath)

        self.composed_pipeline_config = [
                                            {"pipeline":
                                                 {"pipeline_attributes": {},
                                                  "source_attributes": {},
                                                  "target_attributes": {},
                                                  "data_attributes": {
                                                      "use_data_objects_spec":True
                                                  }
                                                  }
                                            },
                                            {
                                              "data_objects_spec": []
                                            }
                                        ]

    def force_assign_value(self, key, message):
        while True:
            self.console.print(message)
            value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]")
            if value.strip() != '':
                break
        return value

    def compose_pipeline(self, pipeline_name):
        self.use_long_form_wizard = prompt.Confirm.ask("\nUse long form")

        self.compose_pipeline_attributes(pipeline_name)
        self.compose_endpoint_attributes('source')
        self.compose_endpoint_attributes('target')
        self.compose_data_attributes()

    def compose_pipeline_attributes(self, pipeline_name):
        """
        """
        # self.pipeline_meta_config.get("pipeline_attributes")
        predefined_values = {'pipeline_name': pipeline_name}
        pipeline_attributes = self.pipeline_meta_config.get("pipeline_attributes")
        assigned_attributes = self.assign_attributes(pipeline_attributes, predefined_values=predefined_values)
        self.composed_pipeline_config[0]['pipeline']['pipeline_attributes'].update(assigned_attributes)

    def compose_endpoint_attributes(self, endpoint):
        endpoint_attributes_name = endpoint + '_attributes'

        self.console.print(
            f"\n[bold blue]---------- Specify {endpoint.upper()} attributes --------------[/bold blue]\n")
        #endpoint_attributes = self.pipeline_meta_config.get("endpoint_attributes")
        endpoint_attributes = {}

        predefined_values = {}
        # step 1. specify endpoint type
        available_connectors = self.m_conf.get_available_connectors()
        endpoint_type = prompt.Prompt.ask(f"Specify [bold yellow]{endpoint} endpoint[/bold yellow] type", choices=available_connectors)
        predefined_values.update({'endpoint_type': endpoint_type})

        assigned_endpoint_attributes = self.assign_attributes(endpoint_attributes, predefined_values=predefined_values)
        self.composed_pipeline_config[0]['pipeline'][endpoint_attributes_name].update(assigned_endpoint_attributes)

        # step 2. get connector category
        #connector_category = self.m_conf.get_connector_class_config(endpoint_type).get('connector_category')

        # step 3. based on connector category define database or file parameters
        connector_attributes = self.m_conf.get_connector_attributes(endpoint_type)
        assigned_connector_attributes = self.assign_attributes(connector_attributes, predefined_values=None)
        self.composed_pipeline_config[0]['pipeline'][endpoint_attributes_name].update(assigned_connector_attributes)

        # step 4. get and define platform type
        platform_type_list = self.m_conf.get_supported_platforms(endpoint_type)

        if len(platform_type_list) == 1:
            platform_type = platform_type_list[0]
        else:
            platform_type = prompt.Prompt.ask(f"Specify [bold green]platform_type[/bold green]",
                                              choices=platform_type_list)

        # step 5. assign platform type and platform attributes
        if platform_type != 'local':
            predefined_values.update({'platform_type': platform_type})
            platform_attributes = self.m_conf.get_platform_attributes(platform_id=platform_type)
            assigned_platform_attributes = self.assign_attributes(platform_attributes, exclude_key_list=['endpoint_type'], predefined_values=predefined_values)
            self.composed_pipeline_config[0]['pipeline'][endpoint_attributes_name].update(assigned_platform_attributes)


    def compose_data_attributes(self):

        self.console.print(
            f"\n[bold blue]---------- Specify data object attributes --------------[/bold blue]")

        data_attributes = self.pipeline_meta_config.get("data_attributes")
        assigned_data_attributes = self.assign_attributes(data_attributes, predefined_values=None, exclude_key_list=["use_data_objects_spec"])
        self.composed_pipeline_config[0]['pipeline']['data_attributes'].update(assigned_data_attributes)

        csv_parse_options = self.pipeline_meta_config.get("csv_parse_options")
        assigned_csv_parse_options = self.assign_attributes(csv_parse_options, predefined_values=None)
        self.composed_pipeline_config[0]['pipeline']['data_attributes'].update({"csv_parse_options": assigned_csv_parse_options})

        # use_data_objects_spec
        self.console.print(f"\nFollowing configuration redefines a configuration for each data object/table:")
        self.use_data_objects_spec = prompt.Confirm.ask("[bold green]use_data_objects_spec:[/bold green]")
        self.composed_pipeline_config[0]['pipeline']['data_attributes'].update(
            {'use_data_objects_spec': self.use_data_objects_spec})

    def compose_data_objects_spec(self, object_names):

        self.use_long_form_wizard = prompt.Confirm.ask("Use data-objects wizard")

        data_objects_spec = self.pipeline_meta_config.get("data_objects_spec")
        data_objects_spec_list = []

        if object_names is not None and len(object_names)>0:
            #self.console.print(f"\n[bold blue]---------- Run initialization of Data-Objects-Spec --------------[/bold blue]")

            for obj_name in object_names:
                self.console.print(
                    f"\n[bold blue]---------- Configure data object/table \[{obj_name}]: ------------------[/bold blue]")

                predefined_values = {'object_name': obj_name}

                assigned_attributes = self.assign_attributes(data_objects_spec, predefined_values=predefined_values, exclude_key_list=['object_name'])
                assigned_attributes.pop('object_name')
                tmp_assigned_attributes = {}
                tmp_assigned_attributes.update({'object_name': obj_name, 'object_attributes': assigned_attributes})
                data_objects_spec_list.append(tmp_assigned_attributes)

        return data_objects_spec_list

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
                #if predefined_values is not None and predefined_values.get(key) is not None:
                if predefined_values is not None:
                    if self.f_handler.check_dict_key_exist(predefined_values, key):
                        assigned_value = predefined_values.get(key)
                        assigned_attributes.update({key: assigned_value})
                        continue

                # 2. define default and preassigned_values
                preassigned_values = value.get('preassigned_values')
                default_value =  None if value.get('default_value') is None else value.get('default_value')
                #default_value = None if preassigned_values[0] is None else preassigned_values[0]
                preassigned_values = None if preassigned_values[0] is None else preassigned_values
                assigned_value = default_value

                # 3. print key comment and default value
                console_print = f"\n{value.get('key_comment')} "
                if default_value is not None:
                    console_print += f"Default: [bold blue]\[{default_value}][/bold blue]"

                self.console.print(console_print)

                if key == 'database_password':
                    if self.use_long_form_wizard:
                        assigned_value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]", password=True)

                elif value.get('key_type') == 'Integer':
                    if self.use_long_form_wizard:
                        assigned_value = prompt.IntPrompt.ask(f"[bold green]{key}[/bold green]", default=default_value,
                                                              show_default=False)

                elif value.get('key_type') == 'Array':

                    if self.use_long_form_wizard:
                        assigned_value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]", default=default_value,
                                                           show_default=False)

                        if type(assigned_value) == str:
                            assigned_value = [item.strip() for item in assigned_value.split(',')]

                    if assigned_value is None:
                        assigned_value = [None]
                else:
                    if self.use_long_form_wizard:
                        assigned_value = prompt.Prompt.ask(f"[bold green]{key}[/bold green]", choices=preassigned_values,
                                                           default=default_value, show_default=False)

                    if value.get('key_type') == 'Boolean':
                        assigned_value = True if assigned_value == 'true' else False

                assigned_attributes.update({key: assigned_value})

        return assigned_attributes