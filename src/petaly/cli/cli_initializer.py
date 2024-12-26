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

import os
import sys

from petaly.utils.file_handler import FileHandler
from petaly.core.pipeline import Pipeline
from petaly.cli.cli_menu import CliMenu
from petaly.core.composer import Composer
import rich.prompt as prompt


class CliInitializer():

	def __init__(self, main_config):
		self.m_conf = main_config
		self.cli_menu = CliMenu(main_config)
		self.console = self.cli_menu.console
		self.composer = Composer()
		self.f_handler = FileHandler()

	### Init Workspace
	def init_workspace(self, skip_message_if_exist=False):
		"""
		"""
		self.create_dir(self.m_conf.pipeline_base_dpath, "Pipeline base", skip_message_if_exist)
		self.create_dir(self.m_conf.output_base_dpath, "Output base", skip_message_if_exist)
		self.create_dir(self.m_conf.logs_base_dpath, "Logs base", skip_message_if_exist)

	def create_dir(self, dpath, dir_type_message, skip_message_if_exist):

		if self.f_handler.is_dir(dpath):
			if not skip_message_if_exist:
				self.console.print (f"{dir_type_message} directory {dpath} already exists.")
		else:
			self.f_handler.make_dirs(dpath)
			self.console.print(f"{dir_type_message} directory {dpath} is created.")


	### Init Pipeline
	def init_pipeline(self, pipeline_name):
		""" Initiate sub folder under pipeline directory with given pipeline_name
		"""
		self.console.print(f"[bold]{self.cli_menu.break_line}[/bold]")
		self.console.print(f"[bold]The pipeline initialization has started.[/bold]\n")

		if pipeline_name is None:
			pipeline_name = self.cli_menu.force_assign_value(key='pipeline_name', message="Specify unique pipeline name")

		self.cli_menu.composed_pipeline_config[0]['pipeline']['pipeline_attributes'].update({'pipeline_name': pipeline_name})

		pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
		pipeline_fpath = os.path.join(pipeline_dpath, self.m_conf.pipeline_fname)
		output_pipeline_dpath = os.path.join(self.m_conf.output_base_dpath, pipeline_name)

		# check pipeline directory and pipeline.yaml
		if self.f_handler.is_file(pipeline_fpath):
			self.console.print (f"Pipeline with the path {pipeline_dpath} already exists.")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"\nDo you want to continue and overwrite the existing {self.m_conf.pipeline_fname} configuration?")

			if process_continue:
				self.console.print(f"Backup with the name {self.m_conf.pipeline_fname}.buckup_<timestamp> from pipeline.yaml will be created.")
			else:
				sys.exit()

		elif self.f_handler.is_dir(pipeline_dpath):
			pass
		else:
			self.f_handler.make_dirs(pipeline_dpath)

		# check output folder and ask for confirmation
		if self.f_handler.is_dir(output_pipeline_dpath):
			self.console.print (f"\nOutput directory with the path {output_pipeline_dpath} already exists.")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"\nDo you want to continue and overwrite the existing output directory? All files inside will be deleted.")
			if process_continue is True:
				self.f_handler.cleanup_dir(output_pipeline_dpath)
				self.f_handler.make_dirs(output_pipeline_dpath)
			else:
				sys.exit()

		else:
			self.f_handler.make_dirs(output_pipeline_dpath)

		self.cli_menu.compose_pipeline(pipeline_name)
		self.f_handler.backup_file(pipeline_fpath)
		self.f_handler.save_dict_to_yaml(pipeline_fpath, self.cli_menu.composed_pipeline_config, dump_all=True)

		self.console.print(f"\nCheck pipeline {pipeline_name} under: {pipeline_fpath}")
		self.console.print(f"Check output directory under: {output_pipeline_dpath}")

		data_objects_spec_mode = self.cli_menu.composed_pipeline_config[0]['pipeline']['data_attributes'].get('data_objects_spec_mode')

		process_continue = True
		if data_objects_spec_mode in ("ignore"):
			self.console.print(f"\nThe parameter [bold]data_objects_spec_mode[/bold] was set to [bold]ignore[/bold]")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"Do you want to continue defining the data objects despite the definition in data_objects_spec_mode=ignore?")

		if process_continue:
			self.console.print("\nContinue with the configuration of objects/tables in the next step.")

			self.init_data_objects(pipeline_name, object_names=None)

		else:
			self.console.print("Review the pipeline.yaml file and modify manually if necessary.")

	def init_data_objects(self, pipeline_name, object_names):

		if pipeline_name is None:
			pipeline_name = self.cli_menu.force_assign_value(key='pipeline_name',
													message="Provide pipeline name. Pipeline with this name should already exists.")

		pipeline = Pipeline(pipeline_name, self.m_conf)
		pipeline_all_obj = pipeline.get_pipeline_entire_config()
		data_objects_spec_meta = self.cli_menu.pipeline_meta_config.get('data_objects_spec')
		object_name_key_comment = data_objects_spec_meta.get('object_name').get("key_comment")
		data_objects_spec_list = []

		use_pipeline_wizard = prompt.Confirm.ask("Use data object specifications wizard")
		ask_for_next_object = True
		# 1. first handle list in array
		if object_names is not None and type(object_names) == str:

			object_name_arr = [item.strip() for item in object_names.split(',')]

			for i, object_name in enumerate(object_name_arr):
				self.console.print(f"\n[bold]{self.cli_menu.break_line}[/bold]")
				self.console.print(f"Continue with the object [bold]{object_name}[/bold] specification")

				object_spec = self.cli_menu.compose_object_spec(pipeline, object_name=object_name, use_pipeline_wizard=use_pipeline_wizard)

				data_objects_spec_list.append(object_spec)
				self.composer.save_data_objects(pipeline_all_obj=pipeline_all_obj, data_objects_spec=data_objects_spec_list, pipeline_fpath=pipeline.pipeline_fpath)

			ask_for_next_object = False

		# 2. continues here if array is emtpy or was handel and adding_objects is still true
		while ask_for_next_object:
			object_name = self.cli_menu.force_assign_value(key='object_name', message=object_name_key_comment)

			object_spec = self.cli_menu.compose_object_spec(pipeline, object_name=object_name, use_pipeline_wizard=use_pipeline_wizard)
			data_objects_spec_list.append(object_spec)

			# save each object to the yaml document
			self.composer.save_data_objects(pipeline_all_obj=pipeline_all_obj, data_objects_spec=data_objects_spec_list, pipeline_fpath=pipeline.pipeline_fpath)

			self.console.print(f"\n[bold]{self.cli_menu.break_line}[/bold]")
			self.console.print(f"The object: [bold]{object_name}[/bold] has been added to the pipeline.")

			ask_for_next_object = prompt.Confirm.ask("\nDo you want to continue defining the next data object?")

		if len(data_objects_spec_list) > 0:
			self.console.print(f"Data-Objects were added for pipeline {pipeline.pipeline_name}. For further configuration review the yaml file: {pipeline.pipeline_fpath} ")
		else:
			self.console.print(
				f"Data-Objects weren't specified for pipeline {pipeline.pipeline_name}. For further configuration review the yaml file: {pipeline.pipeline_fpath} ")
