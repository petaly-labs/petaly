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


class CliInitializer():

	def __init__(self, main_config):
		self.m_conf = main_config
		self.cli_menu = CliMenu(main_config)
		self.console = self.cli_menu.console

		self.f_handler = FileHandler()

	### Init Workspace
	def init_workspace(self):
		"""
		"""
		self.create_dir(self.m_conf.pipeline_base_dpath, "Pipeline base")
		self.create_dir(self.m_conf.output_base_dpath, "Output base")
		self.create_dir(self.m_conf.logs_base_dpath, "Logs base")

	def create_dir(self, dpath, dir_type_message):

		if self.f_handler.is_dir(dpath):
			self.console.print (f"{dir_type_message} directory {dpath} already exists.")
		else:
			self.f_handler.make_dirs(dpath)
			self.console.print(f"{dir_type_message} directory {dpath} is created.")


	### Init Pipeline
	def init_pipeline(self, pipeline_name):
		""" Initiate sub folder under pipeline directory with given pipeline_name
		"""
		self.console.print(f"Init of pipeline-wizard started.")

		if pipeline_name is None:
			pipeline_name = self.cli_menu.force_assign_value(key='pipeline_name', message="Specify unique pipeline name")

		self.cli_menu.composed_pipeline_config[0]['pipeline']['pipeline_attributes'].update({'pipeline_name': pipeline_name})

		pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
		pipeline_fpath = os.path.join(pipeline_dpath, self.m_conf.pipeline_fname)
		output_pipeline_dpath = os.path.join(self.m_conf.output_base_dpath, pipeline_name)

		# check pipeline directory and pipeline.yaml
		if self.f_handler.is_file(pipeline_fpath):
			self.console.print (f"Pipeline with the path {pipeline_dpath} already exists.")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"Do you want to continue and overwrite the existing {self.m_conf.pipeline_fname} configuration?")

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
			self.console.print (f"Output directory with the path {output_pipeline_dpath} already exists.")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"Do you want to continue and overwrite the existing output directory? All files inside will be deleted.")
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

		self.console.print(f"Pipeline {pipeline_name} is initialized.")
		self.console.print(f"For further configuration review the yaml file: {pipeline_fpath}")
		self.console.print(f"Output directory {output_pipeline_dpath} is created.")

		process_continue = self.cli_menu.composed_pipeline_config[0]['pipeline']['data_object_main_config'].get('use_data_objects_spec')
		if process_continue is False:
			self.console.print(f"The parameter [bold green]use_data_objects_spec[/bold green] was set to [bold blue]false[/bold blue]")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"Do you want to continue defining the data objects?")

		if process_continue:
			self.cli_menu.composed_pipeline_config[0]['pipeline']['data_object_main_config'].update({'use_data_objects_spec': True})
			self.console.print(
				f"The parameter [bold green]use_data_objects_spec[/bold green] is set now to [bold blue]true[/bold blue]")
			self.init_data_objects(pipeline_name, object_names=None)
		else:
			self.console.print(
				f"Review the pipeline.yaml file and modify manually if neccessary."
				)

	def init_data_objects(self, pipeline_name, object_names):

		self.console.print(f"Initialization of data-objects started. Before running this step the pipeline {pipeline_name} should already exists.")

		if pipeline_name is None:
			pipeline_name = self.cli_menu.force_assign_value(key='pipeline_name',
													message="Provide pipeline name. Pipeline with this name should already exists.")

		pipe = Pipeline(pipeline_name, self.m_conf)

		if object_names is None:
			object_names = self.cli_menu.force_assign_value(key='object_name_list',
																message="Specify one or more comma-separated object/table names for extraction.")

		if type(object_names) == str:
			object_name_arr = [item.strip() for item in object_names.split(',')]

		pipeline_all_obj = pipe.get_pipeline_entire_config()

		data_objects_spec = self.cli_menu.compose_data_objects_spec(object_name_arr)

		if len(data_objects_spec) > 0:
			self.save_data_objects(pipeline_all_obj=pipeline_all_obj,
												 data_objects_spec=data_objects_spec,
												 pipeline_fpath=pipe.pipeline_fpath
												 )
			self.console.print(f"Data-Objects were added for pipeline {pipe.pipeline_name}. For further configuration review the yaml file: {pipe.pipeline_fpath} ")
		else:
			self.console.print(
				f"Data-Objects weren't specified for pipeline {pipe.pipeline_name}. For further configuration review the yaml file: {pipe.pipeline_fpath} ")

	def save_data_objects(self, pipeline_all_obj, data_objects_spec, pipeline_fpath):

		data_object_list = []

		# make a list of new added objects with the same index order
		for idx, obj in enumerate(data_objects_spec):
			data_object_list.insert(idx, obj.get('object_name'))

		# if object_name is in new list replace it in the pipeline, else just copy existing object on the same place
		if pipeline_all_obj[1].get('data_objects_spec') is not None:
			if len(pipeline_all_obj[1].get('data_objects_spec'))>0 and pipeline_all_obj[1].get('data_objects_spec')[0] is not None:

				for idx, data_object in enumerate(pipeline_all_obj[1].get('data_objects_spec')):

					if data_object.get('object_name') in data_object_list:
						ind, obj =  self.get_object_spec_from_array(data_objects_spec, data_object.get('object_name'))
						#pipeline_all_obj[1].get('data_objects_spec')[idx]={'object_name':data_object.get('object_name'), 'object_attributes':obj}
						pipeline_all_obj[1].get('data_objects_spec')[idx] = obj

						data_objects_spec.pop(ind)
						data_object_list.pop(ind)

		# add the entire new objects at the end of the pipeline
		if len(data_object_list) > 0:
			for i, obj in enumerate(data_objects_spec):
				pipeline_all_obj[1].get('data_objects_spec').append(obj)

		self.f_handler.backup_file(pipeline_fpath)
		self.f_handler.save_dict_to_yaml(pipeline_fpath, pipeline_all_obj, dump_all=True)

	def get_object_spec_from_array(self, object_arr, object_name):
		for idx, obj in enumerate(object_arr):
			if object_name == obj.get('object_name'):
				return idx, obj

