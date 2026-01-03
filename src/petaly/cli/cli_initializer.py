# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

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
		self.cli_menu = CliMenu(main_config)  # CliMenu composes CliPipeline and CliConnections
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
		
		# Create connections.yaml file if it doesn't exist
		self.create_connections_file(skip_message_if_exist)

	def create_dir(self, dpath, dir_type_message, skip_message_if_exist):

		if self.f_handler.is_dir(dpath):
			if not skip_message_if_exist:
				self.console.print (f"{dir_type_message} directory {dpath} already exists.")
		else:
			self.f_handler.make_dirs(dpath)
			self.console.print(f"{dir_type_message} directory {dpath} is created.")
	
	def create_connections_file(self, skip_message_if_exist=False):
		"""
		Creates connections.yaml/json file from skeleton if it doesn't exist.
		
		Args:
			skip_message_if_exist: If True, don't print message if file already exists
		"""
		# Determine connections file path and format
		connection_format = self.m_conf.global_settings.get('connections_file_format', 'yaml')
		connections_fname = f'connections.{connection_format}'
		connections_fpath = os.path.join(self.m_conf.pipeline_base_dpath, connections_fname)
		
		if self.f_handler.is_file(connections_fpath):
			if not skip_message_if_exist:
				self.console.print(f"Connections file {connections_fpath} already exists.")
			return
		
		# Load skeleton
		skeleton_fpath = self.m_conf.connections_skeleton_fpath
		if not self.f_handler.is_file(skeleton_fpath):
			self.console.print(f"[yellow]Warning:[/yellow] Connections skeleton file not found at {skeleton_fpath}")
			return
		
		skeleton_config = self.f_handler.load_json(skeleton_fpath)
		
		# Save to connections file
		self.f_handler.save_dict_to_file(
			connections_fpath,
			skeleton_config,
			file_format=connection_format
		)
		
		self.console.print(f"Connections file {connections_fpath} is created.")


	### Init Pipeline
	def init_pipeline(self, pipeline_name):
		""" Initiate sub folder under pipeline directory with given pipeline_name
		"""
		self.console.print(f"[bold]{self.cli_menu.break_line}[/bold]")
		self.console.print(f"[bold]The pipeline initialization has started.[/bold]\n")

		if pipeline_name is None:
			pipeline_name = self.cli_menu.force_assign_value(key='pipeline_name', message="Specify unique pipeline name")

		pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
		pipeline_fpath = os.path.join(pipeline_dpath, self.m_conf.pipeline_fname)
		output_pipeline_dpath = os.path.join(self.m_conf.output_base_dpath, pipeline_name)

		# check pipeline directory and pipeline.yaml
		pipeline_exists = self.f_handler.is_file(pipeline_fpath)
		output_dir_exists = self.f_handler.is_dir(output_pipeline_dpath)
		
		# Check if both pipeline and output directory exist, merge questions if both exist
		if pipeline_exists and output_dir_exists:
			self.console.print(f"Pipeline with the path {pipeline_dpath} already exists.")
			self.console.print(f"Output directory with the path {output_pipeline_dpath} already exists.")
			process_continue = self.cli_menu.prompt.Confirm.ask(
				f"\nDo you want to continue and overwrite the existing {self.m_conf.pipeline_fname} configuration and output directory?\n"
				f"All files in the output directory will be deleted and a backup of {self.m_conf.pipeline_fname} will be created."
			)
			
			if process_continue:
				self.console.print(f"Backup with the name {self.m_conf.pipeline_fname}.buckup_<timestamp> from pipeline.yaml will be created.")
				self.f_handler.cleanup_dir(output_pipeline_dpath)
				self.f_handler.make_dirs(output_pipeline_dpath)
			else:
				sys.exit()
		elif pipeline_exists:
			self.console.print(f"Pipeline with the path {pipeline_dpath} already exists.")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"\nDo you want to continue and overwrite the existing {self.m_conf.pipeline_fname} configuration?")

			if process_continue:
				self.console.print(f"Backup with the name {self.m_conf.pipeline_fname}.buckup_<timestamp> from pipeline.yaml will be created.")
			else:
				sys.exit()
		elif output_dir_exists:
			self.console.print(f"Output directory with the path {output_pipeline_dpath} already exists.")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"\nDo you want to continue and overwrite the existing output directory? All files inside will be deleted.")
			if process_continue is True:
				self.f_handler.cleanup_dir(output_pipeline_dpath)
				self.f_handler.make_dirs(output_pipeline_dpath)
			else:
				sys.exit()
		
		# Create directories if they don't exist
		if not self.f_handler.is_dir(pipeline_dpath):
			self.f_handler.make_dirs(pipeline_dpath)
		
		if not self.f_handler.is_dir(output_pipeline_dpath):
			self.f_handler.make_dirs(output_pipeline_dpath)

		# Compose pipeline using CliMenu's CliPipeline
		self.cli_menu.cli_pipeline.compose_pipeline(pipeline_name)
		self.f_handler.backup_file(pipeline_fpath)
		
		# Prepare the configuration based on format
		pipeline_format = self.m_conf.global_settings.get('pipeline_file_format', 'yaml')
		composed_config = self.cli_menu.cli_pipeline.get_composed_config()
		
		if pipeline_format == 'yaml':
			config_to_save = {
				'pipeline': composed_config['pipeline'],
				'data_objects_spec': []
			}
		else:
			# For JSON, combine the pipeline and data_objects_spec into a single document
			config_to_save = {
				'pipeline': composed_config['pipeline'],
				'data_objects_spec': []
			}
		
		self.f_handler.save_dict_to_file(pipeline_fpath, config_to_save, file_format=pipeline_format)

		self.console.print(f"\nCheck pipeline {pipeline_name} under: {pipeline_fpath}")
		self.console.print(f"Check output directory under: {output_pipeline_dpath}")

		include_data_objects = composed_config['pipeline']['data_attributes'].get('include_data_objects', 'spec')
		
		# Backward compatibility: check for old parameter names
		if include_data_objects is None or include_data_objects not in ('all', 'spec'):
			# Check for old load_data_objects parameter
			old_load_data_objects = composed_config['pipeline']['data_attributes'].get('load_data_objects')
			if old_load_data_objects is not None:
				# Convert old load_data_objects to new include_data_objects
				if isinstance(old_load_data_objects, str):
					old_load_data_objects = old_load_data_objects.lower()
				include_data_objects = old_load_data_objects if old_load_data_objects in ('all', 'spec') else 'spec'
			else:
				# Check for old load_all_from_schema parameter (boolean)
				old_load_all = composed_config['pipeline']['data_attributes'].get('load_all_from_schema')
				if old_load_all is not None:
					# Convert old boolean to new string format
					if isinstance(old_load_all, str):
						old_load_all = old_load_all.lower() == 'true'
					include_data_objects = 'all' if old_load_all else 'spec'
				else:
					# Check for old load_data_objects_spec_only parameter
					old_load_spec_only = composed_config['pipeline']['data_attributes'].get('load_data_objects_spec_only')
					if old_load_spec_only is not None:
						# Invert: old load_data_objects_spec_only=true means include_data_objects=spec
						if isinstance(old_load_spec_only, str):
							old_load_spec_only = old_load_spec_only.lower() == 'true'
						include_data_objects = 'spec' if old_load_spec_only else 'all'
					else:
						# Check for even older apply_data_objects_spec parameter
						old_apply_spec = composed_config['pipeline']['data_attributes'].get('apply_data_objects_spec')
						if old_apply_spec is not None:
							# Old apply_data_objects_spec=true means prefer (load all), which is include_data_objects=all
							if isinstance(old_apply_spec, str):
								old_apply_spec = old_apply_spec.lower() == 'true'
							include_data_objects = 'all' if old_apply_spec else 'spec'
		
		# Normalize to lowercase string
		if isinstance(include_data_objects, str):
			include_data_objects = include_data_objects.lower()
			if include_data_objects not in ('all', 'spec'):
				include_data_objects = 'spec'
		
		data_objects_spec = composed_config.get('data_objects_spec', [])
		
		process_continue = True
		# Check if include_data_objects='spec' and data_objects_spec is empty
		if include_data_objects == 'spec' and len(data_objects_spec) == 0:
			self.console.print(f"\n[bold yellow]Warning:[/bold yellow] include_data_objects='spec' and data_objects_spec[] is empty. No objects will be loaded.")
			self.console.print(f"Either set include_data_objects='all' to load all objects from schema, or add objects to data_objects_spec[].")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"Do you want to continue defining specific data objects?")
		elif include_data_objects == 'all' and len(data_objects_spec) == 0:
			self.console.print(f"\nThe parameter [bold]data_objects_spec[/bold] is empty, which means all objects will be loaded from schema (include_data_objects='all')")
			process_continue = self.cli_menu.prompt.Confirm.ask(f"Do you want to continue defining specific data objects?")

		if process_continue:
			self.console.print("\nContinue with the configuration of objects/tables in the next step.")

			self.init_data_objects(pipeline_name, object_names=None)

		else:
			# Pipeline configuration was already saved earlier in init_pipeline
			# Just exit with message
			self.console.print("Pipeline configuration saved. Review the pipeline.yaml file and modify manually if necessary.")

	def init_data_objects(self, pipeline_name, object_names):

		if pipeline_name is None:
			pipeline_name = self.cli_menu.force_assign_value(key='pipeline_name',
													message="Provide pipeline name. Pipeline with this name should already exists.")

		pipeline = Pipeline(pipeline_name, self.m_conf)
		pipeline_all_obj = pipeline.get_pipeline_entire_config()
		data_objects_spec_meta = self.cli_menu.cli_pipeline.pipeline_meta_config.get('data_objects_spec')
		object_name_key_comment = data_objects_spec_meta.get('object_name').get("key_comment")
		data_objects_spec_list = []

		# Use wizard mode by default when continuing from init_pipeline
		use_pipeline_wizard = True
		ask_for_next_object = True
		# 1. first handle list in array
		if object_names is not None and type(object_names) == str:

			object_name_arr = [item.strip() for item in object_names.split(',')]

			for i, object_name in enumerate(object_name_arr):
				self.console.print(f"\n[bold]{self.cli_menu.break_line}[/bold]")
				self.console.print(f"Continue with the object [bold]{object_name}[/bold] specification")

				object_spec = self.cli_menu.cli_pipeline.compose_object_spec(pipeline, object_name=object_name, use_pipeline_wizard=use_pipeline_wizard)

				data_objects_spec_list.append(object_spec)
				self.composer.save_data_objects(pipeline_all_obj=pipeline_all_obj, data_objects_spec=data_objects_spec_list, pipeline_fpath=pipeline.pipeline_fpath)

			ask_for_next_object = False

		# 2. continues here if array is emtpy or was handel and adding_objects is still true
		while ask_for_next_object:
			object_name = self.cli_menu.force_assign_value(key='object_name', message=object_name_key_comment)

			object_spec = self.cli_menu.cli_pipeline.compose_object_spec(pipeline, object_name=object_name, use_pipeline_wizard=use_pipeline_wizard)
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
