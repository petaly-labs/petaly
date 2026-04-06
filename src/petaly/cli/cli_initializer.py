# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import os
import sys
import shutil

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
		# Determine connections file path
		# Priority: 1) connections_file_path from config, 2) default: pipeline_dir_path/connections.yaml
		if hasattr(self.m_conf, 'connections_file_path') and self.m_conf.connections_file_path:
			# Use explicitly specified connections file path
			connections_fpath = self.m_conf.connections_file_path
			# Determine format from file extension
			file_ext = os.path.splitext(connections_fpath)[1].lower()
			connection_format = 'yaml' if file_ext == '.yaml' else 'json'
		else:
			# Default: use pipeline_dir_path/connections.yaml
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
		
		# Track what user wants to clean up
		cleanup_pipeline = False
		cleanup_data_folders = False
		cleanup_metadata_folders = False
		
		# Ask separately for pipeline, data folders, and metadata folders
		if pipeline_exists:
			self.console.print(f"\n[bold]{self.cli_menu.break_line}[/bold]")
			self.console.print(f"[bold]Pipeline Configuration[/bold]")
			self.console.print(f"Pipeline with the path {pipeline_dpath} already exists.")
			cleanup_pipeline = self.cli_menu.prompt.Confirm.ask(
				f"Do you want to overwrite the existing {self.m_conf.pipeline_fname} configuration?",
				default=False
			)
			if cleanup_pipeline:
				self.console.print(f"Backup with the name {self.m_conf.pipeline_fname}.buckup_<timestamp> from pipeline.yaml will be created.")
		
		if output_dir_exists:
			self.console.print(f"\n[bold]{self.cli_menu.break_line}[/bold]")
			self.console.print(f"[bold]Output Directory[/bold]")
			self.console.print(f"Output directory with the path {output_pipeline_dpath} already exists.")
			
			# Check if data folders exist
			data_folders_exist = False
			metadata_folders_exist = False
			if os.path.exists(output_pipeline_dpath):
				for item in os.listdir(output_pipeline_dpath):
					item_path = os.path.join(output_pipeline_dpath, item)
					if os.path.isdir(item_path):
						data_path = os.path.join(item_path, 'data')
						metadata_path = os.path.join(item_path, 'metadata')
						if os.path.exists(data_path):
							data_folders_exist = True
						if os.path.exists(metadata_path):
							metadata_folders_exist = True
						if data_folders_exist and metadata_folders_exist:
							break
			
			if data_folders_exist:
				cleanup_data_folders = self.cli_menu.prompt.Confirm.ask(
					f"Do you want to delete all data folders (output/object-name/data/)?",
					default=False
				)
			
			if metadata_folders_exist:
				cleanup_metadata_folders = self.cli_menu.prompt.Confirm.ask(
					f"Do you want to delete all metadata folders (output/object-name/metadata/)?",
					default=False
				)
			
			# Perform cleanup based on user choices
			if cleanup_data_folders or cleanup_metadata_folders:
				if os.path.exists(output_pipeline_dpath):
					for item in os.listdir(output_pipeline_dpath):
						item_path = os.path.join(output_pipeline_dpath, item)
						if os.path.isdir(item_path):
							data_path = os.path.join(item_path, 'data')
							metadata_path = os.path.join(item_path, 'metadata')
							
							if cleanup_data_folders and os.path.exists(data_path):
								shutil.rmtree(data_path)
								self.console.print(f"[yellow]Deleted data folder:[/yellow] {data_path}")
								# Recreate empty data folder
								os.makedirs(data_path, exist_ok=True)
							
							if cleanup_metadata_folders and os.path.exists(metadata_path):
								shutil.rmtree(metadata_path)
								self.console.print(f"[yellow]Deleted metadata folder:[/yellow] {metadata_path}")
								# Recreate empty metadata folder
								os.makedirs(metadata_path, exist_ok=True)
		
		# If user didn't want to continue with pipeline recreation, exit
		if pipeline_exists and not cleanup_pipeline:
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
		
		# Prompt for exclude_objects
		self.console.print(f"\n[bold]{self.cli_menu.break_line}[/bold]")
		self.console.print(f"[bold]Specify objects to exclude[/bold]")
		exclude_objects_input = prompt.Prompt.ask(
			"Enter comma-separated list of object names to exclude (or press Enter for none)",
			default=""
		)
		exclude_objects = [obj.strip() for obj in exclude_objects_input.split(',') if obj.strip()] if exclude_objects_input else []
		
		# Add exclude_objects to load_attributes at the end
		# Ensure load_attributes exists
		if 'load_attributes' not in composed_config['pipeline']:
			composed_config['pipeline']['load_attributes'] = {}
		# Remove exclude_objects if it exists (to re-add at end)
		if 'exclude_objects' in composed_config['pipeline']['load_attributes']:
			del composed_config['pipeline']['load_attributes']['exclude_objects']
		# Add exclude_objects at the end
		composed_config['pipeline']['load_attributes']['exclude_objects'] = exclude_objects
		
		# Save as single document (no "---" separator)
		config_to_save = {
			'pipeline': composed_config['pipeline'],
			'data_objects_spec': []
		}
		self.f_handler.save_dict_to_file(pipeline_fpath, config_to_save, file_format=pipeline_format)

		self.console.print(f"\nCheck pipeline {pipeline_name} under: {pipeline_fpath}")
		self.console.print(f"Check output directory under: {output_pipeline_dpath}")

		# Get new boolean parameters with defaults
		all_from_schema = composed_config['pipeline']['load_attributes'].get('all_from_schema', True)
		use_data_objects_spec = composed_config['pipeline']['load_attributes'].get('use_data_objects_spec', True)
		
		# Convert to boolean if string
		if isinstance(all_from_schema, str):
			all_from_schema = all_from_schema.lower() in ('true', '1', 'yes')
		if isinstance(use_data_objects_spec, str):
			use_data_objects_spec = use_data_objects_spec.lower() in ('true', '1', 'yes')
		
		# Backward compatibility: convert old "prefer"/"strict" string values
		old_use_data_objects_spec_str = composed_config['pipeline']['load_attributes'].get('use_data_objects_spec')
		if isinstance(old_use_data_objects_spec_str, str) and old_use_data_objects_spec_str.lower() in ('prefer', 'strict'):
			old_value = old_use_data_objects_spec_str.lower()
			# Map: 'prefer' -> all_from_schema=True, use_data_objects_spec=True
			# Map: 'strict' -> all_from_schema=False, use_data_objects_spec=True
			if old_value == 'prefer':
				all_from_schema = True
				use_data_objects_spec = True
			elif old_value == 'strict':
				all_from_schema = False
				use_data_objects_spec = True
		
		# Backward compatibility: check for old include_data_objects parameter
		old_include_data_objects = composed_config['pipeline']['load_attributes'].get('include_data_objects')
		if old_include_data_objects is not None:
			if isinstance(old_include_data_objects, str):
				old_include_data_objects = old_include_data_objects.lower()
			# Map: 'all' -> all_from_schema=True, use_data_objects_spec=True
			# Map: 'spec' -> all_from_schema=False, use_data_objects_spec=True
			if old_include_data_objects == 'all':
				all_from_schema = True
				use_data_objects_spec = True
			elif old_include_data_objects == 'spec':
				all_from_schema = False
				use_data_objects_spec = True
		
		# Handle data_objects_spec structure (can be array or object)
		data_objects_spec_raw = composed_config.get('data_objects_spec', [])
		if isinstance(data_objects_spec_raw, dict):
			data_objects_spec = data_objects_spec_raw.get('objects', data_objects_spec_raw.get('data_objects_spec', []))
		else:
			data_objects_spec = data_objects_spec_raw if isinstance(data_objects_spec_raw, list) else []
		
		process_continue = True
		# Validate: all_from_schema=false and use_data_objects_spec=false is invalid
		if not all_from_schema and not use_data_objects_spec:
			self.console.print(f"\n[bold red]Error:[/bold red] Invalid configuration - all_from_schema=false and use_data_objects_spec=false.")
			self.console.print(f"This combination will result in no objects being loaded. Please set at least one to true.")
			# Auto-fix: set use_data_objects_spec to true
			use_data_objects_spec = True
			self.console.print(f"[bold yellow]Auto-corrected:[/bold yellow] set use_data_objects_spec=true to allow loading from data_objects_spec[]")
		
		# When all_from_schema=false, object definition is required.
		# Go directly to the first object definition without showing an extra warning or confirmation.
		if all_from_schema and len(data_objects_spec) == 0:
			self.console.print(f"\nThe parameter [bold]data_objects_spec[/bold] is empty, which means all objects will be loaded from schema (all_from_schema=true).")
			self.console.print(f"In order to load specific objects incrementally, specify them one by one, or modify the pipeline manually.")
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
