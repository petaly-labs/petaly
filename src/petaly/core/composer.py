# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
import sys
import logging

from petaly.utils.file_handler import FileHandler

logger = logging.getLogger(__name__)


class Composer:
	"""
	Handles composition and manipulation of data objects and pipeline configurations.
	Manages column name normalization, object specifications, and file operations.
	Supports both YAML and JSON formats for pipeline configurations.
	"""

	def __init__(self):
		"""
		Initializes the Composer instance.
		
		Logic:
		1. Initialize file handler with JSON format
		"""
		self.f_handler = FileHandler(file_format='json')
		#self.pipeline = pipeline
		pass


	def normalise_column_name(self, column_name):
		"""
		Normalizes column names by replacing special characters.
		
		Logic:
		1. Replace ':' with '_'
		2. Replace '.' with '_'
		"""
		column_name = column_name.replace(':', '_')
		column_name = column_name.replace('.', '_')

		return column_name

	def get_object_list_from_output_dir(self, pipeline):
		"""
		Gets list of objects from output directory.
		
		Logic:
		1. Get object directories from output path
		2. If data_objects_spec has objects, use those (regardless of all_from_schema)
		3. If data_objects_spec is empty, check all_from_schema
		4. Always exclude objects in exclude_objects (applies regardless of all_from_schema)
		"""
		object_dir_list = self.f_handler.get_all_dir_names(pipeline.output_pipeline_dpath)
		pipeline_object_list = pipeline.data_objects

		# If data_objects_spec has objects, use those (regardless of all_from_schema)
		if len(pipeline_object_list) > 0:
			return_list = self.get_data_objects_intersection(object_dir_list, pipeline_object_list)
		# If data_objects_spec is empty and all_from_schema is True, return all objects
		elif pipeline.all_from_schema:
			return_list = object_dir_list
		# If data_objects_spec is empty and all_from_schema is False, return empty list
		else:
			return_list = []

		# Always exclude objects in exclude_objects (applies regardless of all_from_schema)
		# Even if objects are explicitly specified in data_objects_spec, they will be excluded
		if len(pipeline.exclude_objects) > 0:
			original_count = len(return_list)
			return_list = [obj for obj in return_list if obj not in pipeline.exclude_objects]
			if len(return_list) < original_count:
				logger.debug(f"Excluded {original_count - len(return_list)} object(s) from output directory list based on exclude_objects: {pipeline.exclude_objects}")

		return return_list
	def get_data_objects_intersection(self, first_list, second_list):
		"""
		Gets intersection of two data object lists.
		
		Logic:
		1. Compare each value in first list
		2. Add to result if found in second list
		"""
		return_list = []

		for value in first_list:
			if value in second_list:
				return_list.append(value)

		return return_list

	def save_data_objects(self, pipeline_all_obj, data_objects_spec, pipeline_fpath):
		"""
		Saves data objects specification to pipeline file.
		
		Logic:
		1. Determine file format from extension
		2. Update data objects specification while preserving exclude_objects from load_attributes
		3. Save in single document format (YAML/JSON)
		"""
		if pipeline_all_obj is None:
			return

		# Get the file format from the file extension
		file_extension = self.f_handler.get_file_extensions(pipeline_fpath)[-1].lower()
		file_format = 'yaml' if file_extension == '.yaml' else 'json'

		# Extract pipeline dict and exclude_objects from load_attributes
		if isinstance(pipeline_all_obj, list):
			# Legacy multi-document format (backward compatibility)
			pipeline_dict = pipeline_all_obj[0] if len(pipeline_all_obj) > 0 else {}
			# Get exclude_objects from load_attributes if available
			exclude_objects = pipeline_dict.get('pipeline', {}).get('load_attributes', {}).get('exclude_objects', [])
		else:
			# Single document format (current format)
			pipeline_dict = {'pipeline': pipeline_all_obj.get('pipeline', {})}
			# Get exclude_objects from load_attributes
			exclude_objects = pipeline_all_obj.get('pipeline', {}).get('load_attributes', {}).get('exclude_objects', [])
		
		# Ensure exclude_objects is preserved in load_attributes at the end
		if 'pipeline' not in pipeline_dict:
			pipeline_dict['pipeline'] = {}
		if 'load_attributes' not in pipeline_dict['pipeline']:
			pipeline_dict['pipeline']['load_attributes'] = {}
		# Remove exclude_objects if it exists (to re-add at end)
		if 'exclude_objects' in pipeline_dict['pipeline']['load_attributes']:
			del pipeline_dict['pipeline']['load_attributes']['exclude_objects']
		# Add exclude_objects at the end
		pipeline_dict['pipeline']['load_attributes']['exclude_objects'] = exclude_objects
		
		# Save as single document (no "---" separator)
		config_to_save = {
			'pipeline': pipeline_dict['pipeline'],
			'data_objects_spec': data_objects_spec
		}
		
		if file_format == 'yaml':
			self.f_handler.save_dict_to_yaml(pipeline_fpath, config_to_save, dump_all=False)
		else:
			self.f_handler.save_dict_to_json(pipeline_fpath, config_to_save)

	def get_object_spec_from_array(self, data_objects_spec, object_name):
		"""
		Gets object specification from array by name.
		
		Logic:
		1. Search through data objects array
		2. Return index and spec when found
		"""
		for idx, obj_spec in enumerate(data_objects_spec):

			if object_name == obj_spec.get('object_spec').get('object_name'):
				return idx, obj_spec


	def compose_bucket_object_path(self, bucket_pipeline_prefix, pipeline_name, object_name):
		"""
		Composes bucket object path with prefix.
		
		Logic:
		1. Check if prefix exists
		2. Format prefix with pipeline name
		3. Combine with object name
		"""
		blob_prefix = object_name

		if bucket_pipeline_prefix is not None:
			if bucket_pipeline_prefix.strip() != '':
				blob_prefix = bucket_pipeline_prefix.format(pipeline_name=pipeline_name).strip('/') + '/' + object_name

		return blob_prefix