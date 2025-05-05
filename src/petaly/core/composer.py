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
import sys

from petaly.utils.file_handler import FileHandler


class Composer():

	def __init__(self):
		self.f_handler = FileHandler(file_format='json')
		#self.pipeline = pipeline
		pass


	def normalise_column_name(self, column_name):
		""" It replaces unusable characters in column name. e.g. ':' will be replaced with '_'
		"""
		column_name = column_name.replace(':', '_')
		column_name = column_name.replace('.', '_')

		return column_name

	def get_object_list_from_output_dir(self, pipeline):
		""" This function check if object defined in the second list pipeline_data_objects in pipeline.yaml -> data_objects_spec corresponds
				with the first list data_objects which is usually a folder structure in output directory.
		"""
		object_dir_list = self.f_handler.get_all_dir_names(pipeline.output_pipeline_dpath)
		pipeline_object_list = pipeline.data_objects

		if pipeline.data_objects_spec_mode in ("ignore","prefer"):
			return_list = object_dir_list
		else:
			return_list = self.get_data_objects_intersection(object_dir_list, pipeline_object_list)

		return return_list
	def get_data_objects_intersection(self, first_list, second_list):
		"""
		"""
		return_list = []

		for value in first_list:
			if value in second_list:
				return_list.append(value)

		return return_list

	def save_data_objects(self, pipeline_all_obj, data_objects_spec, pipeline_fpath):
		""" Save data objects specification to pipeline file
		"""
		if pipeline_all_obj is None:
			return

		# Get the file format from the file extension
		file_extension = self.f_handler.get_file_extensions(pipeline_fpath)[-1].lower()
		file_format = 'yaml' if file_extension == '.yaml' else 'json'

		# Update the data objects specification
		pipeline_all_obj[1]['data_objects_spec'] = data_objects_spec

		# Save in the appropriate format
		if file_format == 'yaml':
			self.f_handler.save_dict_to_yaml(pipeline_fpath, pipeline_all_obj, dump_all=True)
		else:
			# For JSON, we need to combine both documents into one
			combined_config = {
				'pipeline': pipeline_all_obj[0]['pipeline'],
				'data_objects_spec': pipeline_all_obj[1]['data_objects_spec']
			}
			self.f_handler.save_dict_to_json(pipeline_fpath, combined_config)

	def get_object_spec_from_array(self, data_objects_spec, object_name):

		for idx, obj_spec in enumerate(data_objects_spec):

			if object_name == obj_spec.get('object_spec').get('object_name'):
				return idx, obj_spec


	def compose_bucket_object_path(self, bucket_pipeline_prefix, pipeline_name, object_name):
		"""
		"""
		blob_prefix = object_name

		if bucket_pipeline_prefix is not None:
			if bucket_pipeline_prefix.strip() != '':
				blob_prefix = bucket_pipeline_prefix.format(pipeline_name=pipeline_name).strip('/') + '/' + object_name

		return blob_prefix