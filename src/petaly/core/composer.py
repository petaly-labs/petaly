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

		data_object_list = []
		# make a list of new added objects with the same index order
		for idx, obj in enumerate(data_objects_spec):
			data_object_list.insert(idx, obj.get('object_spec').get('object_name'))

		# if object_name is in new in the list replace it in the pipeline, else do nothing
		if pipeline_all_obj[1].get('data_objects_spec') is not None:
			if len(pipeline_all_obj[1].get('data_objects_spec')) > 0:
				if pipeline_all_obj[1].get('data_objects_spec')[0] is not None:

					for idx, object_spec in enumerate(pipeline_all_obj[1].get('data_objects_spec')):
						if object_spec.get('object_spec').get('object_name') in data_object_list:
							ind, obj = self.get_object_spec_from_array(data_objects_spec,
																	   object_spec.get('object_spec').get(
																		   'object_name'))
							pipeline_all_obj[1].get('data_objects_spec')[idx] = obj
							data_objects_spec.pop(ind)
							data_object_list.pop(ind)

		# add the entire new objects at the end of the pipeline
		if len(data_object_list) > 0:
			for i, obj in enumerate(data_objects_spec):
				pipeline_all_obj[1].get('data_objects_spec').append(obj)

		self.f_handler.backup_file(pipeline_fpath)
		self.f_handler.save_dict_to_yaml(pipeline_fpath, pipeline_all_obj, dump_all=True)

	def get_object_spec_from_array(self, data_objects_spec, object_name):

		for idx, obj_spec in enumerate(data_objects_spec):

			if object_name == obj_spec.get('object_spec').get('object_name'):
				return idx, obj_spec


