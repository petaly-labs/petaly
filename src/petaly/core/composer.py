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


class Composer():

	def __init__(self, pipeline):
		self.f_handler = FileHandler(file_format='json')
		self.pipeline = pipeline
		pass


	def normalise_column_name(self, column_name):
		""" It replaces unusable characters in column name. e.g. ':' will be replaced with '_'
		"""
		column_name = column_name.replace(':', '_')
		column_name = column_name.replace('.', '_')

		return column_name

	#def get_data_object(self, object_name):
	#	return DataObject(self.pipeline, object_name)

	def _deprecated_get_data_objects(self):
		data_objects_spec = self.pipeline.data_objects_spec
		object_name_list = []

		for object in data_objects_spec:
			if object != None:
				object_name_list.append(object.get('object_name'))

		return object_name_list

	def get_data_object_list(self):
		data_objects_spec = self.pipeline.data_objects_spec
		object_name_list = []

		for data_object in data_objects_spec.get('data_objects_spec'):
			if data_object != None:
				object_name_list.append(data_object.get('object_name'))

		pipeline_data_objects = self.pipeline.data_objects
		return self.get_data_objects_intersection(object_name_list, pipeline_data_objects)

	def get_object_list_from_output_dir(self):
		object_dir_list = self.f_handler.get_all_dir_names(self.pipeline.output_pipeline_dpath)
		pipeline_object_list = self.pipeline.data_objects

		return self.get_data_objects_intersection(object_dir_list, pipeline_object_list)

	def get_data_objects_intersection(self, data_objects, pipeline_data_objects):
		""" This function check if data_object defined in pipeline.yaml data_objects corresponded with pipeline.yaml data_objects_spec.
		If pipeline.json the data_objects is None or the first element is set to None all objects from the database_schema will be loaded.
		In case it is a file export definition of objects is required

		:return:
		"""
		return_list = []

		if self.pipeline.data_object_main_config.get('use_data_objects_spec') is False:
			# return the first list without modification
			return_list = data_objects
		else:
			for value in data_objects:
				if value in pipeline_data_objects:
					return_list.append(value)

		return return_list



