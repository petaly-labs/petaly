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
import logging
logger = logging.getLogger(__name__)

import sys
from abc import ABC, abstractmethod

from petaly.utils.utils import measure_time
from petaly.core.composer import Composer
from petaly.utils.file_handler import FileHandler
from petaly.core.object_metadata import ObjectMetadata
from petaly.core.type_mapping import TypeMapping
from petaly.core.data_object import DataObject


class DBExtractor(ABC):

	def __init__(self, pipeline):
		super().__init__()

		self.pipeline = pipeline
		self.composer = Composer(pipeline)
		self.f_handler = FileHandler()
		self.m_conf = self.pipeline.m_conf
		self.type_mapping = TypeMapping(pipeline)
		self.object_metadata = ObjectMetadata(pipeline)

		if self.m_conf.set_extractor_paths(self.pipeline.source_connector_id):
			self.connector_extract_to_stmt_fpath = self.m_conf.connector_extract_to_stmt_fpath
			self.connector_metadata_sql_fpath = self.m_conf.connector_metadata_sql_fpath
			self.query_origin = self.f_handler.load_file(self.connector_metadata_sql_fpath)


	@abstractmethod
	def extract_to(self, extractor_obj_conf):
		pass

	@abstractmethod
	def get_query_result(self, meta_query):
		pass

	@abstractmethod
	def compose_extract_to_stmt(self, extract_to_stmt, extract_config) -> dict:
		pass

	@measure_time
	def extract_data(self):
		""" Its export data as csv into pipeline output directory.
		"""

		# 1. Start with cleanup
		self.f_handler.cleanup_dir(self.pipeline.output_pipeline_dpath)

		# 2. compose_extract_scripts
		meta_query = self.compose_meta_query()

		# 3. get meta query result
		meta_query_result = self.execute_meta_query(meta_query)

		# 4. save metadata and export scripts
		object_list = self.object_metadata.process_metadata(meta_query_result)

		# run loop for each object
		for object_name in object_list:

			# 5. get all export scripts and store data into output directory
			extractor_obj_conf = self.get_extractor_obj_conf(object_name)

			# 6. run export data
			self.extract_to(extractor_obj_conf)

	def execute_meta_query(self, meta_query):
		""" compose and execute meta query and store result in json file """
		logger.info("Execute meta-query and create extract scripts")
		if meta_query is not None:
			query_result = self.get_query_result(meta_query)

		else:
			logger.error(
				f"Meta Query for pipeline {self.pipeline.pipeline_name} can not be executed. Review your configuration.")

			query_result = None
		return query_result

	def get_extractor_obj_conf(self, object_name) ->dict:

		extractor_obj_conf = {'object_name': object_name}

		# 1. compose output data dir
		output_metadata_object_dir = self.pipeline.output_object_metadata_dpath.format(object_name=object_name)
		extractor_obj_conf.update({'output_metadata_object_dir': output_metadata_object_dir})

		# 2. get and compose metadata query
		metadata_fpath = self.pipeline.output_object_metadata_fpath.format(object_name=object_name)
		table_metadata = self.f_handler.load_file_as_dict(metadata_fpath, 'json')
		extract_queries_dict = self.compose_extract_queries(table_metadata)
		extractor_obj_conf.update(extract_queries_dict)

		# 3. load stmt_extract_to.txt and transform it in later stage
		extract_to_stmt = self.f_handler.load_file(self.connector_extract_to_stmt_fpath)
		extract_to_stmt = self.compose_extract_to_stmt(extract_to_stmt, extractor_obj_conf)
		extractor_obj_conf.update({'extract_to_stmt': extract_to_stmt})

		# 4.  save extract_to_stmt under output_extract_to_file_fpath
		output_extract_to_stmt_fpath = self.pipeline.output_extract_to_stmt_fpath.format(object_name=object_name)

		extractor_obj_conf.update({'extract_to_stmt_fpath': output_extract_to_stmt_fpath})

		self.f_handler.save_file(output_extract_to_stmt_fpath, extract_to_stmt)

		# 5. create output_file_path
		output_fpath = self.get_local_output_path(object_name)
		extractor_obj_conf.update({'output_fpath': output_fpath})

		logger.info(f"Config for data extract: {extractor_obj_conf}")
		return extractor_obj_conf

	def get_local_output_path(self, object_name):

		object_dpath = self.pipeline.output_object_data_dpath.format(object_name=object_name)

		self.f_handler.make_dirs(object_dpath)
		output_fpath = os.path.join(object_dpath, object_name + '.csv')
		return output_fpath

	def compose_meta_query(self):
		""" Its compose a meta query by using a meta query file and adding schema, tables and column definitions

		:return:
		"""
		logger.info("Compose data source meta query")

		if self.pipeline.data_object_main_config.get('use_data_objects_spec') is False:
			table_stmt = ''
		else:

			if len(self.pipeline.data_objects)==0:
				logger.warning(f"Pipeline {self.pipeline.pipeline_name} in {self.pipeline.pipeline_fpath} wasn't specified properly. If use_data_objects_spec is true the data_objects_spec: [] should has an object specification")
				sys.exit()

			table_stmt = 'AND tb.table_name IN ({tbl_list})'
			table_string = ''
			for tbl in self.pipeline.data_objects:
				table_string += "'" + tbl + "',"

			table_string = table_string.rstrip(',')

			table_stmt = table_stmt.format(tbl_list=table_string)

		source_schema = self.pipeline.source_attr.get('database_schema')

		if source_schema is None:
			if self.f_handler.check_dict_key_exist(self.pipeline.source_attr, 'database_schema'):
				logger.warning(f"A source database schema wasn't specified. To continue, specify database_schema in pypeline.yaml ")
				sys.exit()

			source_schema = self.pipeline.source_attr.get('database_name')

		meta_query = self.query_origin.format(schema=source_schema, table_statement_list=table_stmt)

		return meta_query

	@measure_time
	def compose_extract_queries(self, dict_obj):
		""" Its compose an extract queries using object dictionary
		"""
		extract_obj_conf = {}

		if dict_obj.get('source_object_name') is not None:
			column_list = ''

			transformation = self.type_mapping.get_extractor_type_transformer()

			col_obj = dict_obj.get('columns')

			for idx, val in enumerate(col_obj):

				column_transformation = transformation.get(val['data_type'])

				if column_transformation is not None:
					column_list += column_transformation.format(column_name=val['column_name']) + ","
				else:
					column_list += f" {self.db_connector.column_quotes}{val['column_name']}{self.db_connector.column_quotes},"
					#columns += self.normalise_column_name(val['column_name']) + ","

			column_list = column_list.rstrip(',')

			extract_obj_conf.update({
								 'source_schema_name':dict_obj['source_schema_name'],
								 'source_object_name':dict_obj['source_object_name'],
								 'column_list':column_list})

			return extract_obj_conf

	def get_data_object(self, object_name):
		return DataObject(self.pipeline, object_name)
