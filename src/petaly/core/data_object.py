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

import logging
logger = logging.getLogger(__name__)

import sys

class DataObject:
    def __init__(self, pipeline, object_name):

        data_objects = pipeline.data_objects_spec
        self.pipeline_data_object_dir = pipeline.output_object_data_dpath.format(object_name=object_name)
        self.data_object_attr = self.get_object_spec(data_objects, object_name)
        if self.data_object_attr == {}:
            logger.info(f"The specification for data object {object_name} wasn't found in the section data_objects_spec[] in the pipeline: {pipeline.pipeline_fpath}")

            if pipeline.source_connector_id in ('csv'):
                logger.info(
                    f"For {pipeline.source_connector_id} extract the paramaeters data_objects[] and data_objects_spec[] are required. Use: python -m petaly init data-objects -p {pipeline.pipeline_name}")

                sys.exit()

            return self.get_default_object_spec(pipeline, object_name)

        # Source ATTRIBUTES
        self.object_name = self.data_object_attr.get('object_name')
        self.excluded_columns = self.data_object_attr.get('object_attributes').get('excluded_columns')
        self.load_type = self.data_object_attr.get('object_attributes').get('load_type')
        self.load_batch_size = self.data_object_attr.get('object_attributes').get('load_batch_size')
        self.incremental_load_column = self.data_object_attr.get('object_attributes').get('incremental_load_column')

        skip_leading_rows = self.data_object_attr.get('object_attributes').get('skip_leading_rows')
        self.skip_leading_rows = 1 if skip_leading_rows is None else skip_leading_rows

        self.file_format = self.data_object_attr.get('object_attributes').get('file_format')
        self.file_dir = self.data_object_attr.get('object_attributes').get('file_dir')
        self.file_name_list = self.data_object_attr.get('object_attributes').get('file_name_list')
        self.target_object_name =  self.data_object_attr.get('object_attributes').get('target_object_name')
        self.recreate_target_object = self.data_object_attr.get('object_attributes').get('recreate_target_object')
        if self.recreate_target_object is not True:
            self.recreate_target_object = False
        self.target_file_format = self.data_object_attr.get('object_attributes').get('target_file_format')
        self.target_file_dir = self.data_object_attr.get('object_attributes').get('target_file_dir')

    def get_object_spec(self, data_objects, object_name):

        data_object_dict = {}
        for data_object in data_objects.get('data_objects_spec'):
            if data_object != None:
                if data_object.get('object_name') == object_name:
                    data_object_dict = data_object
        return data_object_dict

    def get_default_object_spec(self, pipeline, object_name):
        self.object_name = object_name
        self.target_object_name = None
        self.recreate_target_object = False
        self.excluded_columns = None
        self.load_type = pipeline.data_object_main_config.get('preferred_load_type')
        self.column_for_incremental_load = None
        self.batch_size = None
        self.file_format = pipeline.data_object_main_config.get('data_transition_format')
        self.file_dir = None
        self.file_name_list = None
        self.skip_leading_rows = 1
        self.target_file_format = pipeline.data_object_main_config.get('data_transition_format')
        self.target_file_dir = None
