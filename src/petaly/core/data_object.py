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
import logging
logger = logging.getLogger(__name__)

class DataObject:
    def __init__(self, pipeline, object_name):

        data_objects = pipeline.data_objects_spec
        self.pipeline_data_object_dir = pipeline.output_object_data_dpath.format(object_name=object_name)
        self.data_objects_spec_mode = pipeline.data_attributes.get('data_objects_spec_mode')
        self.object_settings = self.format_object_default_settings(pipeline.object_default_settings)
        self.object_settings.update({'cleanup_linebreak_in_fields': False})

        data_object_spec = self.get_object_spec(data_objects, object_name)
        logger.debug(f"Data object spec: {data_object_spec}")
        if data_object_spec == {}:

            if self.data_objects_spec_mode == 'only':
                logger.info(
                    f"For {pipeline.source_connector_id} extract the parameters data_objects_spec_mode=only and specification in the data_objects_spec[] are required. Use python -m petaly init -p {pipeline.pipeline_name} --object_name table1,table2 -c your_config_dir/petaly.ini")
                sys.exit()

            elif self.data_objects_spec_mode in ('ignore', 'prefer'):
                if pipeline.source_connector_id in ('csv'):
                    logger.info(
                        f"In case your source is csv, the parameters data_objects_spec_mode should be set to only and require the specification in the data_objects_spec[]."
                        f"\ndata_objects_spec_mode=only"
                        f"\nCheck pipeline under: {pipeline.pipeline_fpath}")

                    sys.exit()

            return self.set_default_object_spec(pipeline, object_name)

        self.object_name = data_object_spec.get('object_spec').get('object_name')
        self.destination_object_name = data_object_spec.get('object_spec').get('destination_object_name')
        self.recreate_destination_object = True if data_object_spec.get('object_spec').get('recreate_destination_object') is True else False
        self.exclude_columns = data_object_spec.get('object_spec').get('exclude_columns')
        self.object_source_dir = data_object_spec.get('object_spec').get('object_source_dir')
        self.file_names = data_object_spec.get('object_spec').get('file_names')
        cleanup_linebreak_in_fields = data_object_spec.get('object_spec').get('cleanup_linebreak_in_fields')
        self.object_settings.update({'cleanup_linebreak_in_fields': cleanup_linebreak_in_fields})

    def to_dict(self) -> dict:
        return {key: value for key, value in self.__dict__.items()}

    def get_object_spec(self, data_objects, object_name):

        return_object_spec = {}
        for object_spec in data_objects.get('data_objects_spec'):
            if object_spec != None:
                if object_spec.get('object_spec').get('object_name') == object_name:
                    return_object_spec = object_spec.copy()
        return return_object_spec

    def set_default_object_spec(self, pipeline, object_name):
        self.object_name = object_name
        self.destination_object_name = None
        self.recreate_destination_object = False
        self.cleanup_linebreak_in_fields = False
        self.exclude_columns = [None]
        self.object_source_dir = None
        self.file_names = [None]

    def format_object_default_settings(self, object_default_settings):
        """
        """
        object_settings = object_default_settings.copy()
        # 1. Header

        header = True if object_settings.get('header') is True else False
        object_settings.update({'header': header})

        # 2. Columns delimiter
        columns_delimiter = object_settings.get('columns_delimiter')
        #object_settings.update({'columns_delimiter_origin': columns_delimiter})
        if columns_delimiter == "\\t":
            object_settings.update({'columns_delimiter': '\t'})

        return object_settings
