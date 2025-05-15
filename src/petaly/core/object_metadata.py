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

import logging
logger = logging.getLogger(__name__)

import sys

from petaly.core.data_object import DataObject
from petaly.utils.file_handler import FileHandler


class ObjectMetadata():
    """Class for managing object metadata.
    
    This class handles the creation, processing, and storage of metadata for data objects.
    It manages column definitions, data types, and object settings.
    
    Key responsibilities:
    - Processes and formats metadata from queries
    - Manages column metadata and data types
    - Saves and loads metadata files
    - Handles object settings and configurations
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.f_handler = FileHandler()
        self.object_metadata_dict = {
                                "source_schema_name": None,
                                "source_object_name": None,
                                "source_connector_type": None,
                                "output_file_format": None,
                                "object_settings": {},
                                "columns":[]
                                }

        pass

    def save_table_metadata(self, meta_table):
        """Saves table metadata to a file.
        
        The metadata is saved in JSON format to the object's metadata directory.
        """
        object_name = meta_table.get('source_object_name')

        source_object_fpath = self.pipeline.output_object_metadata_fpath.format(object_name=object_name)
        # format and save meta-data of each table in folder
        logger.debug(f"Format and save metadata for table {object_name} in {source_object_fpath}")
        self.f_handler.save_dict_to_file(source_object_fpath, meta_table, 'json')

    def compose_objects_meta_from_query(self, meta_query_result):
        """Composes object metadata from query results.
        
        The method processes query results to create formatted object metadata including:
        - Source object information
        - Column definitions
        - Object settings
        - Excluded columns
        """
        distinct_object_list = []
        formated_object_meta_list = []

        for i, val in enumerate(meta_query_result):

            schema_name = val.get("source_schema_name")
            source_object_name = val.get("source_object_name")

            if source_object_name not in distinct_object_list:
                distinct_object_list.append(source_object_name)
                new_dict = self.object_metadata_dict.copy()
                new_dict.update({'source_schema_name': schema_name, 'source_object_name': source_object_name, 'columns': []})
                formated_object_meta_list.append(new_dict)

        for i, val in enumerate(formated_object_meta_list):
            object_name = formated_object_meta_list[i].get('source_object_name')

            data_object = self.get_data_object(object_name)
            object_settings = data_object.object_settings
            formated_object_meta_list[i].update({'object_settings': object_settings})
            exclude_columns = [] if data_object.exclude_columns is None else data_object.exclude_columns

            for idx, value in enumerate(meta_query_result):

                table_name = meta_query_result[idx].get('source_object_name')
                if table_name == object_name:
                    if value.get('column_name') not in exclude_columns:
                        column_metadata = self.compose_column_metadata( column_name=value.get('column_name'),
                                                                    ordinal_position=value.get('ordinal_position'),
                                                                    is_nullable=value.get('is_nullable'),
                                                                    data_type=value.get('data_type'),
                                                                    character_maximum_length=value.get('character_maximum_length'),
                                                                    numeric_precision=value.get('numeric_precision'),
                                                                    numeric_scale=value.get('numeric_scale'),
                                                                    primary_key=value.get('primary_key'))

                        formated_object_meta_list[i]['columns'].append(column_metadata)

        return formated_object_meta_list

    def compose_object_meta_from_file(self, object_name, columns_metadata_arr):
        """ Its creates a metadata file with all the attributes needed to recreate a table on the target.
        """

        self.object_metadata_dict.update({'source_object_name': object_name})
        self.object_metadata_dict.update({'source_connector_type': self.pipeline.source_attr.get("connector_type")})
        self.object_metadata_dict.update({'output_file_format': 'csv'})

        data_object = self.get_data_object(object_name)
        object_settings = data_object.object_settings
        self.object_metadata_dict.update({'object_settings': object_settings})
        exclude_columns = [] if data_object.exclude_columns is None else data_object.exclude_columns

        columns_arr = []
        for idx, value in enumerate(columns_metadata_arr):
            if value.get('column_name') not in exclude_columns:
                column_metadata = self.compose_column_metadata(
                                                column_name=str(value.get('column_name')),
                                                ordinal_position=str(idx+1),
                                                is_nullable='YES',
                                                data_type=value.get('data_type'),
                                                character_maximum_length=None,
                                                numeric_precision=None,
                                                numeric_scale=None,
                                                primary_key=None)

                columns_arr.append(column_metadata)
        self.object_metadata_dict.update({'columns': columns_arr})
        return self.object_metadata_dict

    def process_metadata(self, meta_query_result):
        """Processes metadata query results and saves them.
        
        The method processes each object's metadata and saves it to files.
        If the meta query result is empty, it raises a SystemExit error.
        """
        object_list = []
        if meta_query_result is not None:
            for meta_table in self.compose_objects_meta_from_query(meta_query_result):
                object_name = meta_table.get('source_object_name')
                object_list.append(object_name)
                self.save_table_metadata(meta_table)

        else:
            logger.error(f"Meta Query for piepline {self.pipeline.pipeline_name} is empty. Review your configuration.")
            sys.exit()
        return object_list

    def compose_column_metadata(self, column_name, ordinal_position, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale, primary_key):
        """Composes metadata for a single column.
        
        The method creates a column metadata dictionary containing:
        - Column name and position
        - Nullability
        - Data type and precision
        - Primary key information
        """

        column_meta = {}
        column_meta.update({'column_name': column_name})
        column_meta.update({'ordinal_position': ordinal_position})
        column_meta.update({'is_nullable': is_nullable})
        column_meta.update({'data_type': data_type})
        column_meta.update({'character_maximum_length': self.__replace_nan_to_none(character_maximum_length)})
        column_meta.update({'numeric_precision':  self.__replace_nan_to_none(numeric_precision)})
        column_meta.update({'numeric_scale':  self.__replace_nan_to_none(numeric_scale)})
        column_meta.update({'primary_key': primary_key})
        return column_meta

    def get_column_metadata_dict(self):
        """Gets the default structure for column metadata.
        
        Creates and returns an empty column metadata dictionary with all
        fields initialized to None.
        """
        return {
                    'column_name':None,
                    'ordinal_position': None,
                    'is_nullable': None,
                    'data_type': None,
                    'character_maximum_length': None,
                    'numeric_precision': None,
                    'numeric_scale': None,
                    'primary_key': None
                }

    def __replace_nan_to_none(self, value):
        """Helper function that replaces NaN or nan values with None in metadata results."""
        return_val = value if str(value).lower() != 'nan' else None
        return return_val

    def get_data_object(self, object_name):
        """Gets a DataObject instance for the specified object.
        
        Creates and returns a DataObject instance containing the object's
        configuration and settings.
        """
        return DataObject(self.pipeline, object_name)

