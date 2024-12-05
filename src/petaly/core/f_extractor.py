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

import os, sys
from pyarrow import csv, parquet

from petaly.core.composer import Composer
from petaly.utils.file_handler import FileHandler
from petaly.core.object_metadata import ObjectMetadata
from petaly.core.data_object import DataObject


class FExtractor():

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.composer = Composer(pipeline)
        self.f_handler = FileHandler()
        self.object_metadata = ObjectMetadata(pipeline)
        self.object_default_settings = pipeline.data_attributes.get("object_default_settings")
        pass


    def save_metadata_into_file(self,meta_table):
        """ Its save a table result as a metadata into file
        """
        object_name = meta_table.get('source_object_name')
        source_object_fpath = self.pipeline.output_object_metadata_fpath.format(object_name=object_name)
        logger.info(f"Format and save metadata for table {meta_table.get('source_object_name')} in {source_object_fpath}")
        self.f_handler.save_dict_to_file(source_object_fpath, meta_table, 'json')

    def extract_metadata_from_parquet_file(self, parquet_fpath):
        """ Its extract metadata from parquet file and also change the dict from parquet.read_metadata in more readable format.
        """

        pq_metadata = parquet.read_metadata(parquet_fpath).to_dict()
        pq_meta_columns = pq_metadata.get('row_groups')[0].get('columns')
        column_names = parquet.read_schema(parquet_fpath).names
        column_data_types = parquet.read_schema(parquet_fpath).types

        pq_columns_metadata_arr = []

        for i in range(len(pq_meta_columns)):
            column_dict = {}
            c_idx = column_names.index(pq_meta_columns[i].get('path_in_schema'))

            # column name
            column_name = pq_meta_columns[i].get('path_in_schema')
            column_dict.update({'column_name': column_name})

            # column type
            column_type = str(column_data_types[c_idx])
            if column_type == 'null':
                column_type = 'string'
            column_dict.update({'data_type': str(column_data_types[c_idx])})

            column_dict.update({'physical_type': str(pq_meta_columns[i].get('physical_type'))})
            column_dict.update({'is_stats_set': str(pq_meta_columns[i].get('is_stats_set'))})
            column_dict.update({'encodings': str(pq_meta_columns[i].get('encodings'))})
            column_dict.update({'compression': str(pq_meta_columns[i].get('compression'))})

            if pq_meta_columns[i].get('is_stats_set') == True:

                column_dict.update({'statistics_distinct_count': str(pq_meta_columns[i].get('statistics').get('distinct_count')) })
                column_dict.update({'statistics_min': str(pq_meta_columns[i].get('statistics').get('min'))})
                column_dict.update({'statistics_max': str(pq_meta_columns[i].get('statistics').get('max'))})
                column_dict.update({'statistics_distinct_count': str(pq_meta_columns[i].get('statistics').get('distinct_count'))})
                column_dict.update({'statistics_null_count': str(pq_meta_columns[i].get('statistics').get('null_count'))})

            else:
                column_dict.update({'statistics_distinct_count': None})
                column_dict.update({'statistics_min': None})
                column_dict.update({'statistics_max': None})
                column_dict.update({'statistics_num_values': None})
                column_dict.update({'statistics_null_count': None})


            pq_columns_metadata_arr.append(column_dict)

        return pq_columns_metadata_arr

    def compose_metadata_file(self, parquet_fpath, object_name):
        """ Its creates a metadata file with all the attributes needed to recreate a table on the target.
        """
        pq_columns_metadata_arr = self.extract_metadata_from_parquet_file(parquet_fpath)

        if len(pq_columns_metadata_arr) == 0:
            logger.warning("The process has failed to extract the metadata from the file, the column definition is empty.")
            sys.exit()
        meta_table = self.object_metadata.compose_object_meta_from_file(object_name, pq_columns_metadata_arr)
        return meta_table

    def describe_parquet_metadata(self, parquet_fpath):
        """ Its print a parquet metadata from parquet file to stdout
        """
        # extract metadata from parquet file
        pq_columns_metadata_arr = self.extract_metadata_from_parquet_file(parquet_fpath)
        header_str = ""
        col_separator = " | "
        output_text = ""

        # exit if array is empty
        if len(pq_columns_metadata_arr) == 0:
            logger.warning("pq_columns_metadata_arr is empty.")
            sys.exit()

        # create the header line
        for key in pq_columns_metadata_arr[0]:
            header_str += key + col_separator
        output_text += header_str + '\n'
        output_text += '-'*len(header_str)  + '\n'

        # compose output row by row
        for i in range(len(pq_columns_metadata_arr)):
            row_str = ""
            for key in pq_columns_metadata_arr[i]:
                row_str += str(pq_columns_metadata_arr[i].get(key)) + col_separator
            output_text += row_str + '\n'
        return output_text

    def analyse_file_structure(self, source_file, data_object_dict, file_format):
        """ Analyse the csv file to determine the column format.
        Transform the source file to parquet format to read the column data type.
        Finally, store the result of this discovery to the metadata file
        """
        is_file = self.f_handler.is_file(source_file)
        if is_file == False:
            logger.error(f"Source file {source_file} doesn't exists")
            sys.exit()

        if  self.f_handler.check_file_extension(source_file, file_format) == False:
            logger.error(f"File format doesn't match")
            sys.exit()

        # transform csv to parquet
        parse_options = csv.ParseOptions(delimiter=self.object_default_settings.get("columns_delimiter"))
        file_data = csv.read_csv(source_file, parse_options=parse_options)

        pq_file_name = self.f_handler.replace_file_extension(source_file, '.parquet')
        parquet_fpath = os.path.join(self.pipeline.output_object_data_dpath.format(object_name=data_object_dict.object_name), pq_file_name)
        parquet.write_table(file_data, parquet_fpath)

        return parquet_fpath

    def get_data_object_list(self):
        return self.composer.get_data_object_list()

    def get_data_object(self, object_name):
        return DataObject(self.pipeline, object_name)