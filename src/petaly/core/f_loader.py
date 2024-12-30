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

import time
from abc import ABC, abstractmethod

from petaly.core.composer import Composer
from petaly.utils.file_handler import FileHandler

from petaly.core.data_object import DataObject

class FLoader(ABC):

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.composer = Composer()
        self.f_handler = FileHandler()
        pass

    @abstractmethod
    def load_from(self, loader_obj_conf):
        pass

    def load_data(self):

        logger.info(f"[--- Load into {self.pipeline.target_connector_id} ---]")
        start_total_time = time.time()

        if self.pipeline.data_attributes.get("data_objects_spec_mode") == 'only':
            object_list = self.pipeline.data_objects
        else:
            #object_list = self.f_handler.get_all_dir_names(self.pipeline.output_pipeline_dpath)
            object_list = self.composer.get_object_list_from_output_dir(self.pipeline)

        for object_name in object_list:

            logger.info(f"Load object: {object_name} started...")
            start_time = time.time()

            loader_obj_conf = {}
            loader_obj_conf.update({'object_name': object_name})

            output_metadata_object_dir = self.pipeline.output_object_metadata_dpath.format(object_name=object_name)
            loader_obj_conf.update({'output_metadata_object_dir': output_metadata_object_dir})

            output_data_object_dir = self.pipeline.output_object_data_dpath.format(object_name=object_name)
            loader_obj_conf.update({'output_data_object_dir': output_data_object_dir})

            output_load_from_stmt_fpath = self.pipeline.output_load_from_stmt_fpath.format(object_name=object_name)
            loader_obj_conf.update({'load_from_stmt_fpath': output_load_from_stmt_fpath})

            self.load_from(loader_obj_conf)

            end_time = time.time()
            logger.info(f"Load object: {object_name} completed | time: {round(end_time - start_time, 2)}s")

        end_total_time = time.time()
        logger.info(f"Load completed, duration: {round(end_total_time - start_total_time, 2)}s")

    def get_data_object(self, object_name):
        return DataObject(self.pipeline, object_name)
