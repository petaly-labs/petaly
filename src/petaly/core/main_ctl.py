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

from petaly.sysconfig.logger import setup_logging
from petaly.core.pipeline import Pipeline

class MainCtl():
    def __init__(self, main_config):
        self.m_conf = main_config
        setup_logging(self.m_conf.logging_config_fpath, self.m_conf.logs_base_dpath)

    def run_pipeline(self, pipeline, run_endpoint, object_name_list):
        """ Call this function to run pipeline source and target
        """
        #pprint(vars(pipeline))
        pipeline_name = pipeline.pipeline_name
        logger.info(f"Start: Pipeline with pipeline-name {pipeline_name} started.")
        #pipeline = Pipeline(pipeline_name, self.m_conf)

        if pipeline.is_enabled is True:
            if object_name_list is not None:
                pipeline.data_objects = object_name_list.split(',')

            if run_endpoint is None or run_endpoint == 'source':
                self.run_source(pipeline)

            if run_endpoint is None or run_endpoint == 'target':
                self.run_target(pipeline)

            logger.info(f"End: Pipeline with pipeline-name {pipeline_name} ended.")
        else:
            logger.info(f"Pipeline with pipeline-name {pipeline_name} is disabled. Check the parameter is_enabled  in pipeline.yaml file")

    ####################### run source ####################################

    def run_source(self, pipe):
        """ Call this function to run pipeline source part
        """
        logger.info("Load source config")

        class_obj = self.m_conf.get_extractor_class(pipe.source_connector_id)
        # run extraction
        if class_obj is not None:
            extractor = class_obj(pipe)
            logger.info(f"Extract connector-id: {pipe.source_connector_id}")
            extractor.extract_data()
        else:
            logger.info(f"Extractor with connector-id: {pipe.source_connector_id} can't initialized.")

    ####################### run targets ####################################

    def run_target(self, pipe):
        """ Call this function to run pipeline target part
        """
        logger.info("Load target config")

        class_obj = self.m_conf.get_loader_class(pipe.target_connector_id)

        # run loader
        if class_obj is not None:
            loader = class_obj(pipe)
            logger.info(f"Load target connector id: {pipe.target_connector_id}")
            loader.load_data()

        else:
            logger.info(f"Loader with connector-id: {pipe.target_connector_id} can't initialized.")