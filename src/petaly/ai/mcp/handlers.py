# src/petaly/mcp/handlers.py

import logging
import json
import os
from typing import Dict, List, Any, Optional

from petaly.core.pipeline import Pipeline
from petaly.core.main_ctl import MainCtl
from petaly.sysconfig.main_config import MainConfig
from petaly.utils.file_handler import FileHandler

logger = logging.getLogger(__name__)


class PetalyFunctionHandlers:
    """Handlers for Petaly functions called via MCP."""

    def __init__(self, main_config=None):
        """Initialize the handlers with Petaly configuration."""
        self.m_conf = MainConfig() if main_config is None else main_config
        self.m_conf.set_workspace_dpaths()
        self.m_conf.set_global_settings()
        self.file_handler = FileHandler()

    def list_pipelines(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """List all available pipelines."""
        try:
            pipelines = self.file_handler.get_all_dir_names(self.m_conf.pipeline_base_dpath)
            return {
                "status": "success",
                "pipelines": pipelines
            }
        except Exception as e:
            logger.error(f"Error listing pipelines: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to list pipelines: {str(e)}"
            }

    def create_pipeline(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new pipeline with specified configuration."""
        try:
            # Extract parameters
            pipeline_name = params.get("pipeline_name")
            source_type = params.get("source_type")
            source_config = params.get("source_config", {})
            target_type = params.get("target_type")
            target_config = params.get("target_config", {})
            objects = params.get("objects", [])

            # Import the AI agent's planner for pipeline generation
            from petaly.ai.agent.pipeline_planner import PipelinePlanner
            from petaly.ai.agent.llm_connector import get_llm_connector

            # Create a simplified plan
            plan = {
                "pipeline_name": pipeline_name,
                "source": {
                    "connector_type": source_type,
                    "details": source_config
                },
                "target": {
                    "connector_type": target_type,
                    "details": target_config
                },
                "data_objects": objects
            }

            # Use the planner to generate config (without requiring an actual LLM)
            planner = PipelinePlanner(None, self.m_conf)
            pipeline_config = planner._build_pipeline_config_from_plan(plan)

            # Save the configuration
            pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
            if not self.file_handler.is_dir(pipeline_dpath):
                self.file_handler.make_dirs(pipeline_dpath)

            pipeline_fpath = os.path.join(pipeline_dpath, self.m_conf.pipeline_fname)
            self.file_handler.save_dict_to_yaml(pipeline_fpath, pipeline_config, dump_all=True)

            return {
                "status": "success",
                "message": f"Pipeline '{pipeline_name}' created successfully",
                "pipeline_path": pipeline_fpath
            }

        except Exception as e:
            logger.error(f"Error creating pipeline: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to create pipeline: {str(e)}"
            }

    def run_pipeline(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Run an existing pipeline."""
        try:
            # Extract parameters
            pipeline_name = params.get("pipeline_name")
            endpoint = params.get("endpoint", "")  # Empty string means both
            objects = params.get("objects", [])

            # Check if pipeline exists
            pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
            if not self.file_handler.is_dir(pipeline_dpath):
                return {
                    "status": "error",
                    "message": f"Pipeline '{pipeline_name}' not found"
                }

            # Initialize pipeline
            pipeline = Pipeline(pipeline_name, self.m_conf)

            # Create main controller
            main_ctl = MainCtl(self.m_conf)

            # Convert objects list to comma-separated string if needed
            objects_str = None
            if objects:
                objects_str = ",".join(objects)

            # Run the pipeline
            main_ctl.run_pipeline(pipeline, endpoint, objects_str)

            return {
                "status": "success",
                "message": f"Pipeline '{pipeline_name}' executed successfully"
            }

        except Exception as e:
            logger.error(f"Error running pipeline: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to run pipeline: {str(e)}"
            }