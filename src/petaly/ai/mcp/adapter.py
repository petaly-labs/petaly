# src/petaly/mcp/adapter.py

import logging
import json
from typing import Dict, List, Any, Optional, Union, Callable

from petaly.ai.mcp.schema import PETALY_TOOL_SCHEMAS
from petaly.ai.mcp.handlers import PetalyFunctionHandlers
from petaly.sysconfig.main_config import MainConfig
logger = logging.getLogger(__name__)


class PetalyMCPAdapter:
    """Adapter for using Petaly with Model Context Protocol."""

    def __init__(self, main_config=None):
        """Initialize the MCP adapter."""
        self.m_conf = MainConfig() if main_config is None else main_config
        self.handlers = PetalyFunctionHandlers(self.m_conf)

        # Map function names to handler methods
        self.function_map = {
            "list_pipelines": self.handlers.list_pipelines,
            "create_pipeline": self.handlers.create_pipeline,
            "run_pipeline": self.handlers.run_pipeline
        }

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        """Return the tool schemas for Petaly functions."""
        return PETALY_TOOL_SCHEMAS

    def handle_function_call(self, function_name: str, arguments: str) -> Dict[str, Any]:
        """
        Handle a function call from an MCP request.

        Args:
            function_name: Name of the function to call
            arguments: JSON string containing function arguments

        Returns:
            Dictionary containing the function result
        """
        try:
            # Parse arguments
            params = json.loads(arguments)

            # Check if function exists
            if function_name not in self.function_map:
                return {
                    "status": "error",
                    "message": f"Unknown function: {function_name}"
                }

            # Call the function
            handler = self.function_map[function_name]
            result = handler(params)

            return result

        except json.JSONDecodeError:
            return {
                "status": "error",
                "message": f"Invalid JSON in arguments: {arguments}"
            }
        except Exception as e:
            logger.error(f"Error handling function call: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Error processing function call: {str(e)}"
            }