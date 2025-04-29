# src/petaly/mcp/cli.py

import argparse
import json
import sys
import logging
from typing import Dict, Any

from petaly.ai.mcp.adapter import PetalyMCPAdapter
from petaly.sysconfig.main_config import MainConfig

logger = logging.getLogger(__name__)

class CliMCP:

    def __init__(self, main_config=None):
        """CLI entry point for Petaly MCP interface."""
        parser = argparse.ArgumentParser(description="Petaly Model Context Protocol Interface")
        parser.add_argument('action', choices=['get-schemas', 'call-function'],
                            help='Action to perform')
        parser.add_argument('-f', '--function', help='Function name for call-function action')
        parser.add_argument('-p', '--parameters', help='JSON string of parameters for function call')
        parser.add_argument('-c', '--config_file_path', help='Path to Petaly config file')

        args = parser.parse_args()

        # Initialize main config
        m_conf = MainConfig()
        m_conf.set_main_config_fpath(args.config_file_path)
        m_conf.set_workspace_dpaths()
        m_conf.set_global_settings()

        # Initialize MCP adapter
        adapter = PetalyMCPAdapter(m_conf)

        if args.action == 'get-schemas':
            # Return tool schemas
            schemas = adapter.get_tool_schemas()
            print(json.dumps(schemas, indent=2))

        elif args.action == 'call-function':
            if not args.function:
                print("Error: --function is required for call-function action")
                sys.exit(1)

            # Call the function
            parameters = args.parameters or "{}"
            result = adapter.handle_function_call(args.function, parameters)
            print(json.dumps(result, indent=2))

if __name__ == "__main__":
    cli_mcp = CliMCP()