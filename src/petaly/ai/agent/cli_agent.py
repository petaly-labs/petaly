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
import sys
import os
import json
import asyncio

from rich.console import Console
from rich.markdown import Markdown
from rich.prompt import Prompt
import argparse

from petaly.ai.agent.petaly_agent import PetalyAgent
from petaly.sysconfig.main_config import MainConfig
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory

logger = logging.getLogger(__name__)


class CliAgent:
    """Command line interface for the Petaly AI Agent."""
    
    def __init__(self, main_config=None):
        """Initialize the CLI agent interface."""
        self.m_conf = MainConfig() if main_config is None else main_config
        self.m_conf.set_workspace_dpaths()
        self.console = Console()
        
        # Set up argument parser
        self.parser = argparse.ArgumentParser(
            description="Petaly AI Agent - Use natural language to create, modify, and run data pipelines",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Available commands:
  interactive    Run in interactive mode for natural language conversations
  command        Execute a single natural language command

Examples:
  # Start interactive mode
  python -m petaly agent interactive

  # Execute a single command
  python -m petaly agent command --instruction "Create pipeline: pipeline-name mypipe; source-systems endpoint-type mysql (database_host=localhost); target-systems endpoint-type postgres (database_host=localhost)"

  # List available pipelines
  python -m petaly agent command --instruction "Show me all available pipelines"

  # Run a specific pipeline
  python -m petaly agent command --instruction "Run pipeline: pipeline-name mypipe"

Natural Language Commands:
  The AI Agent understands various natural language commands for:
  - Creating pipelines
  - Modifying existing pipelines
  - Running pipelines
  - Listing available pipelines
  - Getting pipeline details
  - Managing data objects
            """
        )
        
        # Add subparsers for different actions
        subparsers = self.parser.add_subparsers(dest='action', help='Action to perform')
        
        # interactive command
        interactive_parser = subparsers.add_parser(
            'interactive',
            help='Run in interactive mode for natural language conversations'
        )
        interactive_parser.set_defaults(func=self.process)
        
        # command command
        command_parser = subparsers.add_parser(
            'command',
            help='Execute a single natural language command'
        )
        command_parser.add_argument(
            '--instruction',
            required=True,
            help='Natural language instruction to process'
        )
        command_parser.set_defaults(func=self.process)
        
        # Common arguments
        self.parser.add_argument(
            '-c', '--config_file_path',
            help=self.m_conf.missing_main_config_file_message()
        )
        
        # Set default function to show help if no command is provided
        self.parser.set_defaults(func=lambda args: self.parser.print_help())
        
        # Initialize prompt session with file-based history
        history_file = os.path.expanduser('~/.petaly/command_history')
        os.makedirs(os.path.dirname(history_file), exist_ok=True)
        self.prompt_session = PromptSession(history=FileHistory(history_file))
        
    def process(self, args):
        """Process the command line arguments."""
        # Set up main config
        self.m_conf.set_main_config_fpath(args.config_file_path)
        self.m_conf.set_workspace_dpaths()
        self.m_conf.set_global_settings()
        
        # Initialize the agent
        agent = PetalyAgent(
            main_config=self.m_conf
        )
        
        # Determine mode
        if args.action == 'interactive':
            self.run_interactive_mode(agent)
        elif args.action == 'command':
            if not args.instruction:
                self.console.print("[red]Error: the parameter --instruction is required in mode: agent command[/red]")
                sys.exit(1)
            
            self.run_command_mode(agent, args.instruction)
        
    def run_interactive_mode(self, agent):
        """Run the agent in interactive mode with a chat interface."""
        self.console.print("\n[bold blue]Petaly AI Agent[/bold blue]")
        self.console.print("Type 'exit' or 'quit' to end the session.\n")
        
        # Main interaction loop
        while True:
            try:
                # Get user input with history support
                instruction = self.prompt_session.prompt("PROMPT > ")
                if instruction.lower() in ('exit', 'quit'):
                    break
                
                # Process instruction
                response = asyncio.run(agent.process_instruction(instruction))
                
                # Display response
                self.console.print("\n[bold green]Agent[/bold green]")
                self.console.print(Markdown(response))
                self.console.print()
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error in interactive mode: {e}", exc_info=True)
                self.console.print(f"[red]Error: {str(e)}[/red]")
    
    def run_command_mode(self, agent, instruction):
        """Run the agent in command mode to process a single instruction."""
        try:
            response = asyncio.run(agent.process_instruction(instruction))
            self.console.print(Markdown(response))
        except Exception as e:
            logger.error(f"Error in command mode: {e}", exc_info=True)
            self.console.print(f"[red]Error: {str(e)}[/red]")
            sys.exit(1)
    
    def start(self):
        """Start the CLI agent."""
        try:
            args = self.parser.parse_args()
            logger.debug(f"Executing agent command with args: {args}")
            args.func(args)
        except Exception as e:
            logger.error(f"Error executing agent command: {e}", exc_info=True)
            raise

