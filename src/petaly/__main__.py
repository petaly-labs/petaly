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

import argparse
import logging
import sys
from typing import Optional

from petaly.sysconfig.main_config import MainConfig
from petaly.cli.cli import Cli

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the Petaly package."""
    m_conf = MainConfig()
    m_conf.set_main_config_fpath(config_file_path=None, init_main_config=True)
    m_conf.set_global_settings()
    

    try:
        if len(sys.argv) > 1:
            mode = sys.argv[1]
            if mode == 'agent':
                # Import Agent here to avoid circular imports
                m_conf.set_ai_settings()
                from petaly.ai.agent.cli_agent import CliAgent
                cli_agent = CliAgent(m_conf)
                # Remove 'agent' from sys.argv so it doesn't interfere with Agent's argument parsing
                sys.argv.pop(1)
                cli_agent.start()
            else:
                # Default to CLI mode
                cli = Cli(m_conf)
                cli.start()
        else:
            # No mode specified, default to CLI
            cli = Cli(m_conf)
            cli.start()
            
    except Exception as e:
        logger.error(f"Error running Petaly: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()