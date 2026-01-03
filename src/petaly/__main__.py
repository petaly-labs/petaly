# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

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

def main(main_config: Optional[MainConfig] = None):
    """Main entry point for the Petaly package."""
    
    try:
        # Default to CLI mode
        cli = Cli(main_config)
        cli.start()
    except KeyboardInterrupt:
        # User interrupted with Ctrl+C - exit gracefully
        print("\n\nProcess interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error running Petaly: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main(None)