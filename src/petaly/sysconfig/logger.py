# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
import logging.config
import os.path

from petaly.utils.file_handler import FileHandler


def setup_logging(logging_config_fpath, log_dir, logging_mode='INFO'):
    """
    """
    f_handler = FileHandler()
    config = f_handler.load_json(logging_config_fpath)
    if logging_mode == 'DEBUG':
        config['handlers']['console'].update({'level': logging_mode})
        config['handlers']['console'].update({'show_level': True})
        config['handlers']['console'].update({'omit_repeated_times': True})
        config['handlers']['console'].update({'show_path': True})
        config['handlers']['console'].update({'enable_link_path': True})
        config['handlers']['file'].update({'level': logging_mode})
        config['loggers']['petaly'].update({'level':logging_mode})
        config['root'].update({'level':logging_mode})
        config['formatters']['format_file'].update({"format":"%(asctime)s - %(name)s - %(levelname)s - %(message)s"})
        #config['formatters']['format_console'].update({"format": "%(name)s - %(message)s"})
        config['formatters']['format_console'].update({"format": "%(message)s"})

    log_fpath = os.path.join(log_dir, config.get('handlers').get('file').get('filename'))
    config['handlers']['file'].update({'filename': log_fpath})
    logging.config.dictConfig(config)

