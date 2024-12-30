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
import logging.config
import os.path
#from datetime import datetime

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

