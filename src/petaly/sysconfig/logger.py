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


def setup_logging(logging_config_fpath, log_dir):
    """
    """
    f_handler = FileHandler()
    config = f_handler.load_json(logging_config_fpath)
    ##date = datetime.now().strftime("%Y%m%d")
    log_fpath = os.path.join(log_dir, config.get('handlers').get('file').get('filename'))
    config['handlers']['file']['filename'] = log_fpath
    logging.config.dictConfig(config)

