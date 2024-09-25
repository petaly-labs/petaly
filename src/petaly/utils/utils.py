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

import time
from functools import wraps

def measure_time(func):
    """This decorator prints the execution time for the decorated function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.info(f"Function {func.__module__}.{func.__name__}()  ran in {round(end - start, 2)}s")

        return result

    return wrapper

class FormatDict(dict):
    """ With help of this class the function str.format_map() will ignore a key which wasn't specified in the parameter section

    """
    def __missing__(self, key):
        return '{' + str(key) + '}'