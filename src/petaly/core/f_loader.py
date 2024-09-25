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

from petaly.core.composer import Composer
from petaly.utils.file_handler import FileHandler

from petaly.core.data_object import DataObject

class FLoader():

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.composer = Composer(pipeline)
        self.f_handler = FileHandler()
        pass

    def get_data_object_list(self):
        return self.composer.get_data_object_list()

    def get_data_object(self, object_name):
        return DataObject(self.pipeline, object_name)