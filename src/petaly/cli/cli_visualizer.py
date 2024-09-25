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

import os
from rich.console import Console

from petaly.utils.file_handler import FileHandler


class CliVisualizer():
	def __init__(self, main_config):
		self.m_conf = main_config
		self.file_h = FileHandler()
		self.output_line = '-------------------------------------------------------------'
		self.console = Console()


	def show_pipeline(self, pipeline_name):
		"""
		:return:
		"""
		pipeline_fpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name, self.m_conf.pipeline_fname)
		self.console.print(self.output_line)
		self.console.print(f"Pipeline Config {pipeline_fpath}")
		self.console.print(self.output_line)
		pipeline_config = self.file_h.load_file(pipeline_fpath)
		self.console.print(pipeline_config)
		self.console.print(self.output_line)

	def show_pipelines(self):

		file_arr = self.file_h.list_dir(path_to_dir=self.m_conf.pipeline_base_dpath)
		self.console.print(self.output_line)
		self.console.print('Pipeline Name')
		self.console.print(self.output_line)
		for i in file_arr:
			self.console.print('{0}'.format(i))

		self.console.print(self.output_line)

	def show_workspace(self):
		"""

		:return:
		"""

		if os.path.exists(self.m_conf.pipeline_base_dpath):
			self.console.print (f"Pipeline base directory: {self.m_conf.pipeline_base_dpath}")
		else:
			self.console.print(f"Missing pipeline base directory {self.m_conf.pipeline_base_dpath}. To initiate workspace run: init workspace")

		if os.path.exists(self.m_conf.output_base_dpath):
			self.console.print(f"Pipeline output directory: {self.m_conf.output_base_dpath}")
		else:
			self.console.print(f"Missing output directory {self.m_conf.output_base_dpath}. To initiate workspace run: init workspace")

		if os.path.exists(self.m_conf.logs_base_dpath):
			self.console.print(f"Logs directory: {self.m_conf.logs_base_dpath}")
		else:
			self.console.print(f"Missing logs directory {self.m_conf.logs_base_dpath}. To initiate workspace run: init workspace")

