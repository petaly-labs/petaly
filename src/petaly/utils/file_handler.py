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

import os
import re
import pathlib
import sys

import yaml
from yaml import SafeDumper, Dumper
import json
import shutil
import ast
import gzip
from datetime import datetime


class IndentDumper(yaml.Dumper):
    """ This is a helper class to indent yaml file and make it more readable
    This class is used in FileHandler class. e.g. yaml.dump_all( dict_data, file, Dumper=IndentDumper,  sort_keys=False, allow_unicode=True)
    """
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentDumper, self).increase_indent(flow, False)


class FileHandler:
    """ FileHandler is a file manager, to save, load, modify yaml, json, text files
    """
    def __init__(self, file_format="yaml"):
        """
        """
        if file_format != "yaml" and file_format != "json":  # error!
            raise ValueError("The file format {file_format} is not supported. Parameter expect yaml or json as a file format.".format(file_format=file_format))
        self.file_format = "json" if file_format == "json" else "yaml"
        self.file_extension = "." + self.file_format

    def load_file_as_dict(self, file_fpath, file_format):
        dict_data = {}
        if file_format == 'yaml':
            dict_data = self.load_yaml(file_fpath)
        elif file_format == 'json':
            dict_data = self.load_json(file_fpath)
        return dict_data

    def load_json(self, file_fpath) -> {}:
        try:
            with (open(file_fpath, mode="rt", encoding="utf-8") as f):
                obj = json.loads(f.read())
                return obj
        except Exception as e:
            logger.error(e)
            return {}

    def load_yaml_all(self, file_fpath):
        try:
            with (open(file_fpath, mode="rt", encoding="utf-8") as f):
                yaml_data = yaml.safe_load_all(f)
                obj = list(yaml_data)
                return obj
        except Exception as e:
            logger.error(e)
            sys.exit()

    def load_yaml(self, file_fpath):

        if self.is_file(file_fpath) != True:
            return None

        try:
            with (open(file_fpath, mode="rt", encoding="utf-8") as f):
                obj = yaml.safe_load(f)
                return obj
        except Exception as e:
            logger.error(e)
            sys.exit()
            #return {}


    def replace_file_extension(self, file_fpath, new_file_extension):
        """ This function replace extension from file_fpath to the new_file_extension
        """
        return pathlib.Path(file_fpath).with_suffix(new_file_extension)

    def get_file_extensions(self, file_fpath):
        """ This function replace extension from file_fpath to the new_file_extension
        """
        return pathlib.Path(file_fpath).suffixes

    def check_file_extension(self, file_fpath, file_extension):
        """
        """
        path_in_arr = os.path.splitext(file_fpath)

        return path_in_arr[1].lstrip('.') == file_extension

    def save_dict_to_file(self, file_fpath, dict_data, file_format='yaml'):
        """
        """
        if file_format == 'yaml':
            self.save_dict_to_yaml(file_fpath, dict_data)
        elif file_format == 'json':
            self.save_dict_to_json(file_fpath, dict_data)

    def save_dict_to_yaml(self, file_fpath, dict_data, dump_all=False):
        """
        """
        result = 0
        file_fpath = self.replace_file_extension(file_fpath, ".yaml")
        dir_name = os.path.dirname(file_fpath)
        self.make_dirs(dir_name)

        # This block, in combination with yaml.safe_dump, replaces null values with empty values when saving to a YAML file.
        SafeDumper.add_representer(
            type(None),
            lambda dumper, value: dumper.represent_scalar(u'tag:yaml.org,2002:null', '')
        )

        logger.info(f"Save dict as yaml under: {file_fpath}")

        if dump_all==False:

            with (open(file_fpath, mode="w", encoding="utf-8") as file):
                ## yaml.dump(dict_data, file, Dumper=IndentDumper, sort_keys=False, allow_unicode=True, default_flow_style=False)
                yaml.safe_dump(dict_data, file, sort_keys=False, allow_unicode=True, default_flow_style=False, default_style=None)
        else:
            with (open(file_fpath, mode="w", encoding="utf-8") as file):
                ## yaml.dump_all(dict_data, file, sort_keys=False, allow_unicode=True, default_flow_style=False)
                yaml.safe_dump_all(dict_data, file, sort_keys=False, allow_unicode=True, default_flow_style=False, default_style=None)


    def save_dict_to_json(self, file_fpath, dict_data):
        """
        """
        result = 0
        logger.info(f"Save dict as json under: {file_fpath}")

        file_fpath = self.replace_file_extension(file_fpath, ".json")
        dir_name = os.path.dirname(file_fpath)
        self.make_dirs(dir_name)

        with (open(file_fpath, mode="w", encoding="utf-8") as file):
            json_object = json.dumps(dict_data, indent=2, separators=(',', ':'))
            file.write(json_object)


    def format_json_file_compact(self, file_fpath, modify_file=True):
        """ This function formats json file in a compacter file format. It removes line break in aray
        """
        new_content=""
        # sleep for a second to give an upper stream function to store context into the file
        with (open(file_fpath, mode="r", encoding="utf-8") as file):
            run_strip_line = False

            for line in file:
                # skip run
                if  re.search(r"\[(.*?)\]", line):
                    new_content += line
                    continue

                # if found start strip
                if line.find(':[') > -1:
                    run_strip_line = True
                    new_content += line.rstrip()
                    continue

                # if found close strip
                if run_strip_line == True and line.find(']')>-1:
                    new_content += line.lstrip()
                    run_strip_line = False
                    continue

                if run_strip_line == True:
                    stripped_line = line.strip()
                    new_content += stripped_line
                else:
                    new_content += line

        if new_content != "":
            if modify_file == False:
                return new_content

            with (open(file_fpath, mode="w", encoding="utf-8") as file):
                file.write(new_content)

    def load_file(self, file_fpath):
        """
        """
        with open(file_fpath, "r", encoding="utf-8") as f:
            file_context = f.read()
            return file_context

    def save_file(self, path_to_file, file_context):
        """
        """
        with open(path_to_file, "w", encoding="utf-8") as f:
            f.write(file_context)
            return True

    def make_dirs(self, path_to_dir):
        if not os.path.isdir(path_to_dir):
            os.makedirs(path_to_dir)

    def get_all_dir_files(self, dir_path, file_extension, file_names_only=False):
        """
        """

        # iterating over all files with determine extension
        result_arr = []

        for files in os.listdir(dir_path):
            if files.endswith(file_extension):
                if file_names_only == False:
                    result_arr.append(os.path.join(dir_path,files))
                else:
                    result_arr.append(files)
            else:
                continue

        return result_arr

    def get_all_dir_names(self, path_to_dir):
        """ """
        # iterating over all files with determine extension
        result_arr = []

        for dir in os.listdir(path_to_dir):
            result_arr.append(dir)

        return result_arr

    def get_specific_files(self, path_to_dir, file_name):
        """
        """

        # iterating over all files with determine extension
        result_arr = []
        for path in pathlib.Path(path_to_dir).rglob(file_name):
            result_arr.append(str(path))
        return result_arr

    def get_file_name(self, path_to_file):
        return os.path.basename(path_to_file)

    def list_dir(self, path_to_dir):
        """ It's a wrapper function for os.listdir
        """
        return os.listdir(path_to_dir)

    def is_file(self, path_to_file):
        return os.path.isfile(path_to_file)

    def is_dir(self, path_to_dir):
        return os.path.isdir(path_to_dir)

    def check_dir(self, path_to_dir):
        dir_exists = os.path.isdir(path_to_dir)
        files_in_dir = 0
        if dir_exists:
            files_in_dir = len(os.listdir(path_to_dir))

        return dir_exists, files_in_dir

    def cp_file(self, path_to_file, target_dir, target_file_name=None):

        file_exists = os.path.isfile(path_to_file)
        dir_exists = os.path.isdir(target_dir)

        if dir_exists == False:
            self.make_dirs(target_dir)

        if file_exists:
            destination = target_dir
            if target_file_name != None:
                destination = os.path.join(target_dir, target_file_name)
                shutil.copyfile(path_to_file, destination)
            else:
                shutil.copy(path_to_file, destination)

    def backup_file(self, path_to_file, target_dir=None):

        if target_dir is None:
            target_dir = os.path.dirname(path_to_file)
        base_fname = os.path.basename(path_to_file)
        target_file = base_fname + '.backup_' + datetime.now().strftime("%Y%m%d%H%M%S")
        self.cp_file(path_to_file, target_dir, target_file_name=target_file)

        logger.info(f"The file {base_fname} has been backed up to the: {os.path.join(target_dir,target_file)}")

    def cleanup_files(self, path_to_dir, file_extension):
        """ This function remove all files in a folder of path_to_dir from specific extension.
        """
        files = self.get_all_dir_files(path_to_dir, file_extension)
        for file in files:
            os.remove(file)
        return True

    def cleanup_dir(self, path_to_dir):
        """ This function remove all subfolders from path_to_dir. The folder path_to_dir will be not removed.

        :param path_to_dir:
        :return:
        """

        for folder_name, subfolders, filenames in os.walk(path_to_dir):
            logger.info('Following subfolders are removed:')
            for subfolder in subfolders:
                folder_to_remove = os.path.join(folder_name, subfolder)
                shutil.rmtree(folder_to_remove)
                logger.info(folder_to_remove)

    def copy_file_without_comments(self, path_to_file, path_to_target_file, comment_sign='#'):
        """ This function copy templates file without comments to the specified pipeline
        """
        target_file = open(path_to_target_file, 'w')
        with open(path_to_file, "r") as file:
            for line in file:
                if line.find(comment_sign) < 0:
                    target_file.write(line)


        target_file.close()

    def load_combined_json(self, first_json_fpath, second_json_fpath) -> {}:
        """ combine two json files and return a dict
        """
        first_dict = {}
        second_dict = {}

        if first_json_fpath is not None and self.is_file(first_json_fpath):
            self.is_file(first_json_fpath)
            first_dict = self.load_json(first_json_fpath)

        if second_json_fpath is not None and self.is_file(second_json_fpath):
            second_dict = self.load_json(second_json_fpath)

        result_maping = self.dict_update(first_dict, second_dict)

        return result_maping

    def dict_update(self, dict_1, dict_2):
        """ Recursively update of a dict.
        """
        for key, value in dict_1.items():
            if key not in dict_2:
                dict_2[key] = value
            elif isinstance(value, dict):
                self.dict_update(value, dict_2[key])
        return dict_2

    def string_to_dict(self, input_string):
        result_json = ast.literal_eval('{'+input_string+'}')
        return dict(result_json)

    def unpack_archive(self, filename, extract_dir):
        shutil.unpack_archive(filename = filename, extract_dir=extract_dir)

    def gunzip_file(self, gz_fpath, cleanup_file=True):

        file_fpath = self.replace_file_extension(gz_fpath, '')
        try:
            with gzip.open(gz_fpath, 'rb') as f_in:
                with open(file_fpath, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    if cleanup_file == True:
                        os.remove(gz_fpath)  # delete zipped file

            return file_fpath

        except gzip.BadGzipFile as e:
            logger.error(e)
            return None

    def gzip_file(self, file_fpath, cleanup_file=True):

        gz_file_fpath = file_fpath + '.gz'

        try:
            with open(file_fpath, 'rb') as f_in:
                with gzip.open(gz_file_fpath, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    if cleanup_file == True:
                        os.remove(file_fpath)  # delete file

            return gz_file_fpath

        except gzip.BadGzipFile as e:
            logger.error(e)
            return None

    def gunzip_all_files(self, gz_dpath, cleanup_file=True):
        gz_file_list = self.get_specific_files(gz_dpath, '*.csv.gz')
        for gz_fpath in gz_file_list:
            self.gunzip_file(gz_fpath, cleanup_file=True)

    def gzip_all_files(self, gz_dpath, cleanup_file=True):
        gz_file_list = self.get_specific_files(gz_dpath, '*.csv')
        for gz_fpath in gz_file_list:
            self.gzip_file(gz_fpath, cleanup_file=True)

    def check_dict_key_exist(self, doc_dict: dict, key: str) -> bool:
        result = False
        key_list = list(doc_dict.keys())
        if(key_list.count(key) == 1):
            result = True

        return result