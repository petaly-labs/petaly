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
import sys

logger = logging.getLogger(__name__)

import os
import time
import logging
from google.cloud import storage, exceptions
from petaly.utils.file_handler import FileHandler


class GSConnector():

    def __init__(self):
        self.connector_id = 'gs'
        self.bucket_prefix = 'gs://'
        self.f_handler = FileHandler()
        pass


    def upload_blob(self, full_fpath, bucket_name, destination_blob_name):
        """Function uploads a file to the GS bucket.
        """
        logging.debug(
            f"Load data from the local path {full_fpath} to the backet: {bucket_name} with destination path {destination_blob_name}; ")

        try:
            st_client = storage.Client()
            bucket = st_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(full_fpath)
        except exceptions.GoogleCloudError as err:
            logging.error(err)
            pass

    def rename_blob(self, bucket_name, blob_file_name, new_blob_name = None):
        """ Function renamed blob by giving time sequence suffix in millisecond.

        ToDo: It should be reviewed as keeping blob in the same folder with the same name will cause load to bigquery
        """
        file_sufix = str(time.time_ns())

        if new_blob_name is None:
            new_blob_name = blob_file_name + "." + file_sufix

        logging.debug(f"Bucket renaming started: current name {blob_file_name} new name {new_blob_name}")

        try:
            st_client = storage.Client()
            bucket = st_client.bucket(bucket_name)
            blob = bucket.blob(blob_file_name)
            new_target_file = bucket.rename_blob(blob, new_blob_name)
            logging.debug(f"Blob {blob.name} has been renamed to {new_target_file.name}")

        except exceptions.GoogleCloudError as err:
            logging.error(err)

    def delete_gs_folder(self, bucket_name, folder_name):
        """
        """
        logging.debug(f"Delete folder in Google Storage: {bucket_name}/{folder_name}")
        try:
            st_client = storage.Client()
            bucket = st_client.get_bucket(bucket_name)
            """Delete object under folder"""
            blobs = list(bucket.list_blobs(prefix=folder_name))
            bucket.delete_blobs(blobs)
            logging.debug(f"Folder deleted: {bucket_name}/{folder_name}")
        except exceptions.GoogleCloudError as err:
            logging.error(err)
            pass

    def delete_gs_blob(self, bucket_name, blob_name):
        """ Function drop a blob in specific bucket
        """
        logging.debug(f"Blob {blob_name} in {bucket_name} will be deleted")
        try:
            st_client = storage.Client()
            bucket = st_client.bucket(bucket_name)
            bucket.delete_blob(blob_name)
            logging.debug(f"Blob {blob_name} in {bucket_name} has been deleted")
        except exceptions.GoogleCloudError as err:
            logging.error(err)
            pass

    def download_files_from_bucket(self, bucket_name, blob_prefix, specific_file_list, destination_directory):
        """
        """
        logging.debug(f"Download files from bucket-name: {bucket_name}; blob_prefix: {blob_prefix}; destination_directory: {destination_directory}")
        delimiter = '/'
        blob_prefix += delimiter
        download_specific_files_only = True if specific_file_list is not None and specific_file_list[0] is not None else False

        try:

            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            blobs = storage_client.list_blobs(bucket_name, prefix=blob_prefix, delimiter=delimiter)
            blob_result_list = []
            for blob in blobs:
                if download_specific_files_only:
                    if blob.name in (specific_file_list):
                        blob_result_list.append(blob.name)
                else:
                    blob_result_list.append(blob.name)

            downloaded_file_list = []
            for blob_fpath in blob_result_list:
                blob = bucket.blob(blob_fpath)
                extract_file_name = blob_fpath.rsplit('/')[-1]

                target_fpath = os.path.join(destination_directory, extract_file_name)

                if not self.f_handler.check_file_extension(target_fpath, '.gz'):

                    target_fpath += '.gz'

                blob.download_to_filename(filename=target_fpath)
                downloaded_file_list.append(target_fpath)

            return downloaded_file_list

        except exceptions.GoogleCloudError as err:
            logging.error(err)