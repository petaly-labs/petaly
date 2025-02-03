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

import logging
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
        self.bucket_path_delimiter = '/'
        pass

    def rename_blob(self, bucket_name, blob_file_name, new_blob_name = None):
        """ Function renamed blob by giving time sequence suffix in millisecond.

        ToDo: It should be reviewed as keeping blob in the same folder with the same name will cause load to bigquery
        """
        file_suffix = str(time.time_ns())

        if new_blob_name is None:
            new_blob_name = blob_file_name + "." + file_suffix

        logger.debug(f"Bucket renaming started: current name {blob_file_name} new name {new_blob_name}")

        try:
            st_client = storage.Client()
            bucket = st_client.bucket(bucket_name)
            blob = bucket.blob(blob_file_name)
            new_target_file = bucket.rename_blob(blob, new_blob_name)
            logger.debug(f"Blob {blob.name} has been renamed to {new_target_file.name}")

        except exceptions.GoogleCloudError as err:
            logger.error(err)

    def delete_object_in_bucket(self, bucket_name, blob_prefix):
        """
        """
        logger.debug(f"Delete folder in Google Storage: {bucket_name}/{blob_prefix}")
        try:
            st_client = storage.Client()
            bucket = st_client.get_bucket(bucket_name)
            """Delete object under folder"""
            blobs_list = list(bucket.list_blobs(prefix=blob_prefix))
            bucket.delete_blobs(blobs_list)
            logger.debug(f"Folder deleted: {bucket_name}/{blob_prefix}")
        except exceptions.GoogleCloudError as err:
            logger.error(err)
            pass

    def delete_gs_blob(self, bucket_name, blob_name):
        """ Function drop a blob in specific bucket
        """
        logger.debug(f"Blob {blob_name} in {bucket_name} will be deleted")
        try:
            st_client = storage.Client()
            bucket = st_client.bucket(bucket_name)
            bucket.delete_blob(blob_name)
            logger.debug(f"Blob {blob_name} in {bucket_name} has been deleted")
        except exceptions.GoogleCloudError as err:
            logger.error(err)
            pass

    def download_files_from_bucket(self, bucket_name, blob_prefix, file_names, destination_dpath):
        """
        """
        logger.debug(f"Download files from bucket-name: {bucket_name}; blob-prefix: {blob_prefix}; destination-directory: {destination_dpath}")
        blob_prefix += self.bucket_path_delimiter

        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            object_list = []
            if file_names is None:
                object_list = self.get_bucket_file_list(bucket_name, blob_prefix)

            else:
                for file_name in file_names:
                    file_name = blob_prefix + file_name
                    object_list.append(file_name)

            downloaded_file_list = []
            for blob_fpath in object_list:
                blob = bucket.blob(blob_fpath)
                object_fname = blob_fpath.split(self.bucket_path_delimiter)[-1]

                target_fpath = os.path.join(destination_dpath, object_fname)


                blob.download_to_filename(filename=target_fpath)
                is_gzipped, target_fpath = self.f_handler.check_gzip_modify_path(target_fpath)

                downloaded_file_list.append(target_fpath)
                message_gzipped = 'gzipped ' if is_gzipped else ' '
                logger.debug(
                    f"Download {message_gzipped}file fromm {self.bucket_prefix + bucket_name + self.bucket_path_delimiter + blob_fpath} to output directory: {target_fpath}")

            return downloaded_file_list

        except exceptions.GoogleCloudError as err:
            logger.error(err)

    def get_bucket_file_list(self, bucket_name, blob_prefix):
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)

            object_list = []
            blobs = bucket.list_blobs(prefix=blob_prefix, delimiter=self.bucket_path_delimiter)
            for blob in blobs:
                object_list.append(blob.name)

            return object_list

        except (Exception) as error:
            logger.debug(bucket_name, blob_prefix)
            logger.error(error)

    def upload_blob(self, full_fpath, bucket_name, destination_blob_name):
        """Function uploads a file to the GS bucket.
        """
        logger.debug(
            f"Load data from the local path {full_fpath} to the backet: {bucket_name} with destination path {destination_blob_name}; ")

        try:
            st_client = storage.Client()
            bucket = st_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(full_fpath)
        except exceptions.GoogleCloudError as err:
            logger.error(err)
            pass

    def upload_files_to_bucket(self, bucket_name, blob_prefix, local_file_list):
        """ upload file to GS bucket
        """
        bucket_file_list = []
        for file_local_fpath in local_file_list:
            file_name = os.path.basename(file_local_fpath)

            blob_path = blob_prefix  + self.bucket_path_delimiter + os.path.basename(file_name)
            self.upload_blob(file_local_fpath, bucket_name, blob_path)

            full_blob_path = self.bucket_prefix + bucket_name + self.bucket_path_delimiter + blob_path
            bucket_file_list.append(full_blob_path)

            logger.debug(f"Upload file {file_local_fpath} to destination {full_blob_path}")

        return bucket_file_list
