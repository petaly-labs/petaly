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
import sys
import boto3
from boto3.s3.transfer import S3UploadFailedError
from botocore.exceptions import ClientError

from petaly.utils.file_handler import FileHandler

class S3Connector():
    def __init__(self, endpoint_attr, aws_session=None):
        self.connector_id = 's3'
        self.metaquery_quote = '"'
        self.bucket_prefix = 's3://'
        self.bucket_path_delimiter = '/'
        self.endpoint_attr = endpoint_attr

        if aws_session is None:
            self.aws_session = self.get_aws_session()
        else:
            self.aws_session = aws_session

        self.f_handler = FileHandler()

    def get_aws_session(self):
        """
        """
        try:

            session = boto3.session.Session(aws_access_key_id=self.endpoint_attr.get('aws_access_key_id'),
                                            aws_secret_access_key=self.endpoint_attr.get('aws_secret_access_key'),
                                            profile_name=self.endpoint_attr.get('aws_profile_name'),
                                            region_name=self.endpoint_attr.get('aws_region'))
            return session

        except ClientError as e:
            logger.error(e)
            sys.exit()

    def get_s3_client(self):
        return self.aws_session.client(service_name='s3')

    def delete_object_in_bucket(self, bucket_name, blob_prefix):
        """
        """
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)

        for object_summary in bucket.objects.filter(Prefix=blob_prefix):
            object_summary.delete()

    def download_files_from_bucket(self, bucket_name, blob_prefix, file_names, destination_dpath):
        """
        """
        logger.debug(f"Download files from bucket-name: {bucket_name}; blob-prefix: {blob_prefix}; file-names: {file_names}; destination-directory: {destination_dpath}")

        object_list = []
        if file_names is None:
            object_list = self.get_bucket_file_list(bucket_name, blob_prefix)
        else:
            for file_name in file_names:
                file_name = blob_prefix + self.bucket_path_delimiter + file_name
                object_list.append(file_name)

        s3_client = self.get_s3_client()
        downloaded_file_list = []

        for object_fpath in object_list:
            object_fname = object_fpath.split(self.bucket_path_delimiter)[-1]
            target_fpath = os.path.join(destination_dpath, object_fname)

            s3_client.download_file(bucket_name, object_fpath, target_fpath)
            is_gzipped, target_fpath = self.f_handler.check_gzip_modify_path(target_fpath)

            downloaded_file_list.append(target_fpath)

            message_gzipped = 'gzipped ' if is_gzipped else ' '
            logger.debug(f"Download {message_gzipped}file from {self.bucket_prefix + bucket_name + self.bucket_path_delimiter + object_fpath} to output directory: {target_fpath}")

        return downloaded_file_list

    def get_bucket_file_list(self, bucket_name, blob_prefix):
        try:
            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket(bucket_name)

            object_list = []
            for object_summary in bucket.objects.filter(Prefix=blob_prefix):
                object_list.append(object_summary.key)

            return object_list

        except (Exception) as error:
            logger.debug(bucket_name, blob_prefix)
            logger.error(error)

    def upload_files_to_bucket(self, bucket_name, blob_prefix, object_file_list):

        try:
            s3_client = boto3.client('s3')
            for object_fpath in object_file_list:
                bucket_fpath = blob_prefix + self.bucket_path_delimiter + os.path.basename(object_fpath)

                s3_client.upload_file(object_fpath, bucket_name, bucket_fpath)
                logger.debug(f"Upload file {object_fpath} to destination s3://{bucket_name}/{bucket_fpath}")

        except (Exception, s3_client.exceptions, S3UploadFailedError) as error:
            logger.debug(f"Upload failed for: s3://{bucket_name}/{bucket_fpath} ; object_files", object_file_list)
            logger.error(error)
