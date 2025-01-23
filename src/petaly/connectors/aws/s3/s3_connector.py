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
        self.endpoint_attr = endpoint_attr

        if aws_session is None:
            self.aws_session = self.get_aws_session()
        else:
            self.aws_session = aws_session

        self.f_handler = FileHandler()

    def get_aws_session(self):

        aws_access_key_id = self.endpoint_attr.get('aws_access_key_id')
        aws_secret_access_key = self.endpoint_attr.get('aws_secret_access_key')
        aws_profile_name = self.endpoint_attr.get('aws_profile_name')
        aws_region = self.endpoint_attr.get('aws_region')
        try:

            session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                            aws_secret_access_key=aws_secret_access_key,
                                            profile_name=aws_profile_name,
                                            region_name=aws_region)
            return session

        except ClientError as e:
            logger.error(e)
            sys.exit()

    def get_s3_client(self):
        return self.aws_session.client(service_name='s3')
    def drop_object_from_bucket(self, bucket_name, blob_prefix, object_name):
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)

        if blob_prefix is not None:
            object_name = blob_prefix + '/' + object_name

        for object_summary in bucket.objects.filter(Prefix=object_name):
            object_summary.delete()

    def download_object_from_bucket(self, bucket_name, blob_prefix, object_name, destination_dpath):

        logging.debug(
            f"Download files from bucket-name: {bucket_name}; blob-prefix: {blob_prefix}; object-name: {object_name}; destination-directory: {destination_dpath}")

        object_list = self.get_bucket_file_list(bucket_name, blob_prefix, object_name)

        s3_client = self.get_s3_client()

        for object_fpath in object_list:
            object_fname = object_fpath.split('/')[-1]
            s3_client.download_file(bucket_name, object_fpath, os.path.join(destination_dpath, object_fname))

    def download_files_from_bucket(self, bucket_name, file_names, destination_directory):

        logging.debug(
            f"Download files from bucket-name: {bucket_name}; file-names: {file_names}; destination-directory: {destination_directory}")

        s3_client = self.get_s3_client()

        for file_path in file_names:
            fname = file_path.split('/')[-1]
            s3_client.download_file(bucket_name, file_path, os.path.join(destination_directory, fname))

    def get_bucket_file_list(self, bucket_name, blob_prefix, object_name):
        try:
            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket(bucket_name)
            if blob_prefix is not None:
                object_name = blob_prefix + '/' + object_name

            object_list = []

            for object_summary in bucket.objects.filter(Prefix=object_name):
                object_list.append(object_summary.key)

            return object_list

        except (Exception) as error:
            logger.info(bucket_name, object_name)
            logger.error(error)

    def load_files_to_s3_bucket(self, bucket_name, blob_prefix, object_name, object_file_list):

        try:
            if blob_prefix is not None:
                object_name = blob_prefix + '/' + object_name

            s3_client = boto3.client('s3')
            for object_fpath in object_file_list:
                # to add pipeline_name

                bucket_fpath = object_name + "/" + os.path.basename(object_fpath)

                s3_client.upload_file(object_fpath, bucket_name, bucket_fpath)
                logging.info(f"Load file {object_fpath} to destination s3://{bucket_name}/{bucket_fpath}")

        except (Exception, s3_client.exceptions, S3UploadFailedError) as error:
            logger.error("Upload failed for: ",bucket_name, object_name, object_file_list)
            logger.error(error)


