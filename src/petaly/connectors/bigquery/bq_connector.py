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
import time
import logging
from google.cloud import storage, exceptions, bigquery
from google.cloud.storage import transfer_manager

class BQConnector():

    def __init__(self):
        self.connector_id = 'bigquery'
        self.metaquery_quote = ''
        self.bucket_prefix = 'gs://'
        self.bq_source_format = 'bigquery.SourceFormat.CSV'
        pass

    def extract_to(self, table_ref, destination_uri, region):

        logging.debug(f"Extract table {table_ref} to {destination_uri} started")
        try:
            bq_client = bigquery.Client()
            job_config = bigquery.job.ExtractJobConfig()
            job_config.compression = bigquery.Compression.GZIP

            extract_job = bq_client.extract_table(
                table_ref,
                destination_uri,
                location=region,
                job_config=job_config,
            )

            result = extract_job.result()  # Waits for job to complete.

            logging.debug(f"Table {table_ref} was loaded to {destination_uri}. Result: {result}")

        except exceptions.GoogleCloudError as err:
            logging.error(err)

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

    def load_from(self, bq_job_config, data_fpath, table_id, load_from_bucket, region):
        """ Load file from local file system or from bucket to BigQuery. The param load_from_bucket determine the behaviour.
        """

        logging.debug(f"Load data from the file {data_fpath}")

        try:
            bq_client = bigquery.Client()
            job_config = bigquery.LoadJobConfig(**bq_job_config)
            rows_start = bq_client.get_table(table_id).num_rows  # Make an API request.

            if load_from_bucket:
                job = bq_client.load_table_from_uri(data_fpath,
                                                    table_id,
                                                    job_config=job_config,
                                                    location=region)
            else:
                with open(data_fpath, "rb") as source_file:
                    job = bq_client.load_table_from_file(source_file,
                                                         table_id,
                                                         job_config=job_config,
                                                         location=region)

            job.result()  # Waits for the job to complete.
            table = bq_client.get_table(table_id)  # Make an API request.
            rows_end = table.num_rows
            rows_loaded = rows_end - rows_start
            logging.debug(f"Loaded {rows_loaded} rows and {len(table.schema)} columns to {table_id} from file {data_fpath}")

        except exceptions.GoogleCloudError as err:
            logging.error(err)


    def drop_table(self, table_id):
        """ Function drop BigQuery table
        """
        logging.debug(f"DROP table if exist {table_id}")
        try:
            bq_client = bigquery.Client()
            bq_client.delete_table(table_id, not_found_ok=True)  # Make an API request.
            logging.debug(f"Table {table_id} was dropped.")
        except exceptions.GoogleCloudError as err:
            logging.error(err)
            pass

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

    def execute_sql(self, query):
        """
        """
        logging.debug(f"Execute query.")
        try:
            bq_client = bigquery.Client()
            job = bq_client.query(query)  # API request.
            job.result()  # Waits for the query to finish.
            logging.debug(f"New table {job.destination.project}.{job.destination.dataset_id}.{job.destination.table_id} is created.")
        except exceptions.GoogleCloudError as err:
            logging.debug(query)
            logging.error(err)
            pass


    def get_metadata_result(self, metadata_query):
        logging.debug(f"Execute meta query.")
        try:
            bq_client = bigquery.Client()
            query_result = bq_client.query_and_wait(metadata_query)
            result_arr = []
            for row in query_result:
                result_arr.append(dict(list(row.items())))

            return result_arr
        except exceptions.GoogleCloudError as err:
            logging.debug(metadata_query)
            logging.error(err)
            pass

    def create_table_from_json_schema(self, table_schema_fpath, table_id):
        """ ToDo this function potentially for bq_connector
        """
        # TODO(dev): Change the table_id variable to the full name of the table you want to get schema from.
        logging.debug(f"Create table {table_id}.")
        try:
            bq_client = bigquery.Client()
            schema = bq_client.schema_from_json(table_schema_fpath)
            table = bigquery.Table(table_id, schema=schema)
            table = bq_client.create_table(table)  # API request
            logging.debug( f"Table {table_id} was created.")
        except Exception as e:
            logging.error(e)
            pass

        return table

    def download_files_from_bucket(self, bucket_name, blob_prefix,  destination_directory):

        delimiter = '/'
        prefix = blob_prefix + delimiter
        logging.debug(f"Download files from bucket-name: {bucket_name}; prefix: {prefix}; destination_directory: {destination_directory}.")
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

            blob_list = []
            for blob in blobs:
                blob_list.append(blob.name)

            downloaded_file_list = []
            for blob_fpath in blob_list:
                blob = bucket.blob(blob_fpath)
                file_name = os.path.join(destination_directory, blob_fpath.rsplit('/')[-1] + '.gz')
                blob.download_to_filename(filename=file_name)
                downloaded_file_list.append(file_name)

            return downloaded_file_list

        except exceptions.GoogleCloudError as err:
            logging.error(err)