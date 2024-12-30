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
from google.cloud import exceptions, bigquery


class BQConnector():

    def __init__(self):
        self.connector_id = 'bigquery'
        self.metaquery_quote = ''
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

    def load_from(self, bq_job_config, data_fpath, table_id, load_from_bucket, region):
        """ Load file from local file system or from bucket to BigQuery. The param load_from_bucket determine the behaviour.
        """

        logging.debug(f"Load data from the file {data_fpath}")

        try:
            bq_client = bigquery.Client()


            job_config = bigquery.LoadJobConfig(**bq_job_config)

            #job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,
            #                                    skip_leading_rows=1,
            #                                    autodetect=False,
            #                                    max_bad_records=0,
            #                                    field_delimiter=";",
            #                                    quote_character='"')
            #

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
