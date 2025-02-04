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
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from petaly.utils.file_handler import FileHandler

class RSConnectorIAM():

    def __init__(self, endpoint_attr):
        """
        """
        self.connector_id = 'redshift'
        self.metaquery_quote = '"'
        self.endpoint_attr = endpoint_attr
        self.aws_session = self.get_aws_session()
        self.f_handler = FileHandler()
        self.statement_timeout = 300
        self.is_serverless = True if str(self.endpoint_attr.get('is_serverless')).lower() == 'true' else False

    def get_aws_session(self):
        """
        """
        try:
            session = boto3.session.Session(aws_access_key_id=self.endpoint_attr.get('aws_access_key_id'),
                                            aws_secret_access_key=self.endpoint_attr.get('aws_secret_access_key'),
                                            profile_name=self.endpoint_attr.get('aws_profile_name'),
                                            region_name=self.endpoint_attr.get('aws_region')
                                            )
            return session

        except ClientError as e:
            logger.error(e)
            sys.exit()

    def execute_sql(self, sql, sleep_sec=1):
        try:
            rs_client = self.aws_session.client(service_name='redshift-data')
            database_name = self.endpoint_attr.get('database_name')
            database_user = self.endpoint_attr.get('database_user')

            if self.is_serverless is True:
                workgroup_name = self.endpoint_attr.get('workgroup_name')
                request_metadata = rs_client.execute_statement(
                    WorkgroupName=workgroup_name,
                    Database=database_name,
                    Sql=sql,
                )

            else:
                cluster_identifier = self.endpoint_attr.get('cluster_identifier')
                request_metadata = rs_client.execute_statement(
                    ClusterIdentifier=cluster_identifier,
                    Database=database_name,
                    DbUser=database_user,
                    Sql=sql
                )

            if request_metadata is not None:
                request_id = request_metadata.get('Id')
                logger.debug(f"Query with Id: {request_id} was executed:\n{sql}")
            else:
                logger.error(f"Unexpected Error during the execution:\n{sql}")
                sys.exit()

            last_query_status = ""
            result = False
            finished = False
            result_data = None

            statement_timeout = self.statement_timeout

            while statement_timeout > 0 and not finished:
                time.sleep(sleep_sec)
                statement_timeout -= sleep_sec

                statement_description = rs_client.describe_statement(Id=request_id)

                query_status = statement_description.get('Status')

                if query_status == "FINISHED":
                    finished = True
                    result = statement_description.get('HasResultSet')

                elif query_status == "FAILED":
                    finished = True
                    logger.error(f"Query {query_status}: {statement_description.get('Error')}")

                else:
                    if query_status != last_query_status:
                        last_query_status = query_status
                        logger.debug(f"The last query status is: {last_query_status}")

            if not finished:
                logger.debug(f"The query statement_timeout of {self.statement_timeout} sec is expired.")

            if result:
                result_data = rs_client.get_statement_result(Id=request_id)


            return result_data, request_id

        except ClientError as e:
            logger.error(e)

    def get_metaquery_result(self, sql):
        """
        """
        result_data, request_id = self.execute_sql(sql, sleep_sec=1)

        result_records = result_data.get("Records")
        mapped_result = self.map_result_to_metaquery(result_records)
        return mapped_result

    def map_result_to_metaquery(self, result_records):
        """
        """
        meta_query_structure = ['source_schema_name', 'source_object_name', 'ordinal_position', 'column_name', 'is_nullable', 'data_type', 'character_maximum_length', 'numeric_precision', 'numeric_scale', 'primary_key']
        rec_array = []
        for rec in result_records:
            new_rec = {}


            for idx in range(len(rec)):
                value=None
                for k, v in rec[idx].items():
                    value=v
                    if value is True or value is False:
                        value=None
                new_rec.update({meta_query_structure[idx]: value})
            rec_array.append(new_rec)

        return rec_array

    def extract_to(self, extract_to_stmt):
        """
        """
        result_data, request_id = self.execute_sql(extract_to_stmt, sleep_sec=5)
        return result_data

    def load_from(self, load_from_stmt):
        """
        """
        result_data, request_id = self.execute_sql(load_from_stmt, sleep_sec=5)
        return result_data

    def drop_table(self, schema_table_name):

        sql = f"DROP TABLE IF EXISTS {schema_table_name}"
        result_data, request_id = self.execute_sql(sql, sleep_sec=1)
        return result_data

### --------------------------------------------------------------- ###

import redshift_connector

class RSConnectorTCP():

    def __init__(self, endpoint_attr):
        """
        """
        self.connector_id = 'redshift'
        self.metaquery_quote = '"'
        self.conn = self.get_connection(endpoint_attr)
        self.f_handler = FileHandler()

    def compose_connection_params(self, endpoint_attr):
        connection_params = {"user": endpoint_attr.get('database_user'),
                             "password": endpoint_attr.get('database_password'),
                             "host": endpoint_attr.get('database_host'),
                             "port": endpoint_attr.get('database_port'),
                             "database": endpoint_attr.get('database_name')}

        return connection_params
    def get_connection(self, endpoint_conn_attr):
        """
        :return:
        """
        connection_params = self.compose_connection_params(endpoint_conn_attr)

        try:
            conn = redshift_connector.connect(**connection_params)
            redshift_connector.paramstyle = 'named'
            conn.autocommit = True

        except (Exception, redshift_connector.DatabaseError) as error:
            logger.debug(connection_params)
            logger.error(error)
            sys.exit()

        return conn

    def get_metaquery_result(self, sql):
        """
        """
        result_data = self.get_query_result(sql)
        return result_data

    def get_query_result(self, sql):
        """
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                df = cur.fetch_dataframe()
                rows = df.to_dict('records')
                logger.debug(f"Found {cur.rowcount} rows in query.")
                return rows

        except (Exception, redshift_connector.DatabaseError) as error:
            logger.debug(sql)
            logger.error(error)
            sys.exit()

    def extract_to(self, extract_to_stmt):
        """
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(extract_to_stmt)

        except (Exception, redshift_connector.DatabaseError) as error:
            logger.debug(extract_to_stmt)
            logger.error(error)
            sys.exit()

    def load_from(self, load_from_stmt):
        """
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(load_from_stmt)
        except (Exception, redshift_connector.DatabaseError) as error:
            logger.debug('\n'+load_from_stmt)
            logger.error(error)
            sys.exit()


    def drop_table(self, schema_table_name):
        try:
            sql = f"DROP TABLE IF EXISTS {schema_table_name}"
            with self.conn.cursor() as cur:
                cur.execute(sql)  # Make an API request.
            logger.debug(f"Table {schema_table_name} was dropped.")
        except (Exception, redshift_connector.DatabaseError) as error:
            logger.debug(sql)
            logger.error(error)

    def execute_sql(self, sql):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)  # Make an API request.
            logger.debug(f"Query executed:\n{sql}")
        except (Exception, redshift_connector.DatabaseError) as error:
            logger.debug(sql)
            logger.error(error)


