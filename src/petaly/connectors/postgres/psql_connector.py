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

import os, sys
import psycopg
from psycopg.rows import dict_row


class PsqlConnector():

    def __init__(self, endpoint_attr):
        """
        """
        self.connector_id = 'postgres'
        self.column_quotes = '"'
        self.conn = self.get_connection(endpoint_attr)

    def get_connection_dsn(selfg):
        """
        """
        return os.getenv("PSQL_DSN")



    def get_connection(self, endpoint_attr):
        """
        """
        try:
            connection_string = self.compose_connection_string(endpoint_attr)

            conn = psycopg.connect(connection_string, row_factory=dict_row)

        except (Exception, psycopg.DatabaseError) as error:
            logger.error(error)
            sys.exit()

        return conn

    def compose_connection_string(self, endpoint_attr):
            """ It composes connection for Postgresql based on variables in pipeline.yaml file

            :return: return connection string
            """

            connection_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
                                    user=endpoint_attr.get('database_user'),
                                    password=endpoint_attr.get('database_password'),
                                    host= endpoint_attr.get('database_host'),
                                    port= endpoint_attr.get('database_port'),
                                    database=endpoint_attr.get('database_name') )

            return connection_string

    def get_query_result(self, sql):
        """
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                logger.info(f"Found {cur.rowcount} rows in query.")
                return rows

        except (Exception, psycopg.DatabaseError) as error:
            logger.info(sql)
            logger.error(error)
            sys.exit()

    def extract_to(self, extract_to_stmt, data_fpath):
        """
        """
        with open(data_fpath, "wb") as f:
            with self.conn.cursor() as cur:
                with cur.copy(extract_to_stmt) as copy:
                    for data in copy:
                        f.write(data)

    def load_from(self, load_from_stmt, data_fpath):
        """
        """
        BLOCK_SIZE = 8192

        with open(data_fpath, "r") as f:
            try:
                with self.conn.cursor() as cur:
                    with cur.copy(load_from_stmt) as copy:
                        while data := f.read(BLOCK_SIZE):
                            copy.write(data)
                    self.conn.commit()
            except (Exception, psycopg.DatabaseError) as error:
                logger.error(error)

    def drop_table(self, schema_table_name):
        try:
            sql = f"DROP TABLE IF EXISTS {schema_table_name}"
            self.conn.execute(sql)  # Make an API request.
            self.conn.commit()
            logger.info(f"Table {schema_table_name} was dropped.")
        except (Exception, psycopg.DatabaseError) as error:
            logger.info(sql)
            logger.error(error)

    def execute_sql(self, sql):
        try:
            self.conn.execute(sql)  # Make an API request.
            self.conn.commit()
            logger.info("Query was executed.")
        except (Exception, psycopg.DatabaseError) as error:
            logger.info(sql)
            logger.error(error)