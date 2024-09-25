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

from petaly.utils.utils import measure_time

import os, sys
import csv
import mysql.connector


class MysqlConnector():

    def __init__(self, endpoint_attr):
        """ """
        self.connector_id = "mysql"
        self.column_quotes = ""

        self.conn = self.get_connection(endpoint_attr)
        self.database = endpoint_attr.get('database_name')


    def get_connection_dsn(self):
        return os.getenv("MYSQL_DSN")

    def compose_connection_params (self, endpoint_attr):
        connection_params = {"user": endpoint_attr.get('database_user'),
                             "password": endpoint_attr.get('database_password'),
                             "host": endpoint_attr.get('database_host'),
                             "port": endpoint_attr.get('database_port'),
                             "database": endpoint_attr.get('database_name'),
                             'allow_local_infile': True,
                             'use_pure': False
                             }
        return connection_params
    def get_connection(self, endpoint_attr):
        """
        """
        connection_params = self.compose_connection_params(endpoint_attr)
        try:
            conn = mysql.connector.connect(**connection_params)
            return conn

        except (mysql.connector.Error, IOError) as error:
            logger.error(error)
            sys.exit()

    def get_cursor(self):
        return self.conn.cursor(dictionary=True, buffered=False)

    def get_query_result(self, query):
        """ """
        try:

            with self.get_cursor() as cur:
                cur.execute(f"USE {self.database};")
                cur.execute(query)
                rows = cur.fetchall()
                logger.info(f"Found {cur.rowcount} rows in query.")

                return rows

        except (mysql.connector.Error, IOError) as error:
            logger.error(error)
            sys.exit()

    def extract_to(self, extract_to_stmt, data_fpath):
        self.extract_to_fetchone(extract_to_stmt, data_fpath)

        # extract_to_fetchmany can be used too
        #self.extract_to_fetchmany(extract_to_stmt, data_fpath, batch_size=10000)
        ### deprecated
        #self.extract_to_fetchall(extract_to_stmt, data_fpath)


    def extract_to_fetchone(self, extract_to_stmt, data_fpath):
        """  """
        try:
            cur = self.get_cursor()
            cur.execute(extract_to_stmt)
            with (open(data_fpath, 'w', newline='') as file):

                row = cur.fetchone()
                csvwriter = csv.DictWriter(file, fieldnames=row, delimiter=',', quotechar='"',
                                           quoting=csv.QUOTE_MINIMAL)

                if row is not None:
                    csvwriter.writeheader()

                while row is not None:
                    csvwriter.writerow(row)
                    row = cur.fetchone()

                file.close()

            cur.close()

        except (mysql.connector.Error, IOError) as error:
            logger.info(extract_to_stmt)
            logger.error(error)

    def describe_table(self, table_name):
        show_table_query = f"DESCRIBE {table_name}"
        with self.get_cursor() as cursor:
            cursor.execute(show_table_query)
            # Fetch rows from last executed query
            result = cursor.fetchall()
            for row in result:
                print(row)

    def extract_to_fetchmany(self, extract_to_stmt, data_fpath, batch_size=10000):
        """ """
        try:
            cur = self.get_cursor()
            cur.execute(extract_to_stmt)

            with (open(data_fpath, 'w', newline='') as file):
                row = cur.fetchone()
                csvwriter = csv.DictWriter(file, fieldnames=row, delimiter=',', quotechar='"',
                                           quoting=csv.QUOTE_MINIMAL)
                csvwriter.writeheader()
                csvwriter.writerow(row)

                rows = cur.fetchmany(size=batch_size)
                while len(rows) > 0:
                    csvwriter.writerows(rows)
                    rows = cur.fetchmany(size=batch_size)

                file.close()

            cur.close()

        except (mysql.connector.Error, IOError) as error:
            logger.info(extract_to_stmt)
            logger.error(error)

    @measure_time
    def extract_to_fetchall(self, extract_to_stmt, data_fpath):
        """  """
        try:
            cur = self.get_cursor()
            cur.execute(extract_to_stmt)

            with (open(data_fpath, 'w', newline='') as file):
                row = cur.fetchone()
                csvwriter = csv.DictWriter(file, fieldnames=row, delimiter=',', quotechar='"',
                                           quoting=csv.QUOTE_MINIMAL)
                csvwriter.writeheader()
                csvwriter.writerow(row)
                rows = cur.fetchall()
                csvwriter.writerows(rows)

                file.close()

            cur.close()

        except (mysql.connector.Error, IOError) as error:
            logger.info(extract_to_stmt)
            logger.error(error)

    def load_from(self, load_from_stmt):
        """  """
        try:
            cur = self.get_cursor()
            cur.execute(f"USE {self.database};")
            cur.execute(load_from_stmt)
            self.conn.commit()
        except (mysql.connector.Error, IOError) as error:
            logger.info(load_from_stmt)
            logger.error(error)

    def drop_table(self, table_name):
        try:
            sql = f"DROP TABLE IF EXISTS {table_name}"
            cur = self.get_cursor()
            cur.execute(f"USE {self.database};")
            cur.execute(sql)  # Make an API request.
            self.conn.commit()
            logger.info(f"Table {table_name} was dropped.")

        except (mysql.connector.Error, IOError) as error:
            logger.info(sql)
            logger.error(error)

    def execute_sql(self, sql_stmt):
        try:
            cur = self.get_cursor()
            cur.execute(f"USE {self.database};")
            cur.execute(sql_stmt)  # Make an API request.
            self.conn.commit()
            logger.info("Query was executed.")

        except (mysql.connector.Error, IOError) as error:
            logger.info(sql_stmt)
            logger.error(error)

if __name__ == "__main__":
      pass
