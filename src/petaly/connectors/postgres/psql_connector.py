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
import psycopg
from psycopg.rows import dict_row


class PsqlConnector():
    """PostgreSQL connector implementation."""

    def __init__(self, endpoint_attr):
        """Initialize PostgreSQL connector.
        
        Args:
            endpoint_attr (dict): Dictionary containing connection parameters:
                - database_user: Username
                - database_password: Password
                - database_host: Host
                - database_port: Port
                - database_name: Database name
        """
        self.connector_id = 'postgres'
        self.metaquery_quote = '"'
        self.endpoint_attr = endpoint_attr
        self.conn = None
        self.get_connection(endpoint_attr)

    def get_connection_dsn(self):
        """Get connection DSN from environment variable.
        
        Returns:
            str: Connection DSN string.
        """
        return os.getenv("PSQL_DSN")

    def compose_connection_string(self, endpoint_attr):
        """Compose connection string from endpoint attributes.
        
        Args:
            endpoint_attr (dict): Dictionary containing connection parameters.
            
        Returns:
            str: Connection string.
        """
        return "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            user=endpoint_attr.get('database_user'),
            password=endpoint_attr.get('database_password'),
            host=endpoint_attr.get('database_host'),
            port=endpoint_attr.get('database_port'),
            database=endpoint_attr.get('database_name')
        )

    def get_connection(self, endpoint_attr: dict) -> None:
        """
        Get database connection using connection attributes.
        
        Args:
            endpoint_attr: Dictionary containing connection attributes
            
        Raises:
            SystemExit: If connection fails with a user-friendly error message
        """
        try:
            connection_string = self.compose_connection_string(endpoint_attr)
            self.conn = psycopg.connect(connection_string, row_factory=dict_row)
        except psycopg.OperationalError as e:
            error_msg = str(e)
            if "Connection refused" in error_msg:
                host = endpoint_attr.get('database_host', 'localhost')
                port = endpoint_attr.get('database_port', '5432')
                logger.error(f"Can't connect to PostgreSQL server on '{host}:{port}'")
                raise SystemExit()
            elif "password authentication failed" in error_msg:
                logger.error(f"Access denied for user '{endpoint_attr.get('database_user', 'unknown')}'")
                raise SystemExit()
            elif "database" in error_msg and "does not exist" in error_msg:
                logger.error(f"Unknown database '{endpoint_attr.get('database_name', 'unknown')}'")
                raise SystemExit()
            else:
                logger.error(f"PostgreSQL error: {error_msg}")
                raise SystemExit()
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise SystemExit()

    def get_query_result(self, sql):
        """Execute a query and return the results.
        
        Args:
            sql (str): SQL query to execute.
            
        Returns:
            list: Query results.
            
        Raises:
            psycopg.Error: If query execution fails.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                logger.debug(f"Found {cur.rowcount} rows in query.")
                return rows
        except psycopg.Error as error:
            logger.error(f"PostgreSQL error: {str(error)}")
            raise

    def extract_to(self, extract_to_stmt, data_fpath):
        """Extract data to a file.
        
        Args:
            extract_to_stmt (str): COPY statement.
            data_fpath (str): Path to output file.
            
        Raises:
            psycopg.Error: If extraction fails.
        """
        try:
            with open(data_fpath, "wb") as f:
                with self.conn.cursor() as cur:
                    with cur.copy(extract_to_stmt) as copy:
                        for data in copy:
                            f.write(data)
        except psycopg.Error as error:
            logger.error(f"PostgreSQL error: {str(error)}")
            raise

    def load_from(self, load_from_stmt, data_fpath):
        """Load data from a file.
        
        Args:
            load_from_stmt (str): COPY statement.
            data_fpath (str): Path to input file.
            
        Raises:
            psycopg.Error: If loading fails.
        """
        BLOCK_SIZE = 8192

        try:
            with open(data_fpath, "r") as f:
                with self.conn.cursor() as cur:
                    with cur.copy(load_from_stmt) as copy:
                        while data := f.read(BLOCK_SIZE):
                            copy.write(data)
                    self.conn.commit()
        except psycopg.Error as error:
            logger.error(f"PostgreSQL error: {str(error)}")
            raise

    def drop_table(self, schema_table_name):
        """Drop a table.
        
        Args:
            schema_table_name (str): Schema-qualified table name.
            
        Raises:
            psycopg.Error: If dropping fails.
        """
        try:
            sql = f"DROP TABLE IF EXISTS {schema_table_name}"
            self.conn.execute(sql)
            self.conn.commit()
            logger.debug(f"Table {schema_table_name} was dropped.")
        except psycopg.Error as error:
            logger.error(f"PostgreSQL error: {str(error)}")
            raise

    def execute_sql(self, sql):
        """Execute a SQL statement.
        
        Args:
            sql (str): SQL statement to execute.
            
        Raises:
            psycopg.Error: If execution fails.
        """
        try:
            self.conn.execute(sql)
            self.conn.commit()
            logger.debug(f"Query was executed: {sql}")
        except psycopg.Error as error:
            logger.error(f"PostgreSQL error: {str(error)}")
            raise

    def close(self):
        """Close the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None