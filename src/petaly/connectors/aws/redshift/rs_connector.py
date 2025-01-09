import logging

logger = logging.getLogger(__name__)

import os, sys
import redshift_connector
import boto3
from botocore.config import Config

from petaly.utils.file_handler import FileHandler

class RSConnector():

    def __init__(self, endpoint_attr):
        """
        """
        self.connector_id = 'redshift'
        self.metaquery_quote = '"'
        #self.pipeline = pipeline
        self.bucket_prefix = 's3://'
        #self.conn = self.get_connection()
        self.conn = self.get_connection(endpoint_attr)
        self.f_handler = FileHandler()

    def get_s3_client(self, aws_access_key_id, aws_secret_access_key, region_name):

        config = Config(
            region_name=region_name,
            client_context_params={
                'AWS_ACCESS_KEY_ID': aws_access_key_id,
                'AWS_SECRET_ACCESS_KEY': aws_secret_access_key
            }
        )
        return boto3.client('s3', config=config)

    def get_connection_dsn(selfg):
        """
        :return:
        """
        return os.getenv("RS_DSN")

    def compose_connection_params(self, endpoint_attr, use_iam=False):
        connection_params = {"user": endpoint_attr.get('database_user'),
                             "password": endpoint_attr.get('database_password'),
                             "host": endpoint_attr.get('database_host'),
                             "port": endpoint_attr.get('database_port'),
                             "database": endpoint_attr.get('database_name')}


        # ToDo: not in use now
        if use_iam == True:
            connection_params.update(
                {   'iam': True,
                    'cluster_identifier': 'cluster-name',
                    'region': 'my_region',
                    'profile': 'my_profile',
                    'access_key_id': 'my_aws_access_key_id',
                    'secret_access_key': 'my_aws_secret_access_key',
                    'session_token': 'my_aws_session_token',
                })

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
            logging.info(connection_params)
            logging.error(error)
            sys.exit()

        return conn

    def get_query_result(self, sql):
        """
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                df = cur.fetch_dataframe()
                rows = df.to_dict('records')
                logging.info(f"Found {cur.rowcount} rows in query.")
                return rows

        except (Exception, redshift_connector.DatabaseError) as error:
            logging.info(sql)
            logging.error(error)
            sys.exit()

    def extract_to(self, extract_to_stmt):
        """
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(extract_to_stmt)

        except (Exception, redshift_connector.DatabaseError) as error:
            logging.info(extract_to_stmt)
            logging.error(error)
            sys.exit()

    def load_from(self, load_from_stmt):
        """
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(load_from_stmt)
        except (Exception, redshift_connector.DatabaseError) as error:
            logging.info('\n'+load_from_stmt)
            logging.error(error)
            sys.exit()


    def drop_table(self, schema_table_name):
        try:
            sql = f"DROP TABLE IF EXISTS {schema_table_name}"
            with self.conn.cursor() as cur:
                cur.execute(sql)  # Make an API request.
            logging.info(f"Table {schema_table_name} was dropped.")
        except (Exception, redshift_connector.DatabaseError) as error:
            logging.info(sql)
            logging.error(error)

    def execute_sql(self, sql):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)  # Make an API request.
            logging.info(f"Query executed:\n{sql}")
        except (Exception, redshift_connector.DatabaseError) as error:
            logging.info(sql)
            logging.error(error)


    def drop_object_from_bucket(self, bucket_name, file_object_name):
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)
        for object_summary in bucket.objects.filter(Prefix=file_object_name):
            object_summary.delete()

    def download_files_from_bucket(self, bucket_name, object_name, output_pipeline_dpath):

        object_list = self.get_bucket_file_list(bucket_name, object_name)
        s3_client = boto3.client('s3')

        for object_fpath in object_list:
            path_extensions = self.f_handler.get_file_extensions(object_fpath)
            path_extensions.insert(0, '.csv')
            object_path_extensions = ''.join(path_extensions)

            destination_object_fpath = self.f_handler.replace_file_extension(object_fpath, object_path_extensions)

            s3_client.download_file(bucket_name, object_fpath,
                                    os.path.join(output_pipeline_dpath, destination_object_fpath))

    def get_bucket_file_list(self, bucket_name, object_name):
        try:
            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket(bucket_name)
            object_list = []

            for object_summary in bucket.objects.filter(Prefix=object_name):
                object_list.append(object_summary.key)

            return object_list

        except (Exception) as error:
            logging.info(bucket_name, object_name)
            logging.error(error)

    def load_files_to_s3_bucket(self, bucket_name, object_name, object_file_list):

        try:

            s3_client = boto3.client('s3')
            for object_fpath in object_file_list:
                bucket_path = object_name + "/" + os.path.basename(object_fpath)
                s3_client.upload_file(object_fpath, bucket_name, bucket_path)
                logging.info(f"Load file {object_fpath} to destination s3://{bucket_name}/{bucket_path}")

        except (Exception, s3_client.exceptions) as error:
            logging.info(bucket_name, object_name, object_file_list)
            logging.error(error)

