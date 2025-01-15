from petaly.utils.utils import FormatDict
from petaly.core.db_loader import DBLoader

from petaly.connectors.aws.redshift.rs_connector import RSConnector


class RSLoader(DBLoader):

    def __init__(self, pipeline):
        #connection_params = self.get_connection_params(pipeline.target_attr)
        self.db_connector = RSConnector(pipeline.target_attr)

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.target_attr.get('aws_bucket_name')
        self.cloud_bucket_path = self.db_connector.bucket_prefix + self.cloud_bucket_name + '/'
        self.cloud_iam_role = self.pipeline.target_attr.get('aws_iam_role')

    def load_data(self):
        super().load_data()

    def drop_table(self, loader_obj_conf: dict):

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        self.db_connector.drop_table(schema_table_name)

    def create_table(self, loader_obj_conf: dict):
        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        loader_obj_conf = self.compose_create_table_stmt(loader_obj_conf)
        self.f_handler.save_file(table_ddl_dict.get('create_table_stmt_fpath'),
                                 table_ddl_dict.get('create_table_stmt'))

        self.db_connector.execute_sql(loader_obj_conf.get('table_ddl_dict').get('create_table_stmt'))

    def load_from(self, loader_obj_conf):

        object_name = loader_obj_conf.get('object_name')
        # 1. cleanup object from bucket
        self.db_connector.drop_object_from_bucket(self.cloud_bucket_name, object_name)

        # 2. drop and recreate table
        if loader_obj_conf.get('recreate_destination_object') == True:
            self.drop_table(loader_obj_conf)

        self.create_table(loader_obj_conf)
        output_data_object_dir = loader_obj_conf.get('output_data_object_dir')

        self.f_handler.gzip_csv_files(output_data_object_dir, cleanup_file=True)

        file_list = self.f_handler.get_specific_files(output_data_object_dir, '*.csv.gz')

        self.db_connector.load_files_to_s3_bucket(self.cloud_bucket_name, object_name, file_list)
        s3_file_list = self.db_connector.get_bucket_file_list(self.cloud_bucket_name, object_name)
        load_from_stmt = loader_obj_conf.get('load_from_stmt')

        for data_fpath in s3_file_list:
            path_to_data_file = self.cloud_bucket_path + data_fpath
            load_from_stmt = load_from_stmt.format_map(
                FormatDict(path_to_data_file=path_to_data_file))

            self.db_connector.load_from(load_from_stmt)

    def compose_create_table_stmt(self, loader_obj_conf):

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        create_table_stmt = table_ddl_dict.get('create_table_stmt')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        column_datatype_list = table_ddl_dict.get('column_datatype_list')

        create_table_stmt = create_table_stmt.format_map(FormatDict(schema_table_name=schema_table_name,
                                                                    column_datatype_list=column_datatype_list,
                                                                    partition_by='',
                                                                    cluster_by='',
                                                                    table_options='',
                                                                    alter_table_primary_or_unique_key=''))
        loader_obj_conf.get('table_ddl_dict').update({'create_table_stmt': create_table_stmt})

        return loader_obj_conf

    def compose_from_options(self, loader_obj_conf):
        """
        """
        load_options = ""
        object_settings = loader_obj_conf.get("object_settings")
        load_options += "FORMAT AS CSV "

        columns_delimiter = object_settings.get("columns_delimiter")

        if columns_delimiter == '\t':
            load_options += "DELIMITER '\\t' "
        else:
            load_options += f"DELIMITER '{columns_delimiter}' "

        load_options += "GZIP "
        skip_leading_rows = 1 if object_settings.get("header") is None or True else 0
        load_options += f"IGNOREHEADER {skip_leading_rows} "

        columns_quote = object_settings.get("columns_quote")
        if columns_quote not in ('double','single'):
            load_options += "REMOVEQUOTES "

        return load_options

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        """ Its compose a copy from statement """
        load_data_options = self.compose_from_options(loader_obj_conf)
        load_from_stmt = self.f_handler.load_file(self.connector_load_from_stmt_fpath)

        table_ddl_dict = loader_obj_conf.get('table_ddl_dict')
        schema_table_name = f"{table_ddl_dict.get('schema_name')}.{table_ddl_dict.get('table_name')}"
        column_list = '' if table_ddl_dict.get('column_list') == None else '(' + table_ddl_dict.get('column_list') + ')'

        load_from_stmt = load_from_stmt.format_map(FormatDict(schema_table_name=schema_table_name,
                                                               column_list=column_list,
                                                              iam_role=self.cloud_iam_role,
                                                              load_from_options=load_data_options))
        load_from_file_fpath = loader_obj_conf.get('load_from_stmt_fpath')
        self.f_handler.save_file(load_from_file_fpath, load_from_stmt)

        return load_from_stmt

    def cleanup_object_relations(self, object_name):
        self.db_connector.drop_object_from_bucket(self.cloud_bucket_name, object_name)
