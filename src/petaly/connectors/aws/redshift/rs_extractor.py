import logging

from petaly.core.db_extractor import DBExtractor
from petaly.utils.utils import FormatDict

from petaly.connectors.aws.redshift.rs_connector import RSConnector


class RSExtractor(DBExtractor):
    def __init__(self, pipeline):
        #connection_params = self.get_connection_params(pipeline.source_attr)
        self.db_connector = RSConnector(pipeline.source_attr)

        super().__init__(pipeline)
        self.cloud_bucket_name = self.pipeline.source_attr.get('aws_bucket_name')
        self.cloud_bucket_path = self.db_connector.bucket_prefix + self.cloud_bucket_name + '/'
        self.cloud_iam_role = self.pipeline.source_attr.get('aws_iam_role')

    def extract_data(self):
        super().extract_data()

    def get_query_result(self, meta_query):
        return self.db_connector.get_query_result(meta_query)

    def extract_to(self, extractor_obj_conf):
        object_name = extractor_obj_conf.get('object_name')
        extract_to_stmt = extractor_obj_conf.get('extract_to_stmt')
        logging.info(f"Statement to execute:{extract_to_stmt}")

        # cleanup object from s3 bucket
        self.db_connector.drop_object_from_bucket(self.cloud_bucket_name, object_name)

        # export data into s3 bucket
        self.db_connector.extract_to(extract_to_stmt)

        # download export from s3 into local folder
        self.db_connector.download_files_from_bucket(self.cloud_bucket_name, object_name, self.pipeline.output_pipeline_dpath)

    def compose_extract_to_stmt(self, extract_to_stmt, extractor_obj_conf) -> dict:
        """ Its save copy statement into file
        """
        extract_data_options = self.compose_extract_options(extractor_obj_conf)
        object_name = extractor_obj_conf.get('object_name')
        extract_to_fpath = self.cloud_bucket_path + object_name  + '/' + self.pipeline.data_dname + '/' + object_name + '_'

        extract_to_stmt = extract_to_stmt.format_map(
        					FormatDict( column_list=extractor_obj_conf.get('column_list'),
                                        schema_name=extractor_obj_conf.get('source_schema_name'),
                                        table_name=extractor_obj_conf.get('source_object_name'),
                                        extract_to_fpath = extract_to_fpath,
                                        extract_to_options=extract_data_options,
                                        iam_role=self.cloud_iam_role
                                       ))

        return extract_to_stmt

    def compose_extract_options(self, extractor_obj_conf):
        """ CSV
            DELIMITER AS ','
            GZIP
            HEADER
            PARALLEL OFF
            ALLOWOVERWRITE
            MAXFILESIZE 100 MB
        """

        object_settings = extractor_obj_conf.get('object_settings')
        extract_options = ""

        if object_settings.get('header'):
            extract_options += f"HEADER "

        columns_delimiter = object_settings.get('columns_delimiter')
        if columns_delimiter == '\t':
            extract_options += "DELIMITER '\\t' "
        else:
            extract_options += f"DELIMITER '{columns_delimiter}' "

        return extract_options