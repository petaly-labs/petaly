import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from petaly.connectors.bigquery.bq_connector import BQConnector
from petaly.connectors.bigquery.bq_extractor import BQExtractor
from petaly.connectors.bigquery.bq_loader import BQLoader
from petaly.core.pipeline import Pipeline


class TestBQConnector:
    @pytest.fixture
    def connector(self):
        return BQConnector()

    @patch('google.cloud.bigquery.Client')
    def test_extract_to(self, mock_client, connector):
        # Setup
        table_ref = "project.dataset.table"
        destination_uri = "gs://bucket/path"
        region = "us-central1"
        
        mock_job = MagicMock()
        mock_client.return_value.extract_table.return_value = mock_job
        
        # Execute
        connector.extract_to(table_ref, destination_uri, region)
        
        # Verify
        mock_client.return_value.extract_table.assert_called_once()
        mock_job.result.assert_called_once()

    @patch('google.cloud.bigquery.Client')
    def test_execute_sql(self, mock_client, connector):
        # Setup
        query = "SELECT * FROM table"
        mock_job = MagicMock()
        mock_client.return_value.query.return_value = mock_job
        
        # Execute
        connector.execute_sql(query)
        
        # Verify
        mock_client.return_value.query.assert_called_once_with(query)
        mock_job.result.assert_called_once()

    @patch('google.cloud.bigquery.Client')
    def test_get_metadata_result(self, mock_client, connector):
        # Setup
        query = "SELECT * FROM table"
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [{"col1": "val1"}, {"col2": "val2"}]
        mock_client.return_value.query_and_wait.return_value = mock_result
        
        # Execute
        result = connector.get_metadata_result(query)
        
        # Verify
        assert len(result) == 2
        mock_client.return_value.query_and_wait.assert_called_once_with(query)

    @patch('google.cloud.bigquery.Client')
    def test_load_from(self, mock_client, connector):
        # Setup
        bq_job_config = {"source_format": "CSV"}
        data_fpath = "test.csv"
        table_id = "project.dataset.table"
        region = "us-central1"
        
        mock_table = MagicMock()
        mock_table.num_rows = 100
        mock_client.return_value.get_table.return_value = mock_table
        mock_job = MagicMock()
        mock_client.return_value.load_table_from_file.return_value = mock_job
        
        # Execute
        with tempfile.NamedTemporaryFile() as temp_file:
            connector.load_from(bq_job_config, temp_file.name, table_id, False, region)
        
        # Verify
        mock_client.return_value.load_table_from_file.assert_called_once()
        mock_job.result.assert_called_once()

    @patch('google.cloud.bigquery.Client')
    def test_drop_table(self, mock_client, connector):
        # Setup
        table_id = "project.dataset.table"
        
        # Execute
        connector.drop_table(table_id)
        
        # Verify
        mock_client.return_value.delete_table.assert_called_once_with(table_id, not_found_ok=True)


class TestBQExtractor:
    @pytest.fixture
    def pipeline_mock(self):
        pipeline = MagicMock(spec=Pipeline)
        pipeline.m_conf = MagicMock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        pipeline.source_attr = {
            'gcp_bucket_name': 'test-bucket',
            'gcp_project_id': 'test-project',
            'gcp_region': 'us-central1'
        }
        pipeline.source_connector_id = "test_source"
        pipeline.m_conf.set_extractor_paths = MagicMock(return_value=None)
        return pipeline

    @pytest.fixture
    def extractor(self, pipeline_mock):
        return BQExtractor(pipeline_mock)

    @patch('petaly.connectors.bigquery.bq_connector.BQConnector.get_metadata_result')
    def test_get_query_result(self, mock_get_metadata, extractor):
        # Setup
        mock_get_metadata.return_value = [{"col1": "val1"}]
        
        # Execute
        result = extractor.get_query_result("SELECT * FROM table")
        
        # Verify
        assert len(result) == 1
        mock_get_metadata.assert_called_once()

    @patch('petaly.connectors.bigquery.bq_connector.BQConnector.extract_to')
    @patch('petaly.connectors.gs.gs_connector.GSConnector.delete_object_in_bucket')
    @patch('petaly.connectors.gs.gs_connector.GSConnector.download_files_from_bucket')
    def test_extract_to(self, mock_download, mock_delete, mock_extract, extractor):
        # Setup
        extractor_obj_conf = {
            'object_name': 'test_table',
            'extract_to_stmt': '"table_ref": "project.dataset.table", "destination_uri": "gs://bucket/path"',
            'output_data_object_dir': '/tmp',
            'blob_prefix': 'test/'
        }
        mock_download.return_value = ['file1.csv']
        
        # Execute
        extractor.extract_to(extractor_obj_conf)
        
        # Verify
        mock_delete.assert_called_once()
        mock_extract.assert_called_once()
        mock_download.assert_called_once()


class TestBQLoader:
    @pytest.fixture
    def pipeline_mock(self):
        pipeline = MagicMock(spec=Pipeline)
        pipeline.m_conf = MagicMock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        pipeline.target_attr = {
            'gcp_bucket_name': 'test-bucket',
            'gcp_project_id': 'test-project',
            'gcp_region': 'us-central1'
        }
        pipeline.target_connector_id = "test_target"
        pipeline.m_conf.set_loader_paths = MagicMock(return_value=None)
        return pipeline

    @pytest.fixture
    def loader(self, pipeline_mock):
        return BQLoader(pipeline_mock)

    def test_get_table_id(self, loader):
        # Setup
        table_ddl_dict = {
            'schema_name': 'test_schema',
            'table_name': 'test_table'
        }
        
        # Execute
        table_id = loader.get_table_id(table_ddl_dict)
        
        # Verify
        assert table_id == "test-project.test_schema.test_table"

    @patch('petaly.connectors.bigquery.bq_connector.BQConnector.drop_table')
    def test_drop_table(self, mock_drop, loader):
        # Setup
        loader_obj_conf = {
            'table_ddl_dict': {
                'schema_name': 'test_schema',
                'table_name': 'test_table'
            }
        }
        
        # Execute
        loader.drop_table(loader_obj_conf)
        
        # Verify
        mock_drop.assert_called_once_with("test-project.test_schema.test_table")

    @patch('petaly.connectors.bigquery.bq_connector.BQConnector.execute_sql')
    @patch('petaly.utils.file_handler.FileHandler.save_file')
    def test_create_table(self, mock_save, mock_execute, loader):
        # Setup
        loader_obj_conf = {
            'table_ddl_dict': {
                'schema_name': 'test_schema',
                'table_name': 'test_table',
                'create_table_stmt': 'CREATE TABLE {schema_name}.{table_name}',
                'create_table_stmt_fpath': '/tmp/create.sql'
            }
        }
        
        # Execute
        loader.create_table(loader_obj_conf)
        
        # Verify
        mock_save.assert_called_once()
        mock_execute.assert_called_once()

    def test_compose_from_options(self, loader):
        # Setup
        loader_obj_conf = {
            'object_settings': {
                'columns_delimiter': ',',
                'header': True,
                'columns_quote': 'double'
            }
        }
        
        # Execute
        options = loader.compose_from_options(loader_obj_conf)
        
        # Verify
        assert options['delimiter'] == ','
        assert options['header'] is True
        assert options['quote_char'] == '"' 