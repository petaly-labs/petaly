import pytest
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock, call
import boto3
import redshift_connector
from botocore.exceptions import ClientError

from petaly.connectors.redshift.rs_connector import RSConnectorIAM, RSConnectorTCP
from petaly.connectors.redshift.rs_extractor import RSExtractor
from petaly.connectors.redshift.rs_loader import RSLoader
from petaly.connectors.s3.s3_connector import S3Connector

class TestRSConnectorIAM:
    @pytest.fixture
    def endpoint_attr(self):
        """Create mock endpoint attributes for IAM authentication."""
        return {
            'aws_access_key_id': 'test_access_key',
            'aws_secret_access_key': 'test_secret_key',
            'aws_profile_name': None,  # Don't use profile to avoid AWS config issues
            'aws_region': 'us-west-2',
            'database_name': 'test_db',
            'database_user': 'test_user',
            'is_serverless': 'false',
            'cluster_identifier': 'test-cluster'
        }
    
    @patch('boto3.session.Session')
    def test_connector_initialization(self, mock_session, endpoint_attr):
        """Test Redshift IAM connector initialization."""
        # Setup mock
        mock_aws_session = MagicMock()
        mock_session.return_value = mock_aws_session
        
        # Create connector and test
        connector = RSConnectorIAM(endpoint_attr)
        assert connector.connector_id == "redshift"
        assert connector.metaquery_quote == '"'
        assert connector.aws_session == mock_aws_session
        assert connector.is_serverless is False
        
        # Verify session creation
        mock_session.assert_called_once_with(
            aws_access_key_id='test_access_key',
            aws_secret_access_key='test_secret_key',
            profile_name=None,
            region_name='us-west-2'
        )
    
    @patch('boto3.session.Session')
    def test_execute_sql_cluster(self, mock_session, endpoint_attr):
        """Test executing SQL on a Redshift cluster."""
        # Setup mocks
        mock_aws_session = MagicMock()
        mock_client = MagicMock()
        mock_aws_session.client.return_value = mock_client
        mock_session.return_value = mock_aws_session
        
        # Mock response for execute_statement
        mock_client.execute_statement.return_value = {
            'Id': 'test_request_id'
        }
        
        # Mock response for describe_statement
        mock_client.describe_statement.return_value = {
            'Status': 'FINISHED',
            'HasResultSet': True
        }
        
        # Mock response for get_statement_result
        mock_result = {'Records': [
            [{'test_col': 'test_val'}]
        ]}
        mock_client.get_statement_result.return_value = mock_result
        
        # Create connector and test
        connector = RSConnectorIAM(endpoint_attr)
        result_data, request_id = connector.execute_sql("SELECT * FROM test_table")
        
        # Verify the mock was called correctly
        mock_client.execute_statement.assert_called_once_with(
            ClusterIdentifier='test-cluster',
            Database='test_db',
            DbUser='test_user',
            Sql="SELECT * FROM test_table"
        )
        mock_client.describe_statement.assert_called_with(Id='test_request_id')
        mock_client.get_statement_result.assert_called_once_with(Id='test_request_id')
        assert result_data == mock_result
        assert request_id == 'test_request_id'
    
    @patch('boto3.session.Session')
    def test_execute_sql_serverless(self, mock_session):
        """Test executing SQL on a Redshift Serverless instance."""
        # Setup endpoint attributes for serverless
        endpoint_attr = {
            'aws_access_key_id': 'test_access_key',
            'aws_secret_access_key': 'test_secret_key',
            'aws_profile_name': None,  # Don't use profile to avoid AWS config issues
            'aws_region': 'us-west-2',
            'database_name': 'test_db',
            'is_serverless': 'true',
            'workgroup_name': 'test-workgroup'
        }
        
        # Setup mocks
        mock_aws_session = MagicMock()
        mock_client = MagicMock()
        mock_aws_session.client.return_value = mock_client
        mock_session.return_value = mock_aws_session
        
        # Mock response for execute_statement
        mock_client.execute_statement.return_value = {
            'Id': 'test_request_id'
        }
        
        # Mock response for describe_statement
        mock_client.describe_statement.return_value = {
            'Status': 'FINISHED',
            'HasResultSet': True
        }
        
        # Mock response for get_statement_result
        mock_result = {'Records': [
            [{'test_col': 'test_val'}]
        ]}
        mock_client.get_statement_result.return_value = mock_result
        
        # Create connector and test
        connector = RSConnectorIAM(endpoint_attr)
        result_data, request_id = connector.execute_sql("SELECT * FROM test_table")
        
        # Verify the mock was called correctly
        mock_client.execute_statement.assert_called_once_with(
            WorkgroupName='test-workgroup',
            Database='test_db',
            Sql="SELECT * FROM test_table"
        )
        mock_client.describe_statement.assert_called_with(Id='test_request_id')
        mock_client.get_statement_result.assert_called_once_with(Id='test_request_id')
        assert result_data == mock_result
        assert request_id == 'test_request_id'
    
    @patch('boto3.session.Session')
    def test_get_metaquery_result(self, mock_session, endpoint_attr):
        """Test getting metadata query results."""
        # Setup mocks
        mock_aws_session = MagicMock()
        mock_client = MagicMock()
        mock_aws_session.client.return_value = mock_client
        mock_session.return_value = mock_aws_session
        
        # Mock response for execute_statement
        mock_client.execute_statement.return_value = {
            'Id': 'test_request_id'
        }
        
        # Mock response for describe_statement
        mock_client.describe_statement.return_value = {
            'Status': 'FINISHED',
            'HasResultSet': True
        }
        
        # Mock response for get_statement_result with metadata format
        mock_result = {'Records': [
            [
                {'StringValue': 'public'},  # source_schema_name
                {'StringValue': 'test_table'},  # source_object_name
                {'LongValue': 1},  # ordinal_position
                {'StringValue': 'id'},  # column_name
                {'BooleanValue': False},  # is_nullable
                {'StringValue': 'integer'},  # data_type
                {'NullValue': True},  # character_maximum_length
                {'LongValue': 32},  # numeric_precision
                {'LongValue': 0},  # numeric_scale
                {'BooleanValue': True}  # primary_key
            ]
        ]}
        mock_client.get_statement_result.return_value = mock_result
        
        # Create connector and test
        connector = RSConnectorIAM(endpoint_attr)
        result = connector.get_metaquery_result("SELECT * FROM information_schema.columns")
        
        # Verify the result is mapped correctly
        expected_result = [{
            'source_schema_name': 'public',
            'source_object_name': 'test_table',
            'ordinal_position': 1,
            'column_name': 'id',
            'is_nullable': None,
            'data_type': 'integer',
            'character_maximum_length': None,
            'numeric_precision': 32,
            'numeric_scale': 0,
            'primary_key': None
        }]
        assert result == expected_result

class TestRSConnectorTCP:
    @pytest.fixture
    def endpoint_attr(self):
        """Create mock endpoint attributes for TCP authentication."""
        return {
            'database_user': 'test_user',
            'database_password': 'test_password',
            'database_host': 'test-cluster.region.redshift.amazonaws.com',
            'database_port': 5439,
            'database_name': 'test_db'
        }
    
    @patch('redshift_connector.connect')
    def test_connector_initialization(self, mock_connect, endpoint_attr):
        """Test Redshift TCP connector initialization."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = RSConnectorTCP(endpoint_attr)
        assert connector.connector_id == "redshift"
        assert connector.metaquery_quote == '"'
        assert connector.conn == mock_conn
        
        # Verify connection parameters
        mock_connect.assert_called_once_with(
            user='test_user',
            password='test_password',
            host='test-cluster.region.redshift.amazonaws.com',
            port=5439,
            database='test_db'
        )
    
    @patch('redshift_connector.connect')
    def test_get_query_result(self, mock_connect, endpoint_attr):
        """Test executing a query and getting results."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_df = MagicMock()
        mock_rows = [{'id': 1, 'name': 'test'}]
        mock_df.to_dict.return_value = mock_rows
        mock_cursor.fetch_dataframe.return_value = mock_df
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = RSConnectorTCP(endpoint_attr)
        result = connector.get_query_result("SELECT * FROM test_table")
        
        # Verify the mock was called
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        mock_cursor.fetch_dataframe.assert_called_once()
        mock_df.to_dict.assert_called_once_with('records')
        assert result == mock_rows

class TestRSExtractor:
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.source_attr = {
            'connection_method': 'iam',
            'aws_access_key_id': 'test_access_key',
            'aws_secret_access_key': 'test_secret_key',
            'aws_profile_name': None,  # Don't use profile to avoid AWS config issues
            'aws_region': 'us-west-2',
            'database_name': 'test_db',
            'aws_bucket_name': 'test-bucket',
            'bucket_name': 'test-bucket',  # Add bucket_name (required by RSExtractor)
            'aws_iam_role': 'arn:aws:iam::123456789012:role/test-role'
        }
        pipeline.m_conf = MagicMock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        return pipeline
    
    @patch('boto3.session.Session')
    @patch('petaly.utils.file_handler.FileHandler.load_file')
    def test_extractor_initialization_iam(self, mock_load_file, mock_session, pipeline_mock):
        """Test Redshift extractor initialization with IAM auth."""
        # Setup mocks
        mock_aws_session = MagicMock()
        mock_session.return_value = mock_aws_session
        mock_load_file.return_value = "SELECT * FROM mock_table"
        
        # Create extractor and test
        extractor = RSExtractor(pipeline_mock)
        assert isinstance(extractor.db_connector, RSConnectorIAM)
        assert isinstance(extractor.s3_connector, S3Connector)
        assert extractor.cloud_bucket_name == 'test-bucket'
        assert extractor.aws_iam_role == 'arn:aws:iam::123456789012:role/test-role'
        mock_load_file.assert_called_once_with("mock_sql_path")
    
    @patch('boto3.session.Session')
    @patch('petaly.utils.file_handler.FileHandler.load_file')
    def test_compose_extract_options(self, mock_load_file, mock_session, pipeline_mock):
        """Test composing extract options."""
        # Setup mocks
        mock_aws_session = MagicMock()
        mock_session.return_value = mock_aws_session
        mock_load_file.return_value = "SELECT * FROM mock_table"
        
        # Create extractor and test
        extractor = RSExtractor(pipeline_mock)
        extractor_obj_conf = {
            'object_settings': {
                'header': True,
                'columns_delimiter': ','
            }
        }
        
        # Test with comma delimiter
        options = extractor.compose_extract_options(extractor_obj_conf)
        assert "HEADER" in options
        assert "DELIMITER ','" in options
        
        # Test with tab delimiter
        extractor_obj_conf['object_settings']['columns_delimiter'] = '\t'
        options = extractor.compose_extract_options(extractor_obj_conf)
        assert "DELIMITER '\\t'" in options

class TestRSLoader:
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.target_attr = {
            'connection_method': 'iam',
            'aws_access_key_id': 'test_access_key',
            'aws_secret_access_key': 'test_secret_key',
            'aws_profile_name': None,  # Don't use profile to avoid AWS config issues
            'aws_region': 'us-west-2',
            'database_name': 'test_db',
            'aws_bucket_name': 'test-bucket',
            'bucket_name': 'test-bucket',  # Add bucket_name (required by RSLoader)
            'aws_iam_role': 'arn:aws:iam::123456789012:role/test-role'
        }
        pipeline.source_attr = pipeline.target_attr.copy()
        pipeline.m_conf = MagicMock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        return pipeline
    
    @patch('boto3.session.Session')
    def test_loader_initialization_iam(self, mock_session, pipeline_mock):
        """Test Redshift loader initialization with IAM auth."""
        # Setup mock
        mock_aws_session = MagicMock()
        mock_session.return_value = mock_aws_session
        
        # Create loader and test
        loader = RSLoader(pipeline_mock)
        assert isinstance(loader.db_connector, RSConnectorIAM)
        assert isinstance(loader.s3_connector, S3Connector)
        assert loader.cloud_bucket_name == 'test-bucket'
        assert loader.aws_iam_role == 'arn:aws:iam::123456789012:role/test-role'
    
    @patch('boto3.session.Session')
    def test_compose_from_options(self, mock_session, pipeline_mock):
        """Test composing load options."""
        # Setup mock
        mock_aws_session = MagicMock()
        mock_session.return_value = mock_aws_session
        
        # Create loader and test
        loader = RSLoader(pipeline_mock)
        loader_obj_conf = {
            'output_data_object_dir': '/tmp/test_output',  # Add output_data_object_dir (required by compose_from_options)
            'object_settings': {
                'header': True,
                'columns_delimiter': ',',
                'columns_quote': 'double'
            }
        }
        
        # Test with comma delimiter and quotes
        options = loader.compose_from_options(loader_obj_conf)
        assert "FORMAT AS CSV" in options
        assert "DELIMITER ','" in options
        assert "GZIP" in options
        assert "IGNOREHEADER 1" in options
        assert "REMOVEQUOTES" not in options
        
        # Test with tab delimiter and no quotes
        loader_obj_conf['object_settings'].update({
            'columns_delimiter': '\t',
            'columns_quote': None
        })
        options = loader.compose_from_options(loader_obj_conf)
        assert "DELIMITER '\\t'" in options
        assert "REMOVEQUOTES" in options 