import pytest
import os
import tempfile
import psycopg
from unittest.mock import Mock, patch, MagicMock
from petaly.connectors.postgres.psql_connector import PsqlConnector
from petaly.connectors.postgres.psql_extractor import PsqlExtractor
from petaly.connectors.postgres.psql_loader import PsqlLoader

class TestPsqlConnector:
    @pytest.fixture
    def endpoint_attr(self):
        """Create mock endpoint attributes."""
        return {
            'database_user': 'test_user',
            'database_password': 'test_password',
            'database_host': 'localhost',
            'database_port': 5432,
            'database_name': 'test_db'
        }
    
    @patch('psycopg.connect')
    def test_connector_initialization(self, mock_connect, endpoint_attr):
        """Test PostgreSQL connector initialization."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = PsqlConnector(endpoint_attr)
        assert connector.connector_id == "postgres"
        assert connector.conn == mock_conn
    
    @patch('psycopg.connect')
    def test_get_connection(self, mock_connect, endpoint_attr):
        """Test getting a database connection."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = PsqlConnector(endpoint_attr)
        
        # Verify the mock was called with correct parameters
        mock_connect.assert_called_once()
        assert connector.conn == mock_conn
    
    @patch('psycopg.connect')
    def test_get_query_result(self, mock_connect, endpoint_attr):
        """Test executing a query and getting results."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_rows = [(1, 'test')]
        mock_cursor.fetchall.return_value = mock_rows
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = PsqlConnector(endpoint_attr)
        result = connector.get_query_result("SELECT * FROM test_table")
        
        # Verify the mock was called
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        mock_cursor.fetchall.assert_called_once()
        assert result == mock_rows
    
    @patch('psycopg.connect')
    def test_extract_to(self, mock_connect, endpoint_attr):
        """Test extracting data to a file."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create test file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Create connector and test
            connector = PsqlConnector(endpoint_attr)
            connector.extract_to("COPY test_table TO STDOUT", temp_path)
            
            # Verify the mock was called
            mock_cursor.copy.assert_called_once()
        finally:
            # Cleanup
            os.unlink(temp_path)
    
    @patch('psycopg.connect')
    def test_load_from(self, mock_connect, endpoint_attr):
        """Test loading data from a file."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create test file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b"test,data\n1,2\n")
        
        try:
            # Create connector and test
            connector = PsqlConnector(endpoint_attr)
            connector.load_from("COPY test_table FROM STDIN", temp_path)
            
            # Verify the mock was called
            mock_cursor.copy.assert_called_once()
            mock_conn.commit.assert_called_once()
        finally:
            # Cleanup
            os.unlink(temp_path)
    
    @patch('psycopg.connect')
    def test_drop_table(self, mock_connect, endpoint_attr):
        """Test dropping a table."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = PsqlConnector(endpoint_attr)
        connector.drop_table("test_schema.test_table")
        
        # Verify the mock was called
        mock_conn.execute.assert_called_once_with("DROP TABLE IF EXISTS test_schema.test_table")
        mock_conn.commit.assert_called_once()
    
    @patch('psycopg.connect')
    def test_execute_sql(self, mock_connect, endpoint_attr):
        """Test executing SQL."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = PsqlConnector(endpoint_attr)
        connector.execute_sql("CREATE TABLE test_table (id INT)")
        
        # Verify the mock was called
        mock_conn.execute.assert_called_once_with("CREATE TABLE test_table (id INT)")
        mock_conn.commit.assert_called_once()

class TestPsqlExtractor:
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.source_attr = {
            'database_user': 'test_user',
            'database_password': 'test_password',
            'database_host': 'localhost',
            'database_port': 5432,
            'database_name': 'test_db'
        }
        # Mock configuration
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        return pipeline
    
    @patch('psycopg.connect')
    @patch('petaly.utils.file_handler.FileHandler.load_file')
    def test_extractor_initialization(self, mock_load_file, mock_connect, pipeline_mock):
        """Test PostgreSQL extractor initialization."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_load_file.return_value = "SELECT * FROM mock_table"
        
        extractor = PsqlExtractor(pipeline_mock)
        assert isinstance(extractor, PsqlExtractor)
        assert extractor.db_connector.conn == mock_conn
        mock_load_file.assert_called_once_with("mock_sql_path")
    
    @patch('psycopg.connect')
    @patch('petaly.utils.file_handler.FileHandler.load_file')
    def test_compose_extract_options(self, mock_load_file, mock_connect, pipeline_mock):
        """Test composing extract options."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_load_file.return_value = "SELECT * FROM mock_table"
        
        extractor = PsqlExtractor(pipeline_mock)
        extractor_obj_conf = {
            'object_settings': {
                'columns_delimiter': ',',
                'header': True,
                'columns_quote': 'double'
            }
        }
        
        options = extractor.compose_extract_options(extractor_obj_conf)
        assert ", DELIMITER ','" in options
        assert ", HEADER True" in options
        assert ", QUOTE '\"'" in options
        assert ", FORCE_QUOTE *" in options

class TestPsqlLoader:
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.target_attr = {
            'database_user': 'test_user',
            'database_password': 'test_password',
            'database_host': 'localhost',
            'database_port': 5432,
            'database_name': 'test_db'
        }
        return pipeline
    
    @patch('psycopg.connect')
    def test_loader_initialization(self, mock_connect, pipeline_mock):
        """Test PostgreSQL loader initialization."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = PsqlLoader(pipeline_mock)
        assert isinstance(loader, PsqlLoader)
        assert loader.db_connector.conn == mock_conn
    
    @patch('psycopg.connect')
    def test_compose_from_options(self, mock_connect, pipeline_mock):
        """Test composing load options."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = PsqlLoader(pipeline_mock)
        loader_obj_conf = {
            'object_settings': {
                'columns_delimiter': ',',
                'header': True,
                'columns_quote': 'double'
            }
        }
        
        options = loader.compose_from_options(loader_obj_conf)
        assert ", DELIMITER ','" in options
        assert ", HEADER True" in options
        assert ", QUOTE '\"'" in options 