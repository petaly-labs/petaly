import pytest
import os
import tempfile
import csv
from unittest.mock import Mock, patch, MagicMock, call
from petaly.connectors.mysql.mysql_connector import MysqlConnector
from petaly.connectors.mysql.mysql_extractor import MysqlExtractor
from petaly.connectors.mysql.mysql_loader import MysqlLoader

class TestMysqlConnector:
    @pytest.fixture
    def endpoint_attr(self):
        """Create mock endpoint attributes."""
        return {
            'database_user': 'test_user',
            'database_password': 'test_password',
            'database_host': 'localhost',
            'database_port': 3306,
            'database_name': 'test_db'
        }
    
    @patch('mysql.connector.connect')
    def test_connector_initialization(self, mock_connect, endpoint_attr):
        """Test MySQL connector initialization."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = MysqlConnector(endpoint_attr)
        assert connector.connector_id == "mysql"
        assert connector.conn == mock_conn
        assert connector.database == 'test_db'
    
    @patch('mysql.connector.connect')
    def test_get_connection(self, mock_connect, endpoint_attr):
        """Test getting a database connection."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = MysqlConnector(endpoint_attr)
        
        # Verify the mock was called with correct parameters
        mock_connect.assert_called_once_with(
            user='test_user',
            password='test_password',
            host='localhost',
            port=3306,
            database='test_db',
            allow_local_infile=True,
            use_pure=False
        )
        assert connector.conn == mock_conn
    
    @patch('mysql.connector.connect')
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
        connector = MysqlConnector(endpoint_attr)
        result = connector.get_query_result("SELECT * FROM test_table")
        
        # Verify the mock was called
        mock_cursor.execute.assert_called_with("SELECT * FROM test_table")
        mock_cursor.fetchall.assert_called_once()
        assert result == mock_rows
    
    @patch('mysql.connector.connect')
    def test_extract_to(self, mock_connect, endpoint_attr):
        """Test extracting data to a file."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_row = {'id': 1, 'name': 'test'}
        mock_cursor.fetchone.side_effect = [mock_row, None]  # Return row once, then None to end loop
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create test file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Create connector and test
            connector = MysqlConnector(endpoint_attr)
            extract_options = {
                "delimiter": ",",
                "quotechar": '"',
                "escapechar": "\\",
                "quoting": csv.QUOTE_ALL,
                "lineterminator": "\n",
                "header": True,
                "cleanup_linebreak_in_fields": False
            }
            connector.extract_to("SELECT * FROM test_table", temp_path, extract_options)
            
            # Verify the mock was called
            mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
            assert mock_cursor.fetchone.call_count == 2  # Once for header, once for data
            
            # Verify file was created
            assert os.path.exists(temp_path)
        finally:
            # Cleanup
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    @patch('mysql.connector.connect')
    def test_load_from(self, mock_connect, endpoint_attr):
        """Test loading data from a file."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = MysqlConnector(endpoint_attr)
        connector.load_from("LOAD DATA INFILE 'test.csv' INTO TABLE test_table")
        
        # Verify the mock was called
        assert mock_cursor.execute.call_count == 2
        mock_cursor.execute.assert_has_calls([
            call("USE test_db;"),
            call("LOAD DATA INFILE 'test.csv' INTO TABLE test_table")
        ], any_order=False)
        mock_conn.commit.assert_called_once()
    
    @patch('mysql.connector.connect')
    def test_drop_table(self, mock_connect, endpoint_attr):
        """Test dropping a table."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = MysqlConnector(endpoint_attr)
        connector.drop_table("test_table")
        
        # Verify the mock was called
        assert mock_cursor.execute.call_count == 2
        mock_cursor.execute.assert_has_calls([
            call("USE test_db;"),
            call("DROP TABLE IF EXISTS test_table")
        ], any_order=False)
        mock_conn.commit.assert_called_once()
    
    @patch('mysql.connector.connect')
    def test_execute_sql(self, mock_connect, endpoint_attr):
        """Test executing SQL."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create connector and test
        connector = MysqlConnector(endpoint_attr)
        connector.execute_sql("CREATE TABLE test_table (id INT)")
        
        # Verify the mock was called
        assert mock_cursor.execute.call_count == 2
        mock_cursor.execute.assert_has_calls([
            call("USE test_db;"),
            call("CREATE TABLE test_table (id INT)")
        ], any_order=False)
        mock_conn.commit.assert_called_once()

class TestMysqlExtractor:
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.source_attr = {
            'database_user': 'test_user',
            'database_password': 'test_password',
            'database_host': 'localhost',
            'database_port': 3306,
            'database_name': 'test_db'
        }
        # Mock configuration
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        return pipeline
    
    @patch('mysql.connector.connect')
    @patch('petaly.utils.file_handler.FileHandler.load_file')
    def test_extractor_initialization(self, mock_load_file, mock_connect, pipeline_mock):
        """Test MySQL extractor initialization."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_load_file.return_value = "SELECT * FROM mock_table"
        
        extractor = MysqlExtractor(pipeline_mock)
        assert isinstance(extractor, MysqlExtractor)
        assert extractor.db_connector.conn == mock_conn
        mock_load_file.assert_called_once_with("mock_sql_path")
    
    @patch('mysql.connector.connect')
    @patch('petaly.utils.file_handler.FileHandler.load_file')
    def test_compose_extract_options(self, mock_load_file, mock_connect, pipeline_mock):
        """Test composing extract options."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_load_file.return_value = "SELECT * FROM mock_table"
        
        extractor = MysqlExtractor(pipeline_mock)
        extractor_obj_conf = {
            'object_settings': {
                'columns_delimiter': ',',
                'header': True,
                'columns_quote': 'double'
            }
        }
        
        options = extractor.compose_extract_options(extractor_obj_conf)
        assert options['delimiter'] == ','
        assert options['quoting'] == csv.QUOTE_ALL
        assert options['quotechar'] == '"'
        assert options['header'] is True

class TestMysqlLoader:
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.target_attr = {
            'database_user': 'test_user',
            'database_password': 'test_password',
            'database_host': 'localhost',
            'database_port': 3306,
            'database_name': 'test_db'
        }
        return pipeline
    
    @patch('mysql.connector.connect')
    def test_loader_initialization(self, mock_connect, pipeline_mock):
        """Test MySQL loader initialization."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = MysqlLoader(pipeline_mock)
        assert isinstance(loader, MysqlLoader)
        assert loader.db_connector.conn == mock_conn
    
    @patch('mysql.connector.connect')
    def test_compose_load_options(self, mock_connect, pipeline_mock):
        """Test composing load options."""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        loader = MysqlLoader(pipeline_mock)
        loader_obj_conf = {
            'object_settings': {
                'columns_delimiter': ',',
                'header': True,
                'columns_quote': 'double'
            }
        }
        
        options = loader.compose_load_options(loader_obj_conf)
        assert "FIELDS TERMINATED BY ','" in options
        assert "ENCLOSED BY '\"'" in options
        assert "ESCAPED BY '\\\\'" in options
        assert "LINES TERMINATED BY '\\n'" in options
        assert "IGNORE 1 ROWS" in options 