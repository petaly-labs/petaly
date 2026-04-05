import pytest
import os
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import Mock, patch
from petaly.connectors.file.parquet.parquet_connector import ParquetConnector
from petaly.connectors.file.parquet.parquet_extractor import ParquetExtractor
from petaly.connectors.file.parquet.parquet_loader import ParquetLoader

class TestParquetConnector:
    def test_connector_initialization(self):
        """Test Parquet connector initialization."""
        connector = ParquetConnector()
        assert connector.connector_id == "parquet"
        assert connector.metaquery_quote == ''

class TestParquetExtractor:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def sample_parquet(self, temp_dir):
        """Create a sample Parquet file for testing."""
        parquet_path = os.path.join(temp_dir, 'test.parquet')
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path)
        return parquet_path
    
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.m_conf = Mock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        pipeline.m_conf.get_connector_category = Mock(return_value='file')
        pipeline.source_connector_id = "parquet"
        pipeline.m_conf.set_extractor_paths = Mock(return_value=None)
        pipeline.source_attr = {
            'connector_type': 'parquet',
        }
        pipeline.load_attributes = {
            'use_data_objects_spec': 'strict',
            'csv_default_settings': {
                'header': True,
                'columns_delimiter': ','
            }
        }
        pipeline.csv_default_settings = {
            'header': True,
            'columns_delimiter': ','
        }
        pipeline.data_objects = ['test_object']
        pipeline.data_objects_spec = [{
            'object_spec': {
                'object_name': 'test_object',
                'object_source_dir': 'test_source_dir',
                'file_names': ['test.parquet'],
            }
        }]
        # Set up output paths
        output_dir = os.path.join(tempfile.gettempdir(), 'test_output')
        os.makedirs(output_dir, exist_ok=True)
        pipeline.output_pipeline_dpath = output_dir
        pipeline.output_object_data_dpath = os.path.join(output_dir, '{object_name}')
        pipeline.output_object_metadata_dpath = os.path.join(output_dir, '{object_name}', 'metadata')
        return pipeline
    
    def test_extractor_initialization(self, pipeline_mock):
        """Test Parquet extractor initialization."""
        extractor = ParquetExtractor(pipeline_mock)
        assert extractor.file_format == 'parquet'
        assert isinstance(extractor, ParquetExtractor)
    
    @patch('petaly.utils.file_handler.FileHandler.is_file')
    @patch('petaly.utils.file_handler.FileHandler.cp_file')
    @patch('petaly.core.f_extractor.FExtractor.get_data_object')
    def test_extract_to_with_file_list(self, mock_get_data_object, mock_cp_file, mock_is_file, pipeline_mock, sample_parquet, temp_dir):
        """Test Parquet extraction with specific file list."""
        # Setup mocks
        mock_is_file.return_value = True
        mock_data_object = Mock()
        mock_data_object.exclude_columns = []
        mock_get_data_object.return_value = mock_data_object
        
        # Set up paths
        object_source_dir = temp_dir
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        extractor = ParquetExtractor(pipeline_mock)
        extractor.f_handler = Mock()
        extractor.f_handler.is_file = mock_is_file
        extractor.f_handler.cp_file = mock_cp_file
        
        extractor_obj_conf = {
            'object_name': 'test_object',
            'object_source_dir': object_source_dir,
            'file_names': ['test.parquet'],
            'output_data_object_dir': output_dir,
            'object_settings': {}
        }
        
        # Execute
        result = extractor.extract_to(extractor_obj_conf)
        
        # Verify
        assert len(result) == 1
        assert result[0].endswith('test.parquet')
        mock_is_file.assert_called()
    
    @patch('petaly.utils.file_handler.FileHandler.is_file')
    @patch('petaly.utils.file_handler.FileHandler.get_all_files_from_dir')
    @patch('petaly.core.f_extractor.FExtractor.get_data_object')
    def test_extract_to_without_file_list(self, mock_get_data_object, mock_get_all_files, mock_is_file, pipeline_mock, sample_parquet, temp_dir):
        """Test Parquet extraction without file list (collects all Parquet files)."""
        # Setup mocks
        mock_is_file.return_value = True
        mock_get_all_files.return_value = [sample_parquet]
        mock_data_object = Mock()
        mock_data_object.exclude_columns = []
        mock_get_data_object.return_value = mock_data_object
        
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        extractor = ParquetExtractor(pipeline_mock)
        extractor.f_handler = Mock()
        extractor.f_handler.is_file = mock_is_file
        extractor.f_handler.get_all_files_from_dir = mock_get_all_files
        extractor.f_handler.cp_file = Mock()
        
        extractor_obj_conf = {
            'object_name': 'test_object',
            'object_source_dir': temp_dir,
            'file_names': None,  # No file list - should collect all
            'output_data_object_dir': output_dir,
            'object_settings': {}
        }
        
        # Execute
        result = extractor.extract_to(extractor_obj_conf)
        
        # Verify
        assert len(result) >= 1
        mock_get_all_files.assert_called()
    
    @patch('petaly.core.f_extractor.FExtractor.get_data_object')
    def test_process_parquet_file_with_exclude_columns(self, mock_get_data_object, pipeline_mock, sample_parquet, temp_dir):
        """Test processing Parquet file with excluded columns."""
        # Setup
        mock_data_object = Mock()
        mock_data_object.exclude_columns = ['age']  # Exclude age column
        mock_get_data_object.return_value = mock_data_object
        
        output_path = os.path.join(temp_dir, 'output.parquet')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        extractor = ParquetExtractor(pipeline_mock)
        extractor.f_handler = Mock()
        extractor.f_handler.cp_file = Mock()
        
        # Execute
        extractor.process_parquet_file(sample_parquet, output_path, ['age'], {})
        
        # Verify - check that output file exists and doesn't contain 'age'
        assert os.path.exists(output_path)
        table = pq.read_table(output_path)
        df = table.to_pandas()
        assert 'age' not in df.columns
        assert 'id' in df.columns
        assert 'name' in df.columns
    
    @patch('petaly.core.f_extractor.FExtractor.get_data_object')
    def test_process_parquet_file_supports_parq_extension(self, mock_get_data_object, pipeline_mock, temp_dir):
        """Test that Parquet extractor supports both .parquet and .parq extensions."""
        # Create a file with .parq extension
        parq_path = os.path.join(temp_dir, 'test.parq')
        df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parq_path)
        
        mock_data_object = Mock()
        mock_data_object.exclude_columns = []
        mock_get_data_object.return_value = mock_data_object
        
        output_path = os.path.join(temp_dir, 'output.parq')
        
        extractor = ParquetExtractor(pipeline_mock)
        extractor.f_handler = Mock()
        extractor.f_handler.cp_file = Mock()
        
        # Execute
        extractor.process_parquet_file(parq_path, output_path, [], {})
        
        # Verify
        assert os.path.exists(output_path)

class TestParquetLoader:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.m_conf = Mock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        pipeline.m_conf.get_connector_category = Mock(return_value='file')
        pipeline.target_connector_id = "parquet"
        pipeline.m_conf.set_loader_paths = Mock(return_value=None)
        pipeline.target_attr = {
            'connector_type': 'parquet',
            'destination_dir': '/tmp/test_dest'
        }
        pipeline.pipeline_name = 'test_pipeline'
        pipeline.load_attributes = {
            'use_data_objects_spec': 'strict',
            'csv_default_settings': {
                'header': True,
                'columns_delimiter': ','
            }
        }
        pipeline.csv_default_settings = {
            'header': True,
            'columns_delimiter': ','
        }
        pipeline.data_objects = ['test_object']
        return pipeline
    
    def test_loader_initialization(self, pipeline_mock):
        """Test Parquet loader initialization."""
        loader = ParquetLoader(pipeline_mock)
        assert loader.file_format == 'parquet'
        assert isinstance(loader, ParquetLoader)
    
    @patch('petaly.utils.file_handler.FileHandler.check_dir')
    @patch('petaly.utils.file_handler.FileHandler.cp_file')
    @patch('petaly.core.f_loader.FLoader.get_data_object')
    def test_load_from_success(self, mock_get_data_object, mock_cp_file, mock_check_dir, pipeline_mock, temp_dir):
        """Test loading Parquet files to destination."""
        # Setup mocks
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create a test Parquet file
        test_parquet = os.path.join(output_dir, 'test.parquet')
        df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, test_parquet)
        
        mock_check_dir.return_value = (True, 1)  # Directory exists and has 1 file
        mock_data_object = Mock()
        mock_data_object.destination_object_name = None
        mock_get_data_object.return_value = mock_data_object
        
        loader = ParquetLoader(pipeline_mock)
        loader.f_handler = Mock()
        loader.f_handler.check_dir = mock_check_dir
        loader.f_handler.cp_file = mock_cp_file
        
        loader_obj_conf = {
            'object_name': 'test_object',
            'output_data_object_dir': output_dir,
            'file_list': [test_parquet]
        }
        
        # Execute
        loader.load_from(loader_obj_conf)
        
        # Verify
        mock_check_dir.assert_called_once()
        mock_cp_file.assert_called_once()
    
    @patch('petaly.utils.file_handler.FileHandler.check_dir')
    @patch('petaly.core.f_loader.FLoader.get_data_object')
    def test_load_from_filters_csv_files(self, mock_get_data_object, mock_check_dir, pipeline_mock, temp_dir):
        """Test that CSV files are filtered out and only Parquet files are copied."""
        # Setup
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create both Parquet and CSV files
        test_parquet = os.path.join(output_dir, 'test.parquet')
        test_csv = os.path.join(output_dir, 'test.csv')
        df = pd.DataFrame({'id': [1, 2]})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, test_parquet)
        with open(test_csv, 'w') as f:
            f.write('id\n1\n2')
        
        mock_check_dir.return_value = (True, 2)
        mock_data_object = Mock()
        mock_data_object.destination_object_name = None
        mock_get_data_object.return_value = mock_data_object
        
        loader = ParquetLoader(pipeline_mock)
        loader.f_handler = Mock()
        loader.f_handler.check_dir = mock_check_dir
        loader.f_handler.cp_file = Mock()
        
        loader_obj_conf = {
            'object_name': 'test_object',
            'output_data_object_dir': output_dir,
            'file_list': [test_parquet, test_csv]  # Both Parquet and CSV
        }
        
        # Execute
        loader.load_from(loader_obj_conf)
        
        # Verify - only Parquet file should be copied
        assert loader.f_handler.cp_file.call_count == 1
        # Verify the copied file is Parquet
        call_args = loader.f_handler.cp_file.call_args[0]
        assert call_args[0].endswith('.parquet') or call_args[0].endswith('.parq')
    
    @patch('petaly.utils.file_handler.FileHandler.check_dir')
    @patch('petaly.core.f_loader.FLoader.get_data_object')
    def test_load_from_supports_parq_extension(self, mock_get_data_object, mock_check_dir, pipeline_mock, temp_dir):
        """Test that Parquet loader supports both .parquet and .parq extensions."""
        # Setup
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create a file with .parq extension
        test_parq = os.path.join(output_dir, 'test.parq')
        df = pd.DataFrame({'id': [1, 2]})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, test_parq)
        
        mock_check_dir.return_value = (True, 1)
        mock_data_object = Mock()
        mock_data_object.destination_object_name = None
        mock_get_data_object.return_value = mock_data_object
        
        loader = ParquetLoader(pipeline_mock)
        loader.f_handler = Mock()
        loader.f_handler.check_dir = mock_check_dir
        loader.f_handler.cp_file = Mock()
        
        loader_obj_conf = {
            'object_name': 'test_object',
            'output_data_object_dir': output_dir,
            'file_list': [test_parq]
        }
        
        # Execute
        loader.load_from(loader_obj_conf)
        
        # Verify - .parq file should be copied
        loader.f_handler.cp_file.assert_called_once()

