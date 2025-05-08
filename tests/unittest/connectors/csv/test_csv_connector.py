import pytest
import os
import tempfile
import pandas as pd
from unittest.mock import Mock, patch
from petaly.connectors.csv.csv_connector import CsvConnector
from petaly.connectors.csv.csv_extractor import CsvExtractor
from petaly.connectors.csv.csv_loader import CsvLoader

class TestCsvConnector:
    def test_connector_initialization(self):
        """Test CSV connector initialization."""
        connector = CsvConnector()
        assert connector.connector_id == "csv"
        assert connector.metaquery_quote == ''

class TestCsvExtractor:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def sample_csv(self, temp_dir):
        """Create a sample CSV file for testing."""
        csv_path = os.path.join(temp_dir, 'test.csv')
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        df.to_csv(csv_path, index=False)
        return csv_path
    
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.m_conf = Mock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        pipeline.m_conf.get_connector_category = Mock(return_value='file')
        pipeline.source_connector_id = "csv"
        pipeline.m_conf.set_extractor_paths = Mock(return_value=None)
        pipeline.source = {
            'path': 'test.csv',
            'format': 'csv'
        }
        pipeline.source_attr = {
            'connector_type': 'csv',
            'path': 'test.csv'
        }
        pipeline.data_attributes = {
            'data_objects_spec_mode': 'only',
            'object_default_settings': {
                'header': True,
                'columns_delimiter': ','
            }
        }
        pipeline.object_default_settings = {
            'header': True,
            'columns_delimiter': ','
        }
        pipeline.data_objects = ['test_object']
        pipeline.data_objects_spec = [{
            'object_spec': {
                'object_name': 'test_object',
                'object_source_dir': 'test_source_dir',
                'file_names': ['test.csv'],
                'source': {'path': 'test.csv'},
                'destination': {'path': 'test_output.csv'}
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
        """Test CSV extractor initialization."""
        extractor = CsvExtractor(pipeline_mock)
        assert extractor.file_format == 'csv'
        assert isinstance(extractor, CsvExtractor)
    
    @patch('pandas.read_csv')
    @patch('petaly.utils.file_handler.FileHandler.is_file')
    @patch('petaly.utils.file_handler.FileHandler.cp_file')
    @patch('petaly.utils.file_handler.FileHandler.cleanup_dir')
    def test_extract_data(self, mock_cleanup, mock_cp_file, mock_is_file, mock_read_csv, pipeline_mock, sample_csv):
        """Test data extraction from CSV."""
        # Setup mock
        mock_is_file.return_value = True
        mock_cp_file.return_value = None
        mock_cleanup.return_value = None
        pipeline_mock.source['path'] = sample_csv
        pipeline_mock.source_attr['path'] = sample_csv
        
        # Create test directory structure
        output_dir = pipeline_mock.output_object_data_dpath.format(object_name='test_object')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create extractor and test
        extractor = CsvExtractor(pipeline_mock)
        extractor.extract_data()
        
        # Verify the mocks were called
        mock_is_file.assert_called_once()
        mock_cp_file.assert_called_once()
        assert mock_cleanup.call_count == 2  # Called in both extract_data() and get_extractor_obj_conf()

class TestCsvLoader:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
    
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.m_conf = Mock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        pipeline.m_conf.get_connector_category = Mock(return_value='file')
        pipeline.target_connector_id = "csv"
        pipeline.m_conf.set_loader_paths = Mock(return_value=None)
        pipeline.destination = {
            'path': 'test_output.csv',
            'format': 'csv'
        }
        pipeline.target_attr = {
            'connector_type': 'csv',
            'bucket_pipeline_prefix': '',
            'path': 'test_output.csv',
            'destination_dir': 'test_dest_dir'
        }
        pipeline.pipeline_name = 'test_pipeline'
        pipeline.data_attributes = {
            'data_objects_spec_mode': 'only',
            'object_default_settings': {
                'header': True,
                'columns_delimiter': ','
            }
        }
        pipeline.object_default_settings = {
            'header': True,
            'columns_delimiter': ','
        }
        pipeline.data_objects = ['test_object']
        pipeline.data_objects_spec = [{
            'object_spec': {
                'object_name': 'test_object',
                'object_source_dir': 'test_source_dir',
                'file_names': ['test.csv'],
                'source': {'path': 'test.csv'},
                'destination': {'path': 'test_output.csv'}
            }
        }]
        # Set up directory paths
        output_dir = os.path.join(tempfile.gettempdir(), 'test_output')
        os.makedirs(output_dir, exist_ok=True)
        pipeline.output_pipeline_dpath = output_dir
        pipeline.data_objects = ['test_object']
        pipeline.data_objects_spec = [{
            'object_spec': {
                'object_name': 'test_object',
                'object_source_dir': 'test_source_dir',
                'file_names': ['test.csv'],
                'source': {'path': 'test.csv'},
                'destination': {'path': 'test_output.csv'}
            }
        }]
        # Set up output paths
        output_object_dir = os.path.join(output_dir, 'test_object')
        os.makedirs(output_object_dir, exist_ok=True)
        pipeline.output_object_data_dpath = output_object_dir
        # Create a dummy file to satisfy the directory check
        with open(os.path.join(output_object_dir, 'dummy.csv'), 'w') as f:
            f.write('dummy')
        return pipeline
    
    def test_loader_initialization(self, pipeline_mock):
        """Test CSV loader initialization."""
        loader = CsvLoader(pipeline_mock)
        assert loader.file_format == 'csv'
        assert isinstance(loader, CsvLoader)
    
    @patch('petaly.utils.file_handler.FileHandler.get_specific_files')
    @patch('petaly.utils.file_handler.FileHandler.cp_file')
    @patch('petaly.utils.file_handler.FileHandler.check_dir')
    def test_load_data(self, mock_check_dir, mock_cp_file, mock_get_files, pipeline_mock, sample_data, temp_dir):
        """Test data loading to CSV."""
        # Setup mock
        mock_get_files.return_value = ['dummy.csv']
        mock_cp_file.return_value = None
        mock_check_dir.return_value = (True, 1)  # Directory exists and has 1 file
        
        # Set up paths
        output_file = os.path.join(pipeline_mock.output_object_data_dpath, 'test_output.csv')
        pipeline_mock.destination['path'] = output_file
        pipeline_mock.target_attr['path'] = output_file
        
        # Create test directory structure
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        # Create a dummy file to satisfy the directory check
        with open(os.path.join(os.path.dirname(output_file), 'dummy.csv'), 'w') as f:
            f.write('dummy')
        
        # Create loader and test
        loader = CsvLoader(pipeline_mock)
        loader.data = sample_data
        loader.load_data()
        
        # Verify the mocks were called
        mock_get_files.assert_called_once()
        mock_cp_file.assert_called_once()
        mock_check_dir.assert_called_once() 