import pytest
import os
import tempfile
import pandas as pd
import json
from unittest.mock import Mock, patch
from petaly.connectors.file.json.json_connector import JsonConnector
from petaly.connectors.file.json.json_extractor import JsonExtractor
from petaly.connectors.file.json.json_loader import JsonLoader

class TestJsonConnector:
    def test_connector_initialization(self):
        """Test JSON connector initialization."""
        connector = JsonConnector()
        assert connector.connector_id == "json"
        assert connector.metaquery_quote == ''

class TestJsonExtractor:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def sample_json(self, temp_dir):
        """Create a sample JSON file for testing."""
        json_path = os.path.join(temp_dir, 'test.json')
        data = [
            {'id': 1, 'name': 'Alice', 'age': 25},
            {'id': 2, 'name': 'Bob', 'age': 30},
            {'id': 3, 'name': 'Charlie', 'age': 35}
        ]
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
        return json_path
    
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = Mock()
        pipeline.m_conf = Mock()
        pipeline.m_conf.connector_metadata_sql_fpath = "mock_sql_path"
        pipeline.m_conf.get_connector_category = Mock(return_value='file')
        pipeline.source_connector_id = "json"
        pipeline.m_conf.set_extractor_paths = Mock(return_value=None)
        pipeline.source_attr = {
            'connector_type': 'json',
        }
        pipeline.data_attributes = {
            'include_data_objects': 'spec',
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
                'file_names': ['test.json'],
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
        """Test JSON extractor initialization."""
        extractor = JsonExtractor(pipeline_mock)
        assert extractor.file_format == 'json'
        assert isinstance(extractor, JsonExtractor)
    
    @patch('petaly.utils.file_handler.FileHandler.is_file')
    @patch('petaly.utils.file_handler.FileHandler.cp_file')
    @patch('petaly.core.f_extractor.FExtractor.get_data_object')
    def test_extract_to_with_file_list(self, mock_get_data_object, mock_cp_file, mock_is_file, pipeline_mock, sample_json, temp_dir):
        """Test JSON extraction with specific file list."""
        # Setup mocks
        mock_is_file.return_value = True
        mock_data_object = Mock()
        mock_data_object.exclude_columns = []
        mock_get_data_object.return_value = mock_data_object
        
        # Set up paths
        object_source_dir = temp_dir
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        extractor = JsonExtractor(pipeline_mock)
        extractor.f_handler = Mock()
        extractor.f_handler.is_file = mock_is_file
        extractor.f_handler.cp_file = mock_cp_file
        
        extractor_obj_conf = {
            'object_name': 'test_object',
            'object_source_dir': object_source_dir,
            'file_names': ['test.json'],
            'output_data_object_dir': output_dir,
            'object_settings': {}
        }
        
        # Execute
        result = extractor.extract_to(extractor_obj_conf)
        
        # Verify
        assert len(result) == 1
        assert result[0].endswith('test.json')
        mock_is_file.assert_called()
    
    @patch('petaly.utils.file_handler.FileHandler.is_file')
    @patch('petaly.utils.file_handler.FileHandler.get_all_files_from_dir')
    @patch('petaly.core.f_extractor.FExtractor.get_data_object')
    def test_extract_to_without_file_list(self, mock_get_data_object, mock_get_all_files, mock_is_file, pipeline_mock, sample_json, temp_dir):
        """Test JSON extraction without file list (collects all JSON files)."""
        # Setup mocks
        mock_is_file.return_value = True
        mock_get_all_files.return_value = [sample_json]
        mock_data_object = Mock()
        mock_data_object.exclude_columns = []
        mock_get_data_object.return_value = mock_data_object
        
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        extractor = JsonExtractor(pipeline_mock)
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
    def test_process_json_file_with_exclude_columns(self, mock_get_data_object, pipeline_mock, sample_json, temp_dir):
        """Test processing JSON file with excluded columns."""
        # Setup
        mock_data_object = Mock()
        mock_data_object.exclude_columns = ['age']  # Exclude age column
        mock_get_data_object.return_value = mock_data_object
        
        output_path = os.path.join(temp_dir, 'output.json')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        extractor = JsonExtractor(pipeline_mock)
        extractor.f_handler = Mock()
        extractor.f_handler.cp_file = Mock()
        
        # Execute
        extractor.process_json_file(sample_json, output_path, ['age'], {})
        
        # Verify - check that output file exists and doesn't contain 'age'
        assert os.path.exists(output_path)
        with open(output_path, 'r') as f:
            output_data = json.load(f)
            if isinstance(output_data, list) and len(output_data) > 0:
                assert 'age' not in output_data[0]
                assert 'id' in output_data[0]
                assert 'name' in output_data[0]

class TestJsonLoader:
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
        pipeline.target_connector_id = "json"
        pipeline.m_conf.set_loader_paths = Mock(return_value=None)
        pipeline.target_attr = {
            'connector_type': 'json',
            'destination_dir': '/tmp/test_dest'
        }
        pipeline.pipeline_name = 'test_pipeline'
        pipeline.data_attributes = {
            'include_data_objects': 'spec',
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
        """Test JSON loader initialization."""
        loader = JsonLoader(pipeline_mock)
        assert loader.file_format == 'json'
        assert isinstance(loader, JsonLoader)
    
    @patch('petaly.utils.file_handler.FileHandler.check_dir')
    @patch('petaly.utils.file_handler.FileHandler.cp_file')
    @patch('petaly.core.f_loader.FLoader.get_data_object')
    def test_load_from_success(self, mock_get_data_object, mock_cp_file, mock_check_dir, pipeline_mock, temp_dir):
        """Test loading JSON files to destination."""
        # Setup mocks
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create a test JSON file
        test_json = os.path.join(output_dir, 'test.json')
        with open(test_json, 'w') as f:
            json.dump([{'id': 1, 'name': 'test'}], f)
        
        mock_check_dir.return_value = (True, 1)  # Directory exists and has 1 file
        mock_data_object = Mock()
        mock_data_object.destination_object_name = None
        mock_get_data_object.return_value = mock_data_object
        
        loader = JsonLoader(pipeline_mock)
        loader.f_handler = Mock()
        loader.f_handler.check_dir = mock_check_dir
        loader.f_handler.cp_file = mock_cp_file
        
        loader_obj_conf = {
            'object_name': 'test_object',
            'output_data_object_dir': output_dir,
            'file_list': [test_json]
        }
        
        # Execute
        loader.load_from(loader_obj_conf)
        
        # Verify
        mock_check_dir.assert_called_once()
        mock_cp_file.assert_called_once()
    
    @patch('petaly.utils.file_handler.FileHandler.check_dir')
    @patch('petaly.core.f_loader.FLoader.get_data_object')
    def test_load_from_filters_csv_files(self, mock_get_data_object, mock_check_dir, pipeline_mock, temp_dir):
        """Test that CSV files are filtered out and only JSON files are copied."""
        # Setup
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create both JSON and CSV files
        test_json = os.path.join(output_dir, 'test.json')
        test_csv = os.path.join(output_dir, 'test.csv')
        with open(test_json, 'w') as f:
            json.dump([{'id': 1}], f)
        with open(test_csv, 'w') as f:
            f.write('id,name\n1,test')
        
        mock_check_dir.return_value = (True, 2)
        mock_data_object = Mock()
        mock_data_object.destination_object_name = None
        mock_get_data_object.return_value = mock_data_object
        
        loader = JsonLoader(pipeline_mock)
        loader.f_handler = Mock()
        loader.f_handler.check_dir = mock_check_dir
        loader.f_handler.cp_file = Mock()
        
        loader_obj_conf = {
            'object_name': 'test_object',
            'output_data_object_dir': output_dir,
            'file_list': [test_json, test_csv]  # Both JSON and CSV
        }
        
        # Execute
        loader.load_from(loader_obj_conf)
        
        # Verify - only JSON file should be copied
        assert loader.f_handler.cp_file.call_count == 1
        # Verify the copied file is JSON
        call_args = loader.f_handler.cp_file.call_args[0]
        assert call_args[0].endswith('.json')

