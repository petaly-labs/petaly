"""
Unit tests for DataObject class.

Tests object settings formatting, csv_default_settings handling,
and object configuration merging.
"""
import pytest
import sys
from unittest.mock import Mock, MagicMock
from petaly.core.data_object import DataObject


class TestDataObject:
    """Test suite for DataObject class."""
    
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = MagicMock()
        pipeline.output_object_data_dpath = '/output/{object_name}'
        pipeline.use_data_objects_spec = 'strict'
        pipeline.csv_default_settings = {
            'header': True,
            'columns_delimiter': ',',
            'columns_quote': 'double'
        }
        pipeline.data_objects_spec = [
            {
                'object_spec': {
                    'object_name': 'table1',
                    'destination_object_name': 'table1_dest',
                    'exclude_columns': ['col1', 'col2'],
                    'object_source_dir': '/source/table1',
                    'file_names': ['file1.csv'],
                    'cleanup_linebreak_in_fields': True
                }
            }
        ]
        pipeline.source_connector_id = 'postgres'
        pipeline.pipeline_name = 'test_pipeline'
        pipeline.pipeline_fpath = '/path/to/pipeline.yaml'
        return pipeline
    
    def test_initialization_with_spec(self, pipeline_mock):
        """Test DataObject initialization with object spec."""
        data_object = DataObject(pipeline_mock, 'table1')
        
        assert data_object.object_name == 'table1'
        assert data_object.destination_object_name == 'table1_dest'
        assert data_object.exclude_columns == ['col1', 'col2']
        assert data_object.object_source_dir == '/source/table1'
        assert data_object.file_names == ['file1.csv']
        assert data_object.object_settings['cleanup_linebreak_in_fields'] is True
    
    def test_initialization_without_spec_all_mode(self, pipeline_mock):
        """Test DataObject initialization without spec in 'all' mode."""
        pipeline_mock.use_data_objects_spec = 'prefer'
        pipeline_mock.data_objects_spec = []
        
        data_object = DataObject(pipeline_mock, 'table1')
        
        assert data_object.object_name == 'table1'
        assert data_object.destination_object_name is None
        assert data_object.exclude_columns == [None]
        assert data_object.object_source_dir is None
    
    def test_initialization_without_spec_spec_mode(self, pipeline_mock):
        """Test DataObject initialization without spec in 'spec' mode should exit."""
        pipeline_mock.use_data_objects_spec = 'strict'
        pipeline_mock.data_objects_spec = []
        
        with pytest.raises(SystemExit):
            DataObject(pipeline_mock, 'table1')
    
    def test_initialization_file_connector_spec_required(self, pipeline_mock):
        """Test that file connectors require spec even in 'all' mode."""
        pipeline_mock.use_data_objects_spec = 'prefer'
        pipeline_mock.data_objects_spec = []
        pipeline_mock.source_connector_id = 'csv'
        
        with pytest.raises(SystemExit):
            DataObject(pipeline_mock, 'table1')
    
    def test_format_csv_default_settings(self, pipeline_mock):
        """Test format_csv_default_settings method."""
        csv_settings = {
            'header': True,
            'columns_delimiter': ',',
            'columns_quote': 'double'
        }
        
        data_object = DataObject(pipeline_mock, 'table1')
        formatted = data_object.format_csv_default_settings(csv_settings)
        
        assert formatted['header'] is True
        assert formatted['columns_delimiter'] == ','
        assert formatted['columns_quote'] == 'double'
    
    def test_format_csv_default_settings_tab_delimiter(self, pipeline_mock):
        """Test format_csv_default_settings with tab delimiter."""
        csv_settings = {
            'header': True,
            'columns_delimiter': '\\t',
            'columns_quote': 'double'
        }
        
        data_object = DataObject(pipeline_mock, 'table1')
        formatted = data_object.format_csv_default_settings(csv_settings)
        
        assert formatted['columns_delimiter'] == '\t'  # Should convert \\t to \t
    
    def test_format_csv_default_settings_header_false(self, pipeline_mock):
        """Test format_csv_default_settings with header=False."""
        csv_settings = {
            'header': False,
            'columns_delimiter': ','
        }
        
        data_object = DataObject(pipeline_mock, 'table1')
        formatted = data_object.format_csv_default_settings(csv_settings)
        
        assert formatted['header'] is False
    
    def test_object_settings_inheritance(self, pipeline_mock):
        """Test that object_settings inherit from csv_default_settings."""
        data_object = DataObject(pipeline_mock, 'table1')
        
        assert data_object.object_settings['header'] is True
        assert data_object.object_settings['columns_delimiter'] == ','
        assert data_object.object_settings['columns_quote'] == 'double'
    
    def test_get_object_spec_found(self, pipeline_mock):
        """Test get_object_spec when object is found."""
        data_object = DataObject(pipeline_mock, 'table1')
        spec = data_object.get_object_spec(pipeline_mock.data_objects_spec, 'table1')
        
        assert spec is not None
        assert spec['object_name'] == 'table1'
    
    def test_get_object_spec_not_found(self, pipeline_mock):
        """Test get_object_spec when object is not found."""
        data_object = DataObject(pipeline_mock, 'table1')
        spec = data_object.get_object_spec(pipeline_mock.data_objects_spec, 'nonexistent')
        
        assert spec == {}
    
    def test_to_dict(self, pipeline_mock):
        """Test to_dict method."""
        data_object = DataObject(pipeline_mock, 'table1')
        obj_dict = data_object.to_dict()
        
        assert isinstance(obj_dict, dict)
        assert 'object_name' in obj_dict
        assert 'destination_object_name' in obj_dict
        assert 'object_settings' in obj_dict
    
    def test_recreate_destination_object_true(self, pipeline_mock):
        """Test recreate_destination_object=True."""
        pipeline_mock.data_objects_spec[0]['object_spec']['recreate_destination_object'] = True
        data_object = DataObject(pipeline_mock, 'table1')
        
        assert data_object.recreate_destination_object is True
    
    def test_recreate_destination_object_false(self, pipeline_mock):
        """Test recreate_destination_object=False."""
        pipeline_mock.data_objects_spec[0]['object_spec']['recreate_destination_object'] = False
        data_object = DataObject(pipeline_mock, 'table1')
        
        assert data_object.recreate_destination_object is False

