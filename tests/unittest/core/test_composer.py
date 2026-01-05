"""
Unit tests for Composer class.

Tests object filtering logic and include_data_objects handling.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from petaly.core.composer import Composer


class TestComposer:
    """Test suite for Composer class."""
    
    @pytest.fixture
    def composer(self):
        """Create a Composer instance."""
        return Composer()
    
    @pytest.fixture
    def pipeline_mock(self):
        """Create a mock pipeline object."""
        pipeline = MagicMock()
        pipeline.output_pipeline_dpath = '/output'
        pipeline.include_data_objects = 'spec'
        pipeline.data_objects = ['table1', 'table2']
        return pipeline
    
    def test_initialization(self, composer):
        """Test Composer initialization."""
        assert composer.f_handler is not None
    
    def test_normalise_column_name(self, composer):
        """Test column name normalization."""
        assert composer.normalise_column_name('col:name') == 'col_name'
        assert composer.normalise_column_name('col.name') == 'col_name'
        assert composer.normalise_column_name('col:name.test') == 'col_name_test'
        assert composer.normalise_column_name('normal_col') == 'normal_col'
    
    def test_get_object_list_from_output_dir_with_spec(self, composer, pipeline_mock):
        """Test get_object_list_from_output_dir when data_objects_spec has objects."""
        with patch.object(composer.f_handler, 'get_all_dir_names', return_value=['table1', 'table2', 'table3']):
            result = composer.get_object_list_from_output_dir(pipeline_mock)
            
            # Should return intersection of output dirs and pipeline.data_objects
            assert set(result) == {'table1', 'table2'}
    
    def test_get_object_list_from_output_dir_all_mode(self, composer, pipeline_mock):
        """Test get_object_list_from_output_dir with include_data_objects='all'."""
        pipeline_mock.include_data_objects = 'all'
        pipeline_mock.data_objects = []
        
        with patch.object(composer.f_handler, 'get_all_dir_names', return_value=['table1', 'table2', 'table3']):
            result = composer.get_object_list_from_output_dir(pipeline_mock)
            
            # Should return all objects from output dir
            assert set(result) == {'table1', 'table2', 'table3'}
    
    def test_get_object_list_from_output_dir_spec_mode_empty(self, composer, pipeline_mock):
        """Test get_object_list_from_output_dir with include_data_objects='spec' and empty spec."""
        pipeline_mock.include_data_objects = 'spec'
        pipeline_mock.data_objects = []
        
        with patch.object(composer.f_handler, 'get_all_dir_names', return_value=['table1', 'table2']):
            result = composer.get_object_list_from_output_dir(pipeline_mock)
            
            # Should return empty list
            assert result == []
    
    def test_get_data_objects_intersection(self, composer):
        """Test get_data_objects_intersection method."""
        list1 = ['table1', 'table2', 'table3']
        list2 = ['table2', 'table3', 'table4']
        
        result = composer.get_data_objects_intersection(list1, list2)
        
        assert set(result) == {'table2', 'table3'}
    
    def test_get_data_objects_intersection_empty(self, composer):
        """Test get_data_objects_intersection with no intersection."""
        list1 = ['table1', 'table2']
        list2 = ['table3', 'table4']
        
        result = composer.get_data_objects_intersection(list1, list2)
        
        assert result == []
    
    def test_get_object_spec_from_array_found(self, composer):
        """Test get_object_spec_from_array when object is found."""
        data_objects_spec = [
            {'object_spec': {'object_name': 'table1'}},
            {'object_spec': {'object_name': 'table2'}}
        ]
        
        idx, spec = composer.get_object_spec_from_array(data_objects_spec, 'table1')
        
        assert idx == 0
        assert spec['object_spec']['object_name'] == 'table1'
    
    def test_get_object_spec_from_array_not_found(self, composer):
        """Test get_object_spec_from_array when object is not found."""
        data_objects_spec = [
            {'object_spec': {'object_name': 'table1'}}
        ]
        
        result = composer.get_object_spec_from_array(data_objects_spec, 'nonexistent')
        
        assert result is None
    
    def test_compose_bucket_object_path_with_prefix(self, composer):
        """Test compose_bucket_object_path with prefix."""
        prefix = 'pipelines/{pipeline_name}/data'
        pipeline_name = 'test_pipeline'
        object_name = 'table1'
        
        result = composer.compose_bucket_object_path(prefix, pipeline_name, object_name)
        
        assert result == 'pipelines/test_pipeline/data/table1'
    
    def test_compose_bucket_object_path_without_prefix(self, composer):
        """Test compose_bucket_object_path without prefix."""
        result = composer.compose_bucket_object_path(None, 'test_pipeline', 'table1')
        
        assert result == 'table1'
    
    def test_compose_bucket_object_path_empty_prefix(self, composer):
        """Test compose_bucket_object_path with empty prefix."""
        result = composer.compose_bucket_object_path('', 'test_pipeline', 'table1')
        
        assert result == 'table1'
    
    def test_save_data_objects_yaml(self, composer, tmp_path):
        """Test save_data_objects with YAML format."""
        pipeline_fpath = tmp_path / 'pipeline.yaml'
        pipeline_all_obj = {
            'pipeline': {'pipeline_name': 'test'},
            'data_objects_spec': []
        }
        data_objects_spec = [
            {'object_spec': {'object_name': 'table1'}}
        ]
        
        with patch.object(composer.f_handler, 'save_dict_to_yaml') as mock_save:
            composer.save_data_objects(pipeline_all_obj, data_objects_spec, str(pipeline_fpath))
            
            mock_save.assert_called_once()
            call_args = mock_save.call_args[0]
            assert call_args[1]['data_objects_spec'] == data_objects_spec
    
    def test_save_data_objects_json(self, composer, tmp_path):
        """Test save_data_objects with JSON format."""
        pipeline_fpath = tmp_path / 'pipeline.json'
        pipeline_all_obj = {
            'pipeline': {'pipeline_name': 'test'},
            'data_objects_spec': []
        }
        data_objects_spec = [
            {'object_spec': {'object_name': 'table1'}}
        ]
        
        with patch.object(composer.f_handler, 'save_dict_to_json') as mock_save:
            composer.save_data_objects(pipeline_all_obj, data_objects_spec, str(pipeline_fpath))
            
            mock_save.assert_called_once()
            call_args = mock_save.call_args[0]
            assert call_args[1]['data_objects_spec'] == data_objects_spec

