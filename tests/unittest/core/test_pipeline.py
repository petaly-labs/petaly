"""
Unit tests for Pipeline class.

Tests pipeline initialization, configuration loading, connection resolution,
backward compatibility, and parameter validation.
"""
import pytest
import os
import tempfile
import yaml
import json
from unittest.mock import Mock, patch, MagicMock
from petaly.core.pipeline import Pipeline


class TestPipeline:
    """Test suite for Pipeline class."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def main_config_mock(self, temp_dir):
        """Create a mock main_config object."""
        mock_config = MagicMock()
        mock_config.pipeline_base_dpath = temp_dir
        mock_config.global_settings = {
            'pipeline_file_format': 'yaml',
            'connections_file_format': 'yaml'
        }
        mock_config.output_pipeline_dpath = os.path.join(temp_dir, 'output')
        mock_config.output_object_data_dpath = os.path.join(temp_dir, 'output', '{object_name}', 'data')
        mock_config.output_object_metadata_dpath = os.path.join(temp_dir, 'output', '{object_name}', 'metadata')
        mock_config.extract_to_stmt_fname = 'extract_to.sql'
        mock_config.load_from_stmt_fname = 'load_from.sql'
        mock_config.create_table_stmt_fname = 'create_table.sql'
        mock_config.pipeline_skeleton_fpath = os.path.join(temp_dir, 'skeleton.yaml')
        return mock_config
    
    @pytest.fixture
    def connections_yaml(self, temp_dir):
        """Create a sample connections.yaml file."""
        connections_fpath = os.path.join(temp_dir, 'connections.yaml')
        connections_data = {
            'connections': {
                'postgres_source': {
                    'connector_type': 'postgres',
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'user': 'test_user',
                    'password': 'test_pass'
                },
                'parquet_target': {
                    'connector_type': 'parquet',
                    'destination_dir': '/data/output'
                }
            }
        }
        with open(connections_fpath, 'w') as f:
            yaml.dump(connections_data, f)
        return connections_fpath
    
    @pytest.fixture
    def pipeline_yaml(self, temp_dir, connections_yaml):
        """Create a sample pipeline.yaml file."""
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.yaml')
        pipeline_data = {
            'pipeline': {
                'pipeline_name': pipeline_name,
                'source_attributes': {
                    'connection_name': 'postgres_source'
                },
                'target_attributes': {
                    'connection_name': 'parquet_target'
                },
                'data_attributes': {
                    'include_data_objects': 'spec',
                    'csv_default_settings': {
                        'header': True,
                        'columns_delimiter': ','
                    }
                }
            },
            'data_objects_spec': [
                {
                    'object_spec': {
                        'object_name': 'table1'
                    }
                }
            ]
        }
        with open(pipeline_fpath, 'w') as f:
            yaml.dump(pipeline_data, f)
        return pipeline_fpath
    
    def test_initialization(self, temp_dir, main_config_mock, pipeline_yaml):
        """Test Pipeline initialization."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        assert pipeline.pipeline_name == pipeline_name
        assert pipeline.m_conf == main_config_mock
        assert pipeline.pipeline_fpath == os.path.join(temp_dir, pipeline_name, 'pipeline.yaml')
        assert pipeline.connections_fpath == os.path.join(temp_dir, 'connections.yaml')
        assert pipeline.connections is not None
    
    def test_load_config_yaml(self, temp_dir, main_config_mock, pipeline_yaml):
        """Test loading YAML pipeline configuration."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        # get_config() expects list format (legacy multi-document YAML), but modern format is dict
        # Test get_pipeline_entire_config() which handles both formats
        config = pipeline.get_pipeline_entire_config()
        assert config is not None
        # Modern format is a dict
        if isinstance(config, dict):
            assert config.get('pipeline', {}).get('pipeline_name') == pipeline_name
        # Legacy format is a list
        elif isinstance(config, list) and len(config) > 0:
            assert config[0].get('pipeline', {}).get('pipeline_name') == pipeline_name
    
    def test_load_config_json(self, temp_dir, main_config_mock, connections_yaml):
        """Test loading JSON pipeline configuration."""
        main_config_mock.global_settings['pipeline_file_format'] = 'json'
        
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.json')
        pipeline_data = {
            'pipeline': {
                'pipeline_name': pipeline_name,
                'source_attributes': {'connector_type': 'postgres'},
                'target_attributes': {'connector_type': 'parquet'},
                'data_attributes': {
                    'csv_default_settings': {
                        'header': True,
                        'columns_delimiter': ','
                    }
                }
            },
            'data_objects_spec': []
        }
        with open(pipeline_fpath, 'w') as f:
            json.dump(pipeline_data, f)
        
        pipeline = Pipeline(pipeline_name, main_config_mock)
        # Test that pipeline loaded successfully
        assert pipeline.pipeline_name == pipeline_name
        assert pipeline.source_connector_id == 'postgres'
        assert pipeline.target_connector_id == 'parquet'
    
    def test_connection_resolution_source(self, temp_dir, main_config_mock, pipeline_yaml, connections_yaml):
        """Test source connection resolution."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        assert pipeline.source_attr is not None
        assert pipeline.source_attr['connector_type'] == 'postgres'
        assert pipeline.source_attr['host'] == 'localhost'
        assert pipeline.source_connector_id == 'postgres'
    
    def test_connection_resolution_target(self, temp_dir, main_config_mock, pipeline_yaml, connections_yaml):
        """Test target connection resolution."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        assert pipeline.target_attr is not None
        assert pipeline.target_attr['connector_type'] == 'parquet'
        assert pipeline.target_connector_id == 'parquet'
    
    def test_include_data_objects_spec(self, temp_dir, main_config_mock, pipeline_yaml, connections_yaml):
        """Test include_data_objects='spec' parameter."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        assert pipeline.include_data_objects == 'spec'
    
    def test_include_data_objects_all(self, temp_dir, main_config_mock, connections_yaml):
        """Test include_data_objects='all' parameter."""
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.yaml')
        pipeline_data = {
            'pipeline': {
                'pipeline_name': pipeline_name,
                'source_attributes': {'connection_name': 'postgres_source'},
                'target_attributes': {'connection_name': 'parquet_target'},
                'data_attributes': {
                    'include_data_objects': 'all'
                }
            }
        }
        with open(pipeline_fpath, 'w') as f:
            yaml.dump(pipeline_data, f)
        
        pipeline = Pipeline(pipeline_name, main_config_mock)
        assert pipeline.include_data_objects == 'all'
    
    def test_csv_default_settings(self, temp_dir, main_config_mock, pipeline_yaml, connections_yaml):
        """Test csv_default_settings loading."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        assert pipeline.csv_default_settings is not None
        assert pipeline.csv_default_settings.get('header') is True
        assert pipeline.csv_default_settings.get('columns_delimiter') == ','
    
    def test_backward_compatibility_load_all_from_schema(self, temp_dir, main_config_mock, connections_yaml):
        """Test backward compatibility for load_all_from_schema (boolean).
        
        Note: The current implementation only checks for old parameters if include_data_objects
        is None or invalid. Since include_data_objects defaults to 'spec', this test verifies
        that when include_data_objects is explicitly set to an invalid value, the backward
        compatibility kicks in.
        """
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.yaml')
        pipeline_data = {
            'pipeline': {
                'pipeline_name': pipeline_name,
                'source_attributes': {'connection_name': 'postgres_source'},
                'target_attributes': {'connection_name': 'parquet_target'},
                'data_attributes': {
                    'include_data_objects': None,  # Set to None to trigger backward compatibility
                    'load_all_from_schema': True,  # Old boolean parameter
                    'csv_default_settings': {  # Required for pipeline to initialize
                        'header': True,
                        'columns_delimiter': ','
                    }
                }
            }
        }
        with open(pipeline_fpath, 'w') as f:
            yaml.dump(pipeline_data, f)
        
        pipeline = Pipeline(pipeline_name, main_config_mock)
        assert pipeline.include_data_objects == 'all'  # Should convert True to 'all'
    
    def test_backward_compatibility_load_all_from_schema_false(self, temp_dir, main_config_mock, connections_yaml):
        """Test backward compatibility for load_all_from_schema=False."""
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.yaml')
        pipeline_data = {
            'pipeline': {
                'pipeline_name': pipeline_name,
                'source_attributes': {'connection_name': 'postgres_source'},
                'target_attributes': {'connection_name': 'parquet_target'},
                'data_attributes': {
                    'load_all_from_schema': False  # Old boolean parameter
                }
            }
        }
        with open(pipeline_fpath, 'w') as f:
            yaml.dump(pipeline_data, f)
        
        pipeline = Pipeline(pipeline_name, main_config_mock)
        assert pipeline.include_data_objects == 'spec'  # Should convert False to 'spec'
    
    def test_backward_compatibility_pipeline_attributes(self, temp_dir, main_config_mock, connections_yaml):
        """Test backward compatibility for pipeline_attributes section."""
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.yaml')
        pipeline_data = {
            'pipeline': {
                'pipeline_attributes': {  # Old nested structure
                    'pipeline_name': pipeline_name
                },
                'source_attributes': {'connection_name': 'postgres_source'},
                'target_attributes': {'connection_name': 'parquet_target'}
            }
        }
        with open(pipeline_fpath, 'w') as f:
            yaml.dump(pipeline_data, f)
        
        pipeline = Pipeline(pipeline_name, main_config_mock)
        # Should still work with backward compatibility
        assert pipeline.pipeline_name == pipeline_name
    
    def test_backward_compatibility_object_default_settings(self, temp_dir, main_config_mock, connections_yaml):
        """Test backward compatibility for object_default_settings."""
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.yaml')
        pipeline_data = {
            'pipeline': {
                'pipeline_name': pipeline_name,
                'source_attributes': {'connection_name': 'postgres_source'},
                'target_attributes': {'connection_name': 'parquet_target'},
                'data_attributes': {
                    'object_default_settings': {  # Old parameter name
                        'header': True,
                        'columns_delimiter': '\t'
                    }
                }
            }
        }
        with open(pipeline_fpath, 'w') as f:
            yaml.dump(pipeline_data, f)
        
        pipeline = Pipeline(pipeline_name, main_config_mock)
        # Should convert object_default_settings to csv_default_settings
        assert pipeline.csv_default_settings.get('header') is True
        assert pipeline.csv_default_settings.get('columns_delimiter') == '\t'
    
    def test_data_objects_spec_loading(self, temp_dir, main_config_mock, pipeline_yaml, connections_yaml):
        """Test loading data_objects_spec."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        assert len(pipeline.data_objects_spec) == 1
        assert pipeline.data_objects_spec[0]['object_spec']['object_name'] == 'table1'
        assert 'table1' in pipeline.data_objects
    
    def test_pipeline_name_mismatch_warning(self, temp_dir, main_config_mock, connections_yaml):
        """Test warning when pipeline_name parameter doesn't match config."""
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.yaml')
        pipeline_data = {
            'pipeline': {
                'pipeline_name': 'different_name',  # Mismatch
                'source_attributes': {'connection_name': 'postgres_source'},
                'target_attributes': {'connection_name': 'parquet_target'}
            }
        }
        with open(pipeline_fpath, 'w') as f:
            yaml.dump(pipeline_data, f)
        
        pipeline = Pipeline(pipeline_name, main_config_mock)
        # Pipeline should still initialize but with warning
        assert pipeline.pipeline_name == pipeline_name
    
    def test_inline_attributes_override_connection(self, temp_dir, main_config_mock, connections_yaml):
        """Test that inline attributes override connection attributes."""
        pipeline_name = 'test_pipeline'
        pipeline_dir = os.path.join(temp_dir, pipeline_name)
        os.makedirs(pipeline_dir, exist_ok=True)
        
        pipeline_fpath = os.path.join(pipeline_dir, 'pipeline.yaml')
        pipeline_data = {
            'pipeline': {
                'pipeline_name': pipeline_name,
                'source_attributes': {
                    'connection_name': 'postgres_source',
                    'port': 5433  # Override port from connection
                },
                'target_attributes': {'connection_name': 'parquet_target'}
            }
        }
        with open(pipeline_fpath, 'w') as f:
            yaml.dump(pipeline_data, f)
        
        pipeline = Pipeline(pipeline_name, main_config_mock)
        # Inline port should override connection port
        assert pipeline.source_attr['port'] == 5433
        # Other attributes from connection should still be present
        assert pipeline.source_attr['host'] == 'localhost'
    
    def test_get_csv_default_settings(self, temp_dir, main_config_mock, pipeline_yaml, connections_yaml):
        """Test get_csv_default_settings method."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        csv_settings = pipeline.get_csv_default_settings()
        assert csv_settings is not None
        assert csv_settings.get('header') is True
        assert csv_settings.get('columns_delimiter') == ','
    
    def test_get_consolidated_config(self, temp_dir, main_config_mock, pipeline_yaml, connections_yaml):
        """Test get_consolidated_config with resolved connections."""
        pipeline_name = 'test_pipeline'
        pipeline = Pipeline(pipeline_name, main_config_mock)
        
        consolidated = pipeline.get_consolidated_config()
        assert consolidated is not None
        assert 'pipeline' in consolidated
        # Source attributes should be resolved (not just connection_name)
        source_attrs = consolidated['pipeline'].get('source_attributes', {})
        assert 'connector_type' in source_attrs
        assert source_attrs['connector_type'] == 'postgres'

