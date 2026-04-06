"""
Unit tests for Connections class.

Tests connection loading, resolution, and attribute merging functionality.
"""
import pytest
import os
import tempfile
import yaml
import json
from unittest.mock import Mock, patch, MagicMock
from petaly.core.connections import Connections


class TestConnections:
    """Test suite for Connections class."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def connections_yaml(self, temp_dir):
        """Create a sample connections.yaml file."""
        connections_fpath = os.path.join(temp_dir, 'connections.yaml')
        connections_data = {
            'connections': {
                'postgres_prod': {
                    'connector_type': 'postgres',
                    'host': 'prod-db.example.com',
                    'port': 5432,
                    'database': 'production',
                    'user': 'admin',
                    'password': 'secret123'
                },
                'bigquery_warehouse': {
                    'connector_type': 'bigquery',
                    'gcp_project_id': 'my-project',
                    'gcp_region': 'us-central1'
                },
                'csv_local': {
                    'connector_type': 'csv',
                    'source_base_dir': '/data/source',
                    'target_base_dir': '/data/dest'
                }
            }
        }
        with open(connections_fpath, 'w') as f:
            yaml.dump(connections_data, f)
        return connections_fpath
    
    @pytest.fixture
    def connections_json(self, temp_dir):
        """Create a sample connections.json file."""
        connections_fpath = os.path.join(temp_dir, 'connections.json')
        connections_data = {
            'connections': {
                'postgres_prod': {
                    'connector_type': 'postgres',
                    'host': 'prod-db.example.com',
                    'port': 5432,
                    'database': 'production',
                    'user': 'admin',
                    'password': 'secret123'
                }
            }
        }
        with open(connections_fpath, 'w') as f:
            json.dump(connections_data, f)
        return connections_fpath
    
    def test_initialization(self, temp_dir):
        """Test Connections initialization."""
        connections_fpath = os.path.join(temp_dir, 'connections.yaml')
        connections = Connections(connections_fpath)
        
        assert connections.connections_fpath == connections_fpath
        assert connections._connections_config is None
        assert connections._connections_dict is None
        assert connections.f_handler is not None
    
    def test_load_config_yaml(self, connections_yaml):
        """Test loading YAML connections file."""
        connections = Connections(connections_yaml)
        config = connections.load_config()
        
        assert config is not None
        assert 'connections' in config
        assert 'postgres_prod' in config['connections']
        assert config['connections']['postgres_prod']['connector_type'] == 'postgres'
    
    def test_load_config_json(self, connections_json):
        """Test loading JSON connections file."""
        connections = Connections(connections_json)
        config = connections.load_config()
        
        assert config is not None
        assert 'connections' in config
        assert 'postgres_prod' in config['connections']
    
    def test_load_config_nonexistent_file(self, temp_dir):
        """Test loading non-existent connections file."""
        connections_fpath = os.path.join(temp_dir, 'nonexistent.yaml')
        connections = Connections(connections_fpath)
        config = connections.load_config()
        
        assert config is None
        assert connections._connections_dict is None
    
    def test_load_config_caching(self, connections_yaml):
        """Test that config is cached after first load."""
        connections = Connections(connections_yaml)
        
        # First load
        config1 = connections.load_config()
        # Second load should return cached config
        config2 = connections.load_config()
        
        assert config1 is config2  # Same object reference
    
    def test_resolve_connection_success(self, connections_yaml):
        """Test successful connection resolution."""
        connections = Connections(connections_yaml)
        connection = connections.resolve_connection('postgres_prod')
        
        assert connection is not None
        assert connection['connector_type'] == 'postgres'
        assert connection['host'] == 'prod-db.example.com'
        assert connection['port'] == 5432
    
    def test_resolve_connection_not_found(self, connections_yaml):
        """Test connection resolution when connection doesn't exist."""
        connections = Connections(connections_yaml)
        connection = connections.resolve_connection('nonexistent')
        
        assert connection is None
    
    def test_resolve_connection_with_type(self, connections_yaml):
        """Test connection resolution with connection type."""
        connections = Connections(connections_yaml)
        connection = connections.resolve_connection('postgres_prod', 'source')
        
        assert connection is not None
        assert connection['connector_type'] == 'postgres'
    
    def test_resolve_attributes_with_connection_name(self, connections_yaml):
        """Test attribute resolution with connection_name reference."""
        connections = Connections(connections_yaml)
        attributes = {'connection_name': 'postgres_prod'}
        
        resolved = connections.resolve_attributes(attributes, 'source')
        
        assert resolved['connector_type'] == 'postgres'
        assert resolved['host'] == 'prod-db.example.com'
        assert resolved['connection_name'] == 'postgres_prod'  # Preserved
    
    def test_resolve_attributes_with_inline_overrides(self, connections_yaml):
        """Test that inline attributes override connection attributes."""
        connections = Connections(connections_yaml)
        attributes = {
            'connection_name': 'postgres_prod',
            'port': 5433,  # Override port
            'database': 'test_db'  # Override database
        }
        
        resolved = connections.resolve_attributes(attributes, 'source')
        
        assert resolved['connector_type'] == 'postgres'  # From connection
        assert resolved['host'] == 'prod-db.example.com'  # From connection
        assert resolved['port'] == 5433  # Overridden
        assert resolved['database'] == 'test_db'  # Overridden
        assert resolved['connection_name'] == 'postgres_prod'  # Preserved
    
    def test_resolve_attributes_without_connection_name(self, connections_yaml):
        """Test attribute resolution without connection_name (inline only)."""
        connections = Connections(connections_yaml)
        attributes = {
            'connector_type': 'mysql',
            'host': 'localhost',
            'port': 3306
        }
        
        resolved = connections.resolve_attributes(attributes, 'source')
        
        assert resolved == attributes  # Should return copy of attributes
    
    def test_resolve_attributes_missing_connection_error(self, temp_dir):
        """Test error when connection_name is provided but connection doesn't exist."""
        connections_fpath = os.path.join(temp_dir, 'empty.yaml')
        with open(connections_fpath, 'w') as f:
            yaml.dump({'connections': {}}, f)
        
        connections = Connections(connections_fpath)
        attributes = {'connection_name': 'nonexistent'}
        
        with pytest.raises(ValueError, match="Connection 'nonexistent' not found"):
            connections.resolve_attributes(attributes, 'source')
    
    def test_resolve_attributes_missing_connection_with_other_attrs(self, temp_dir):
        """Test warning when connection doesn't exist but other attributes are present."""
        connections_fpath = os.path.join(temp_dir, 'empty.yaml')
        with open(connections_fpath, 'w') as f:
            yaml.dump({'connections': {}}, f)
        
        connections = Connections(connections_fpath)
        attributes = {
            'connection_name': 'nonexistent',
            'connector_type': 'mysql',  # Other attributes present
            'host': 'localhost'
        }
        
        resolved = connections.resolve_attributes(attributes, 'source')
        
        # Should return other attributes without connection_name
        assert 'connection_name' not in resolved
        assert resolved['connector_type'] == 'mysql'
        assert resolved['host'] == 'localhost'
    
    def test_get_all_connections(self, connections_yaml):
        """Test getting all connections."""
        connections = Connections(connections_yaml)
        all_connections = connections.get_all_connections()
        
        assert isinstance(all_connections, dict)
        assert 'postgres_prod' in all_connections
        assert 'bigquery_warehouse' in all_connections
        assert 'csv_local' in all_connections
    
    def test_get_all_connections_empty(self, temp_dir):
        """Test getting all connections when file doesn't exist."""
        connections_fpath = os.path.join(temp_dir, 'nonexistent.yaml')
        connections = Connections(connections_fpath)
        all_connections = connections.get_all_connections()
        
        assert all_connections == {}
    
    def test_unsupported_file_format(self, temp_dir):
        """Test loading unsupported file format."""
        connections_fpath = os.path.join(temp_dir, 'connections.txt')
        with open(connections_fpath, 'w') as f:
            f.write('some text')
        
        connections = Connections(connections_fpath)
        config = connections.load_config()
        
        assert config is None
