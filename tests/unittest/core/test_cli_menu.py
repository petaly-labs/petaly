import os
import tempfile
import yaml
from types import SimpleNamespace

from petaly.cli.cli_menu import CliMenu


class TestCliMenu:
    def test_get_connection_names_by_type_filters_source_and_target(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            connections_fpath = os.path.join(temp_dir, 'connections.yaml')
            with open(connections_fpath, 'w', encoding='utf-8') as f:
                yaml.safe_dump(
                    {
                        'connections': {
                            'postgres_source': {
                                'endpoint_type': 'source',
                                'connector_type': 'postgres',
                            },
                            'mysql_target': {
                                'endpoint_type': 'target',
                                'connector_type': 'mysql',
                            },
                            'csv_source': {
                                'endpoint_type': 'source',
                                'connector_type': 'csv',
                            },
                            'legacy_untyped': {
                                'connector_type': 'postgres',
                            },
                        }
                    },
                    f,
                    sort_keys=False,
                )

            main_config = SimpleNamespace(
                global_settings={'full_pipeline_wizard': 'true', 'connections_file_format': 'yaml'},
                connections_skeleton_fpath=os.path.join(temp_dir, 'connections_skeleton.json'),
                pipeline_meta_config_fpath=os.path.join(temp_dir, 'pipeline_meta_config.json'),
                pipeline_skeleton_fpath=os.path.join(temp_dir, 'pipeline_skeleton.json'),
                pipeline_base_dpath=temp_dir,
            )

            with open(main_config.connections_skeleton_fpath, 'w', encoding='utf-8') as f:
                f.write('{"connections": {}}')
            with open(main_config.pipeline_meta_config_fpath, 'w', encoding='utf-8') as f:
                f.write('{}')
            with open(main_config.pipeline_skeleton_fpath, 'w', encoding='utf-8') as f:
                f.write('{"pipeline": {"source_attributes": {}, "target_attributes": {}, "load_attributes": {}}, "data_objects_spec": []}')

            cli_menu = CliMenu(main_config)

            source_connections = cli_menu.get_connection_names_by_type(connections_fpath, 'source')
            target_connections = cli_menu.get_connection_names_by_type(connections_fpath, 'target')

            assert source_connections == ['postgres_source', 'csv_source']
            assert target_connections == ['mysql_target']
