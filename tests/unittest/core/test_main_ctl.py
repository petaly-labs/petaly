"""
Unit tests for MainCtl incremental object batching.
"""
from types import SimpleNamespace

from petaly.core.main_ctl import MainCtl


class _DummyExtractor:
    rows_sequence = []
    extract_calls = 0
    where_timestamps = []

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.f_handler = SimpleNamespace(cleanup_dir=lambda *_args, **_kwargs: None)

    def extract_per_object(self, object_name):
        _DummyExtractor.extract_calls += 1
        _DummyExtractor.where_timestamps.append(
            self.pipeline.runtime_incremental_state.get(object_name, '1970-01-01T00:00:00Z')
        )


class _DummyLoader:
    load_calls = 0

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.load_state = SimpleNamespace(ensure_object_state=lambda *_args, **_kwargs: None)

    def get_loader_obj_conf(self, object_name):
        return {
            'output_data_object_dir': '/tmp/output',
            'object_settings': {'header': True},
            'table_ddl_dict': {'schema_name': None, 'table_name': object_name},
            'recreate_destination_object': False
        }

    def count_rows_in_csv_files(self, output_data_object_dir, loader_obj_conf):
        batch_index = _DummyExtractor.extract_calls - 1
        if batch_index < 0 or batch_index >= len(_DummyExtractor.rows_sequence):
            return 0
        return _DummyExtractor.rows_sequence[batch_index]

    def load_per_object(self, object_name, load_summary=None):
        _DummyLoader.load_calls += 1
        batch_index = _DummyLoader.load_calls - 1
        rows_loaded = _DummyExtractor.rows_sequence[batch_index]
        self.pipeline.runtime_incremental_state[object_name] = f"2026-04-06T08:00:0{_DummyLoader.load_calls}Z"
        return 'success', rows_loaded, object_name, False


class TestMainCtl:
    def setup_method(self):
        _DummyExtractor.rows_sequence = []
        _DummyExtractor.extract_calls = 0
        _DummyExtractor.where_timestamps = []
        _DummyLoader.load_calls = 0

    def test_incremental_batch_size_processes_all_batches_in_one_run(self):
        _DummyExtractor.rows_sequence = [1000, 1000, 200]

        main_config = SimpleNamespace(
            get_extractor_class=lambda connector_id: _DummyExtractor,
            get_loader_class=lambda connector_id: _DummyLoader
        )

        pipeline = SimpleNamespace(
            pipeline_name='test_pipeline',
            pipeline_fpath='/tmp/pipeline.yaml',
            source_connector_id='postgres',
            target_connector_id='mysql',
            source_attr={'connection_name': 'postgres_source'},
            target_attr={'connection_name': 'mysql_target'},
            load_attributes={'max_workers': 1, 'dump_mode': False},
            output_pipeline_dpath='/tmp/output',
            output_object_data_dpath='/tmp/output/{object_name}/data',
            output_object_metadata_dpath='/tmp/output/{object_name}/metadata',
            csv_default_settings={'header': True, 'columns_delimiter': ',', 'columns_quote': 'double'},
            data_objects=['name_basics_incr'],
            data_objects_spec=[
                {
                    'object_spec': {
                        'object_name': 'name_basics_incr',
                        'extract_load_mode': 'incremental',
                        'column_primary_key': 'id',
                        'column_last_modified': 'modified_at',
                        'batch_size': 1000,
                        'recreate_destination_object': False,
                        'cleanup_linebreak_in_fields': False
                    }
                }
            ],
            all_from_schema=False,
            use_data_objects_spec=True,
            runtime_incremental_state={},
            m_conf=main_config
        )

        main_ctl = MainCtl(main_config)
        main_ctl.run_pipeline(pipeline, run_endpoint=None, object_name_list=None)

        assert _DummyExtractor.extract_calls == 3
        assert _DummyLoader.load_calls == 3
        assert _DummyExtractor.where_timestamps == [
            '1970-01-01T00:00:00Z',
            '2026-04-06T08:00:01Z',
            '2026-04-06T08:00:02Z',
        ]
