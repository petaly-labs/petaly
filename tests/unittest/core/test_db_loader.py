from types import SimpleNamespace

from petaly.core.db_loader import DBLoader


class _DummyLoader(DBLoader):
    def load_from(self, object_load_conf):
        return None

    def compose_load_from_stmt(self, data_object, loader_obj_conf):
        return ""

    def create_table(self, loader_obj_conf: dict):
        return None


class TestDbLoader:
    def test_normalize_mysql_temporal_column_type_adds_precision(self):
        loader = object.__new__(_DummyLoader)
        loader.pipeline = SimpleNamespace(target_connector_id='mysql')

        assert loader._normalize_mysql_temporal_column_type(
            {'data_type': 'timestamp with time zone'}, 'datetime'
        ) == 'datetime(6)'
        assert loader._normalize_mysql_temporal_column_type(
            {'data_type': 'timestamp'}, 'timestamp'
        ) == 'timestamp(6)'

    def test_normalize_mysql_temporal_column_type_keeps_non_temporal(self):
        loader = object.__new__(_DummyLoader)
        loader.pipeline = SimpleNamespace(target_connector_id='mysql')

        assert loader._normalize_mysql_temporal_column_type(
            {'data_type': 'text'}, 'text'
        ) == 'text'
