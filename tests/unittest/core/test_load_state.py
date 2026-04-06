import os
import tempfile
from unittest.mock import MagicMock

from petaly.core.load_state import LoadState


class TestLoadState:
    def _make_pipeline(self, temp_dir):
        pipeline = MagicMock()
        pipeline.pipeline_dpath = temp_dir
        pipeline.output_object_metadata_dpath = os.path.join(temp_dir, "{object_name}", "metadata")
        pipeline.m_conf = MagicMock()
        return pipeline

    def test_initialize_first_batch_state_empty_existing_table_uses_epoch(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = self._make_pipeline(temp_dir)
            load_state = LoadState(pipeline)

            timestamp = load_state.initialize_first_batch_state(
                object_name="users",
                schema_table_name="public.users",
                column_last_modified="modified_at",
                db_connector=MagicMock(),
                table_exists=True,
            )

            assert timestamp == LoadState.UNIX_EPOCH_TIMESTAMP

            state = load_state.load_state("users")
            assert state["load_mode"] == "incremental"
            assert state["object_loaded_timestamp"] == LoadState.UNIX_EPOCH_TIMESTAMP

    def test_initialize_existing_state_without_timestamp_uses_epoch_when_table_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = self._make_pipeline(temp_dir)
            load_state = LoadState(pipeline)

            load_state._create_initial_state_file("users", load_mode="incremental", timestamp=None)

            timestamp = load_state.initialize_first_batch_state(
                object_name="users",
                schema_table_name="public.users",
                column_last_modified="modified_at",
                db_connector=MagicMock(),
                table_exists=True,
            )

            assert timestamp == LoadState.UNIX_EPOCH_TIMESTAMP

            state = load_state.load_state("users")
            assert state["load_mode"] == "incremental"
            assert state["object_loaded_timestamp"] == LoadState.UNIX_EPOCH_TIMESTAMP

    def test_initialize_first_batch_state_refreshes_timestamp_from_state_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = self._make_pipeline(temp_dir)
            extractor_state = LoadState(pipeline)

            timestamp = extractor_state.initialize_first_batch_state(
                object_name="users",
                schema_table_name="public.users",
                column_last_modified="modified_at",
                db_connector=MagicMock(),
                table_exists=True,
            )

            assert timestamp == LoadState.UNIX_EPOCH_TIMESTAMP

            loader_state = LoadState(pipeline)
            loader_state.save_batch_state(
                object_name="users",
                batch_end_timestamp="2026-04-06T08:00:00Z",
                rows_loaded=1000,
                load_mode="incremental",
            )

            refreshed_timestamp = extractor_state.initialize_first_batch_state(
                object_name="users",
                schema_table_name="public.users",
                column_last_modified="modified_at",
                db_connector=MagicMock(),
                table_exists=True,
            )

            assert refreshed_timestamp == "2026-04-06T08:00:00Z"

    def test_pipeline_level_state_file_is_load_state_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = self._make_pipeline(temp_dir)
            load_state = LoadState(pipeline)

            load_state.save_batch_state(
                object_name="users",
                batch_end_timestamp="2026-04-06T09:00:00Z",
                rows_loaded=100,
                load_mode="incremental",
            )

            assert os.path.exists(os.path.join(temp_dir, "load_state.json"))
            assert not os.path.exists(os.path.join(temp_dir, "incremental_load.json"))
