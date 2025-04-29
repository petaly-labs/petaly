import pytest
from unittest.mock import Mock, patch, MagicMock, create_autospec, call
from google.cloud import storage, exceptions
from google.cloud.exceptions import NotFound, Forbidden
from google.api_core import exceptions as core_exceptions
from petaly.connectors.gs.gs_connector import GSConnector
from petaly.connectors.gs.gs_extractor import GSExtractor
from petaly.connectors.gs.gs_loader import GSLoader
from petaly.utils.file_handler import FileHandler
import os

@pytest.fixture
def mock_pipeline():
    pipeline = MagicMock()
    pipeline.source_attr = {
        'gcp_bucket_name': 'test-bucket',
        'gcp_project_id': 'test-project',
        'gcp_region': 'us-central1',
        'bucket_pipeline_prefix': 'test-prefix'
    }
    pipeline.target_attr = {
        'gcp_bucket_name': 'test-bucket',
        'gcp_project_id': 'test-project',
        'gcp_region': 'us-central1'
    }
    return pipeline

@pytest.fixture
def mock_storage_client():
    with patch('google.cloud.storage.Client', autospec=True) as mock:
        yield mock

class TestGSConnector:
    @pytest.fixture
    def connector(self):
        return GSConnector()

    def test_rename_blob_success(self, connector, mock_storage_client):
        # Setup
        bucket_name = "test-bucket"
        old_blob_name = "old/path/file.txt"
        new_blob_name = "new/path/file.txt"

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_new_blob = MagicMock()
        mock_new_blob.name = new_blob_name

        mock_bucket.blob.return_value = mock_blob
        mock_bucket.rename_blob.return_value = mock_new_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Execute
        result = connector.rename_blob(bucket_name, old_blob_name, new_blob_name)

        # Verify
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.blob.assert_called_once_with(old_blob_name)
        mock_bucket.rename_blob.assert_called_once_with(mock_blob, new_blob_name)
        assert result == new_blob_name

    def test_rename_blob_not_found(self, connector, mock_storage_client):
        # Setup
        bucket_name = "test-bucket"
        old_blob_name = "old/path/file.txt"
        new_blob_name = "new/path/file.txt"

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.side_effect = core_exceptions.NotFound("Blob not found")
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Execute and verify
        with pytest.raises(core_exceptions.NotFound):
            connector.rename_blob(bucket_name, old_blob_name, new_blob_name)

    def test_delete_object_in_bucket_success(self, connector, mock_storage_client):
        # Setup
        bucket_name = "test-bucket"
        prefix = "test/"

        mock_bucket = MagicMock()
        mock_blobs = [MagicMock(name="file1.txt"), MagicMock(name="file2.txt")]
        mock_bucket.list_blobs.return_value = mock_blobs
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Execute
        connector.delete_object_in_bucket(bucket_name, prefix)

        # Verify
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.list_blobs.assert_called_once_with(prefix=prefix)
        mock_bucket.delete_blobs.assert_called_once_with(mock_blobs)

    def test_download_files_from_bucket_success(self, connector, mock_storage_client):
        # Setup
        bucket_name = "test-bucket"
        blob_prefix = "test/"
        file_names = ["file1.txt", "file2.txt"]
        destination_dpath = "/tmp/test"

        mock_bucket = MagicMock()
        mock_blobs = {}
        for name in file_names:
            blob_path = f"{blob_prefix}/{name}"
            mock_blob = MagicMock()
            mock_blobs[blob_path] = mock_blob

        mock_bucket.blob.side_effect = lambda name: mock_blobs.get(name)
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        with patch('os.path.exists', return_value=False), \
             patch('os.makedirs') as mock_makedirs:

            # Execute
            result = connector.download_files_from_bucket(
                bucket_name=bucket_name,
                blob_prefix=blob_prefix,
                file_names=file_names,
                destination_dpath=destination_dpath
            )

            # Verify
            mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
            mock_makedirs.assert_called_once_with(destination_dpath, exist_ok=True)
            assert len(result) == len(file_names)
            for name in file_names:
                blob_path = f"{blob_prefix}/{name}"
                mock_blobs[blob_path].download_to_filename.assert_called_once()

    def test_get_bucket_file_list_success(self, connector, mock_storage_client):
        # Setup
        bucket_name = "test-bucket"
        blob_prefix = "test/"
        mock_blobs = [
            MagicMock(name=f"{blob_prefix}file1.txt"),
            MagicMock(name=f"{blob_prefix}file2.txt")
        ]

        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = mock_blobs
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Execute
        result = connector.get_bucket_file_list(bucket_name, blob_prefix)

        # Verify
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.list_blobs.assert_called_once_with(prefix=blob_prefix, delimiter='/')
        assert len(result) == 2
        assert all(blob.name in result for blob in mock_blobs)

    def test_upload_files_to_bucket_success(self, connector, mock_storage_client):
        # Setup
        bucket_name = "test-bucket"
        blob_prefix = "test"
        source_files = ["/tmp/test/file1.txt", "/tmp/test/file2.txt"]

        mock_bucket = MagicMock()
        mock_blobs = {}
        for file_path in source_files:
            file_name = file_path.split('/')[-1]
            blob_path = f"{blob_prefix}/{file_name}"
            mock_blob = MagicMock()
            mock_blobs[blob_path] = mock_blob
            mock_blob.name = blob_path

        mock_bucket.blob.side_effect = lambda name: mock_blobs.get(name)
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Execute
        with patch('os.path.exists', return_value=True):
            result = connector.upload_files_to_bucket(bucket_name, blob_prefix, source_files)

        # Verify
        mock_storage_client.return_value.bucket.assert_has_calls([call(bucket_name)] * len(source_files), any_order=True)
        for file_path in source_files:
            file_name = file_path.split('/')[-1]
            blob_path = f"{blob_prefix}/{file_name}"
            mock_blobs[blob_path].upload_from_filename.assert_called_once_with(file_path)
        assert len(result) == len(source_files)
        assert all(f"gs://{bucket_name}/{blob.name}" in result for blob in mock_blobs.values())

class TestGSExtractor:
    @pytest.fixture
    def extractor(self, mock_storage_client, mock_pipeline):
        return GSExtractor(mock_pipeline)

    @pytest.fixture
    def mock_gs_connector_extractor(self):
        with patch('petaly.connectors.gs.gs_extractor.GSConnector') as mock:
            yield mock

    @pytest.fixture
    def mock_file_handler_extractor(self):
        with patch('petaly.utils.file_handler.FileHandler') as mock:
            yield mock

    def test_extract_to_success(self, extractor, mock_gs_connector_extractor, mock_file_handler_extractor):
        # Setup
        extractor_obj_conf = {
            'output_data_object_dir': '/tmp/test',
            'blob_prefix': 'test/',
            'file_names': ["file1.txt", "file2.txt"]
        }

        mock_connector = MagicMock()
        mock_gs_connector_extractor.return_value = mock_connector
        mock_connector.download_files_from_bucket.return_value = [
            "/tmp/test/file1.txt",
            "/tmp/test/file2.txt"
        ]

        mock_handler = MagicMock()
        mock_file_handler_extractor.return_value = mock_handler
        extractor.f_handler = mock_handler
        extractor.gs_connector = mock_connector

        # Execute
        result = extractor.extract_to(extractor_obj_conf)

        # Verify
        mock_handler.cleanup_dir.assert_called_once_with('/tmp/test')
        mock_connector.download_files_from_bucket.assert_called_once_with(
            bucket_name=extractor.cloud_bucket_name,
            blob_prefix='test/',
            file_names=["file1.txt", "file2.txt"],
            destination_dpath='/tmp/test'
        )
        assert result == ["/tmp/test/file1.txt", "/tmp/test/file2.txt"]

    def test_extract_to_no_files(self, extractor, mock_gs_connector_extractor, mock_file_handler_extractor):
        # Setup
        extractor_obj_conf = {
            'output_data_object_dir': '/tmp/test',
            'blob_prefix': 'test/',
            'file_names': []
        }

        mock_connector = MagicMock()
        mock_gs_connector_extractor.return_value = mock_connector
        mock_connector.download_files_from_bucket.return_value = []

        mock_handler = MagicMock()
        mock_file_handler_extractor.return_value = mock_handler
        extractor.f_handler = mock_handler
        extractor.gs_connector = mock_connector

        # Execute
        result = extractor.extract_to(extractor_obj_conf)

        # Verify
        mock_handler.cleanup_dir.assert_called_once_with('/tmp/test')
        mock_connector.download_files_from_bucket.assert_called_once_with(
            bucket_name=extractor.cloud_bucket_name,
            blob_prefix='test/',
            file_names=[],
            destination_dpath='/tmp/test'
        )
        assert result == []

    def test_extract_to_download_error(self, extractor, mock_gs_connector_extractor, mock_file_handler_extractor):
        # Setup
        extractor_obj_conf = {
            'output_data_object_dir': '/tmp/test',
            'blob_prefix': 'test/',
            'file_names': ["file1.txt"]
        }

        mock_connector = MagicMock()
        mock_gs_connector_extractor.return_value = mock_connector
        mock_connector.download_files_from_bucket.side_effect = Forbidden("Access denied")

        mock_handler = MagicMock()
        mock_file_handler_extractor.return_value = mock_handler
        extractor.f_handler = mock_handler
        extractor.gs_connector = mock_connector

        # Execute and verify
        with pytest.raises(Forbidden):
            extractor.extract_to(extractor_obj_conf)

    def test_extract_to_directory_error(self, extractor, mock_gs_connector_extractor, mock_file_handler_extractor):
        # Setup
        extractor_obj_conf = {
            'output_data_object_dir': '/tmp/test',
            'blob_prefix': 'test/',
            'file_names': ["file1.txt"]
        }

        mock_connector = MagicMock()
        mock_gs_connector_extractor.return_value = mock_connector
        mock_connector.download_files_from_bucket.return_value = ["/tmp/test/file1.txt"]

        mock_handler = MagicMock()
        mock_file_handler_extractor.return_value = mock_handler
        mock_handler.cleanup_dir.side_effect = OSError("Directory error")
        extractor.f_handler = mock_handler
        extractor.gs_connector = mock_connector

        # Execute and verify
        with pytest.raises(OSError):
            extractor.extract_to(extractor_obj_conf)

class TestGSLoader:
    @pytest.fixture
    def loader(self, mock_storage_client, mock_pipeline):
        return GSLoader(mock_pipeline)

    @pytest.fixture
    def mock_gs_connector_loader(self):
        with patch('petaly.connectors.gs.gs_loader.GSConnector') as mock:
            yield mock

    @pytest.fixture
    def mock_file_handler_loader(self):
        with patch('petaly.utils.file_handler.FileHandler') as mock:
            yield mock

    def test_load_from_success(self, loader, mock_gs_connector_loader, mock_file_handler_loader):
        # Setup
        loader_obj_conf = {
            'blob_prefix': 'test/',
            'file_list': ['/tmp/test/file1.txt', '/tmp/test/file2.txt']
        }

        mock_connector = MagicMock()
        mock_gs_connector_loader.return_value = mock_connector
        mock_connector.upload_files_to_bucket.return_value = [
            "gs://test-bucket/test/file1.txt",
            "gs://test-bucket/test/file2.txt"
        ]

        mock_handler = MagicMock()
        mock_file_handler_loader.return_value = mock_handler
        loader.f_handler = mock_handler
        loader.gs_connector = mock_connector

        # Execute
        loader.load_from(loader_obj_conf)

        # Verify
        mock_connector.delete_object_in_bucket.assert_called_once_with(
            loader.cloud_bucket_name,
            loader_obj_conf['blob_prefix']
        )
        mock_connector.upload_files_to_bucket.assert_called_once_with(
            loader.cloud_bucket_name,
            loader_obj_conf['blob_prefix'],
            loader_obj_conf['file_list']
        )

    def test_load_from_file_not_found(self, loader, mock_gs_connector_loader, mock_file_handler_loader):
        # Setup
        loader_obj_conf = {
            'blob_prefix': 'test/',
            'file_list': ['/tmp/test/nonexistent.txt']
        }

        mock_connector = MagicMock()
        mock_gs_connector_loader.return_value = mock_connector
        mock_connector.upload_files_to_bucket.side_effect = FileNotFoundError("File not found")

        mock_handler = MagicMock()
        mock_file_handler_loader.return_value = mock_handler
        loader.f_handler = mock_handler
        loader.gs_connector = mock_connector

        # Execute and verify
        with pytest.raises(FileNotFoundError):
            loader.load_from(loader_obj_conf)

    def test_load_from_no_files(self, loader, mock_gs_connector_loader, mock_file_handler_loader):
        # Setup
        loader_obj_conf = {
            'blob_prefix': 'test/',
            'file_list': []
        }

        mock_connector = MagicMock()
        mock_gs_connector_loader.return_value = mock_connector
        mock_connector.upload_files_to_bucket.return_value = []

        mock_handler = MagicMock()
        mock_file_handler_loader.return_value = mock_handler
        loader.f_handler = mock_handler
        loader.gs_connector = mock_connector

        # Execute
        loader.load_from(loader_obj_conf)

        # Verify
        mock_connector.delete_object_in_bucket.assert_called_once_with(
            loader.cloud_bucket_name,
            loader_obj_conf['blob_prefix']
        )
        mock_connector.upload_files_to_bucket.assert_called_once_with(
            loader.cloud_bucket_name,
            loader_obj_conf['blob_prefix'],
            []
        )

    def test_load_from_upload_error(self, loader, mock_gs_connector_loader, mock_file_handler_loader):
        # Setup
        loader_obj_conf = {
            'blob_prefix': 'test/',
            'file_list': ['/tmp/test/file1.txt']
        }

        mock_connector = MagicMock()
        mock_gs_connector_loader.return_value = mock_connector
        mock_connector.upload_files_to_bucket.side_effect = Forbidden("Access denied")

        mock_handler = MagicMock()
        mock_file_handler_loader.return_value = mock_handler
        loader.f_handler = mock_handler
        loader.gs_connector = mock_connector

        # Execute and verify
        with pytest.raises(Forbidden):
            loader.load_from(loader_obj_conf) 