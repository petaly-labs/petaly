import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock
import boto3
from botocore.exceptions import ClientError

from petaly.connectors.s3.s3_connector import S3Connector
from petaly.connectors.s3.s3_extractor import S3Extractor
from petaly.connectors.s3.s3_loader import S3Loader
from petaly.core.pipeline import Pipeline


class TestS3Connector:
    @pytest.fixture
    def endpoint_attr(self):
        return {
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'aws_region': 'us-east-1'
        }

    @pytest.fixture
    def mock_aws_session(self):
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        return mock_session

    @pytest.fixture
    def connector(self, endpoint_attr, mock_aws_session):
        with patch('boto3.session.Session', return_value=mock_aws_session):
            return S3Connector(endpoint_attr)

    def test_get_aws_session(self, endpoint_attr, mock_aws_session):
        # Setup
        with patch('boto3.session.Session', return_value=mock_aws_session):
            # Execute
            connector = S3Connector(endpoint_attr)
            
            # Verify
            assert connector.aws_session == mock_aws_session

    @patch('boto3.resource')
    def test_delete_object_in_bucket(self, mock_resource, connector):
        # Setup
        mock_bucket = MagicMock()
        mock_resource.return_value.Bucket.return_value = mock_bucket
        mock_objects = MagicMock()
        mock_bucket.objects.filter.return_value = [mock_objects]
        
        # Execute
        connector.delete_object_in_bucket('test-bucket', 'test/prefix')
        
        # Verify
        mock_resource.assert_called_once_with('s3')
        mock_bucket.objects.filter.assert_called_once_with(Prefix='test/prefix')
        mock_objects.delete.assert_called_once()

    @patch('boto3.resource')
    def test_get_bucket_file_list(self, mock_resource, connector):
        # Setup
        mock_bucket = MagicMock()
        mock_resource.return_value.Bucket.return_value = mock_bucket
        mock_objects = MagicMock()
        mock_objects.key = 'test/file.txt'
        mock_bucket.objects.filter.return_value = [mock_objects]
        
        # Execute
        result = connector.get_bucket_file_list('test-bucket', 'test/')
        
        # Verify
        assert result == ['test/file.txt']
        mock_resource.assert_called_once_with('s3')
        mock_bucket.objects.filter.assert_called_once_with(Prefix='test/')

    def test_download_files_from_bucket(self, connector):
        # Setup
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(connector.aws_session, 'client') as mock_client_factory:
                mock_client = MagicMock()
                mock_client_factory.return_value = mock_client
                
                # Execute
                result = connector.download_files_from_bucket(
                    bucket_name='test-bucket',
                    blob_prefix='test/',
                    file_names=['file1.txt', 'file2.txt'],
                    destination_dpath=temp_dir
                )
                
                # Verify
                assert len(result) == 2
                mock_client_factory.assert_called_once_with(service_name='s3')
                assert mock_client.download_file.call_count == 2

    @patch('boto3.client')
    def test_upload_files_to_bucket(self, mock_client, connector):
        # Setup
        with tempfile.NamedTemporaryFile() as temp_file:
            mock_client.return_value = MagicMock()
            mock_client.return_value.upload_file.return_value = None
            
            # Execute
            connector.upload_files_to_bucket(
                bucket_name='test-bucket',
                blob_prefix='test/',
                object_file_list=[temp_file.name]
            )
            
            # Verify
            mock_client.assert_called_once_with('s3')
            mock_client.return_value.upload_file.assert_called_once()


class TestS3Extractor:
    @pytest.fixture
    def pipeline_mock(self):
        pipeline = MagicMock(spec=Pipeline)
        pipeline.source_attr = {
            'aws_bucket_name': 'test-bucket',
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'aws_region': 'us-east-1'
        }
        pipeline.data_attributes = {
            'object_default_settings': {}
        }
        return pipeline

    @pytest.fixture
    def mock_aws_session(self):
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        return mock_session

    @pytest.fixture
    def extractor(self, pipeline_mock, mock_aws_session):
        with patch('boto3.session.Session', return_value=mock_aws_session):
            return S3Extractor(pipeline_mock)

    def test_extract_to(self, extractor):
        # Setup
        extractor_obj_conf = {
            'blob_prefix': 'test/',
            'file_names': ['file1.txt', 'file2.txt'],
            'output_data_object_dir': '/tmp'
        }
        
        # Mock the connector's download_files_from_bucket method
        with patch.object(extractor.s3_connector, 'download_files_from_bucket') as mock_download:
            mock_download.return_value = ['/tmp/file1.txt', '/tmp/file2.txt']
            
            # Execute
            result = extractor.extract_to(extractor_obj_conf)
            
            # Verify
            mock_download.assert_called_once_with(
                bucket_name='test-bucket',
                blob_prefix='test/',
                file_names=['file1.txt', 'file2.txt'],
                destination_dpath='/tmp'
            )
            assert result == ['/tmp/file1.txt', '/tmp/file2.txt']


class TestS3Loader:
    @pytest.fixture
    def pipeline_mock(self):
        pipeline = MagicMock(spec=Pipeline)
        pipeline.target_attr = {
            'aws_bucket_name': 'test-bucket',
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'aws_region': 'us-east-1'
        }
        return pipeline

    @pytest.fixture
    def mock_aws_session(self):
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        return mock_session

    @pytest.fixture
    def loader(self, pipeline_mock, mock_aws_session):
        with patch('boto3.session.Session', return_value=mock_aws_session):
            return S3Loader(pipeline_mock)

    def test_load_from(self, loader):
        # Setup
        loader_obj_conf = {
            'blob_prefix': 'test/',
            'file_list': ['/tmp/file1.txt', '/tmp/file2.txt']
        }
        
        # Mock the connector's methods
        with patch.object(loader.s3_connector, 'delete_object_in_bucket') as mock_delete, \
             patch.object(loader.s3_connector, 'upload_files_to_bucket') as mock_upload:
            
            # Execute
            loader.load_from(loader_obj_conf)
            
            # Verify
            mock_delete.assert_called_once_with('test-bucket', 'test/')
            mock_upload.assert_called_once_with('test-bucket', 'test/', ['/tmp/file1.txt', '/tmp/file2.txt']) 