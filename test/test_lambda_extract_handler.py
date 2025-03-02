import boto3
import os
import json
from datetime import datetime
from moto import mock_aws
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock
from src.lambda_extract import lambda_handler, db_connection, get_secret

TEST_BUCKET_NAME = "test-totesys"


class TestGetSecret:
    @mock_aws
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"SECRET_NAME": "totesys-key"})
    def test_get_secret_value_is_found(self, mock_dotenv, mock_secrets_client):

        conn = boto3.client('secretsmanager', region_name="eu-west-2")
        conn.create_secret(Name="totesys-key", SecretString=json.dumps({
            "dbname": "test_db",
            "username": "test_user",
            "password": "test_pass",
            "host": "test_host"
        }))

        secret_value = get_secret()
        assert secret_value == {
            'dbname': 'test_db',
            'username': 'test_user',
            'password': 'test_pass',
            'host': 'test_host'}

    @mock_aws
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"WRONG_ENV": "does-not-exist"})
    def test_get_secret_error_when_no_env_variable(
            self, mock_dotenv, mock_secrets_client):

        secret_value = get_secret()

        assert "can't find the specified secret" in secret_value["error"]

    @mock_aws
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"SECRET_NAME": "totesys-test-id"})
    def test_get_secret_not_found(self, mock_dotenv):

        result = get_secret()
        print(result)
        assert "can't find the specified secret" in result["error"]

    @mock_aws
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"SECRET_NAME": "totesys-test-id"})
    def test_failed_to_retrieve_secret_for_credentials(
            self, mock_dotenv, mock_secrets_client):

        result = get_secret()

        assert "can't find the specified secret" in result['error']

    @mock_aws
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"secret": "secret"})
    def test_get_secret_does_not_find_secret_id(
            self, mock_dotenv, mock_secrets_client):

        result = get_secret()
        assert "can't find the specified secret" in result['error']


class TestConnection:
    @patch('src.lambda_extract.get_secret')
    @patch('src.lambda_extract.Connection')
    def test_mock_connection_is_being_made_with_mock_credentials(
            self, mock_connection, mock_get_secret):
        # Mock get_secret with fake credentials
        mock_get_secret.return_value = {
            "dbname": "test_db",
            "username": "test_user",
            "password": "test_pass",
            "host": "test_host"
        }

        # Mock connection object - to be returned when db_connection is called
        mock_connection_instance = MagicMock()
        mock_connection.return_value = mock_connection_instance

        result = db_connection()

        mock_connection.assert_called_once_with(
            database='test_db',
            user='test_user',
            password='test_pass',
            host='test_host'
        )

    @patch('src.lambda_extract.get_secret')
    @patch('src.lambda_extract.Connection')
    def test_db_connection_failure(self, mock_connection, mock_get_secret):
        mock_get_secret.return_value = {
            "dbname": "test_db",
        }
        result = db_connection()

        assert result == {"error": "Missing credentials from DB connection"}


class TestS3:
    @patch('src.lambda_extract.get_secret')
    @patch('src.lambda_extract.db_connection')
    def test_objects_are_added_to_s3_with_lambda_handler(
            self, mock_connection, mock_get_secret, mock_db_data, mock_s3):
        mock_connection.return_value = mock_db_data
        mock_s3.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'})

        result = lambda_handler({}, {})

        assert "Batch extraction job completed" in result['message']

    @patch('src.lambda_extract.get_secret')
    @patch('src.lambda_extract.db_connection')
    def test_data_key_and_json_being_added_to_s3_by_lambda(
            self, mock_connection, mock_get_secret, mock_db_data, mock_s3):
        mock_connection.return_value = mock_db_data
        mock_s3.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'})

        result = lambda_handler({}, {})
        list_s3 = mock_s3.list_objects(Bucket=TEST_BUCKET_NAME)['Contents']
        mock_s3_keys = [file['Key'] for file in list_s3]

        assert any("data/" in key for key in mock_s3_keys)
        assert any(".jsonl" in key for key in mock_s3_keys)

    @patch('src.lambda_extract.get_secret')
    @patch('src.lambda_extract.db_connection')
    def test_log_file_being_added_to_s3_by_lambda(
            self, mock_connection, mock_get_secret, mock_db_data, mock_s3):
        mock_connection.return_value = mock_db_data
        mock_s3.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'})

        result = lambda_handler({}, {})
        list_s3 = mock_s3.list_objects(Bucket=TEST_BUCKET_NAME)['Contents']
        mock_s3_keys = [file['Key'] for file in list_s3]
        print(mock_s3_keys)

        assert any("logs/" in key for key in mock_s3_keys)
        assert any(".log" in key for key in mock_s3_keys)
        # assert ".log" in any(mock_s3_keys)

    @patch('src.lambda_extract.get_secret')
    @patch('src.lambda_extract.db_connection')
    def test_s3_bucket_not_found(
            self, mock_connection, mock_get_secret, mock_s3, mock_db_data):
        # Simulate S3 bucket not found
        mock_s3.create_bucket(
            Bucket="does-not-exist",
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'})

        bucket_name = "does not exist"

        result = lambda_handler({}, {})

        err = {
            'error': 'An error occurred (NoSuchBucket) when calling the PutObject operation: The specified bucket does not exist'}
        assert result == err


class TestData:
    @patch('src.lambda_extract.get_secret')
    @patch('src.lambda_extract.db_connection')
    def test_data_can_be_read_from_file_in_s3_bucket_as_json(
            self, mock_connection, mock_get_secret, mock_db_data, mock_s3):
        mock_connection.return_value = mock_db_data
        mock_s3.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'})

        filepath = 'test/data/address.jsonl'
        table_name = Path(filepath).stem

        result = lambda_handler({}, {})

        list_s3 = mock_s3.list_objects_v2(Bucket=TEST_BUCKET_NAME)['Contents']
        mock_s3_keys = [file['Key'] for file in list_s3]

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        response = mock_s3.get_object(
            Bucket="test-totesys",
            Key=f"data/{timestamp}/{table_name}.jsonl")
        file_content = response["Body"].read().decode("utf-8")

        print("FILE CONTENT", file_content)

        assert any(
            f"data/{timestamp}/{table_name}" in key for key in mock_s3_keys)
        assert any(".jsonl" in key for key in mock_s3_keys)
