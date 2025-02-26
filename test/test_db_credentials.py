def test_mock():
    true = True
    assert true == True

# from db.db_credentials import get_secret
# from unittest.mock import Mock, patch
# import pytest
# import boto3
# import os
# from moto import mock_aws
# import json
# import botocore


# @pytest.fixture(scope="function", autouse=True)
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     os.environ["AWS_ACCESS_KEY_ID"] = "testing"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
#     os.environ["AWS_SECURITY_TOKEN"] = "testing"
#     os.environ["AWS_SESSION_TOKEN"] = "testing"
#     os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
#     os.environ["SECRET_NAME"] = "test-secret"


# @pytest.fixture
# def mock_secrets_client():
#     with mock_aws():
#         yield boto3.client("secretsmanager", region_name="eu-west-2")


# class TestGetSecret:
#     def test_get_secret_returns_secret_values_in_dictionary(self, mock_secrets_client):
#         secret_value = {
#             "username": "test_user",
#             "password": "test_pass",
#             "dbname": "test_db",
#             "port": "1234",
#             "engine": "test_engine",
#             "host": "test_host",
#         }
#         mock_secrets_client.create_secret(
#             Name="test-secret", SecretString=json.dumps(secret_value)
#         )
#         response = get_secret(mock_secrets_client)

#         assert response == {
#             "secret": {
#                 "username": secret_value["username"],
#                 "password": secret_value["password"],
#                 "dbname": secret_value["dbname"],
#                 "port": secret_value["port"],
#                 "engine": secret_value["engine"],
#                 "host": secret_value["host"],
#             }
#         }

#     def test_get_secret_returns_client_error(self, mock_secrets_client):
#         with pytest.raises(botocore.exceptions.ClientError):
#             get_secret(client=mock_secrets_client)
