import pytest
import os
import boto3
from moto import mock_aws
from unittest.mock import Mock, patch
import pg8000.native

@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    os.environ["SECRET_NAME"] = "test-secret"


@pytest.fixture
@patch("src.lambda_extract.create_conn")
def mock_totesys_connection(mock_conn):
    return mock_conn





@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")

@pytest.fixture(scope="function")
def mock_empty_s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")

@pytest.fixture
def mock_secret(mock_empty_s3):
    return {
        "dbname": "test_db",
        "username": "test_user",
        "password": "test_password",
        "host": "localhost"
        }


@pytest.fixture
def mock_secrets_client():
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")



BUCKET_NAME = "test_bucket"
FILES = [
    "address",
    "counterparty",
    "currency",
    "department",
    "design",
    "payment_type",
    "payment",
    "purchase_order",
    "sales_order",
    "staff",
    "transaction",
]

@pytest.fixture
def bucket(s3):
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    for file in FILES:
            file_path = f"test/test_data/{file}.json"
            s3_key = f"data/2025_02_27__1030/{file}.json"

            # Read JSON file and upload to S3
            with open(file_path, "r") as json_file:
                text_to_write = json_file.read()
                s3.put_object(Body=text_to_write, Bucket=BUCKET_NAME, Key=s3_key)
