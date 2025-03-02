import pytest
import os
import boto3
import json
from pathlib import Path
from moto import mock_aws
from unittest.mock import MagicMock

TEST_BUCKET_NAME = "test-totesys"


@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    os.environ["SECRET_NAME"] = "test-secret"


@pytest.fixture(scope="function")
def mock_s3(aws_credentials):
    with mock_aws():
        s3_client = boto3.client('s3')
        yield s3_client


@pytest.fixture
def mock_secrets_client():
    with mock_aws():
        secrets_client = boto3.client(
            "secretsmanager", region_name="eu-west-2")
        yield secrets_client


def json_to_tuples(filepath, table_name):
    with open(filepath) as json_file:
        json_content = json.load(json_file)

    column_names = [column for column in json_content[0].keys()]

    data = [[(f"{table_name}",)], [(f"{column}",)
                                   for column in json_content[0].keys()]]

    rows = []

    for idx, column in enumerate(column_names):
        row = tuple(f"{row}" for row in json_content[idx].values())
        rows.append(row)

    data.append(rows)
    return data


filepath = 'test/data/address.jsonl'
table_name = Path(filepath).stem
data = json_to_tuples(filepath, table_name)


@pytest.fixture
def mock_db_data():
    mock_db_table = MagicMock()
    mock_db_table.run.side_effect = data
    return mock_db_table
