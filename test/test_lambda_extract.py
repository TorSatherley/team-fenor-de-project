import os
import json
import botocore.exceptions
import pytest
import boto3
from moto import mock_aws
from dotenv import load_dotenv
import pg8000.native
import botocore
from botocore.exceptions import ClientError
from datetime import datetime, date
from unittest.mock import MagicMock, Mock, patch
from src.lambda_extract import (
    lambda_handler,
)


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
def mock_conn():
    """mock conn for conn.run"""
    mock_conn = MagicMock()
    mock_conn.run.return_value = [("address",), ("staff",)]
    return mock_conn


@pytest.fixture
def mock_last_checked_populated():
    mock_date = date(2000, 1, 1)
    return mock_date


@pytest.fixture
def rows_columns_recentcheck(mock_last_checked_populated):
    """fixture to mock the output of get_rows_and_columns_from_tables in the handler for loop.
    This will be called twice in this mock, once for 'address' and 'staff'."""
    return [
        (  # address table
            [[1, "123 Northcode Road", "Leeds"], [2, "66 Fenor Drive", "Manchester"]],
            ["address_ID", "address", "city"],
            mock_last_checked_populated,
        ),
        (  # staff table
            [
                [1, "Connor", "Creed", "creedmoney@gmail.com"],
                [2, "Brendan", "Corbett", "yeaaboii@hotmail.co.uk"],
            ],
            ["staff_ID", "first_name", "last_name", "email"],
            mock_last_checked_populated,
        ),
    ]


@pytest.fixture
def s3_keys():
    """mocks the result of write_table_to_s3"""
    return [
        "data/2025/03/28_11-15-28/address.json",
        "data/2025/03/28_11-15-31/staff.json",
    ]


@patch("src.lambda_extract.secret_name")
@patch("src.lambda_extract.sm_client")
@patch("src.lambda_extract.bucket_name")
@patch("src.lambda_extract.s3_client")
@patch("src.lambda_extract.get_secret")
@patch("src.lambda_extract.create_conn")
@patch("src.lambda_extract.get_rows_and_columns_from_table")
@patch("src.lambda_extract.write_table_to_s3")
@patch("src.lambda_extract.log_file")
@patch("src.lambda_extract.close_db")
def test_lambda_handler(
    mock_close_db,
    mock_log_file,
    mock_write_table_to_s3,
    mock_get_rows_columns,
    mock_create_conn,
    mock_get_secret,
    mock_s3_client,
    mock_bucket_name,
    mock_sm_client,
    mock_secret_name,
    capsys,
    rows_columns_recentcheck,
    s3_keys,
    mock_conn,
    mock_last_checked_populated,
):
    """test that the handler is calling all of the utils with the correct arguments the correct number
    of times to ensure proper integration."""
    # ARRANGE:
    mock_get_secret.return_value = {"dbname": "test_db", "user": "test_user"}
    mock_create_conn.return_value = mock_conn
    mock_get_rows_columns.side_effect = rows_columns_recentcheck
    mock_write_table_to_s3.side_effect = s3_keys

    event = {}
    context = None
    # ACT:
    with patch("src.lambda_extract.datetime") as mock_datetime:
        test_date = date(2025, 7, 23)
        mock_datetime.today.return_value = test_date
        mock_datetime.side_effect = lambda *args, **kw: datetime.strftime(*args, **kw)

        result = lambda_handler(event, context)
    # ASSERT:
    assert result == {"message": "Batch extraction job completed"}
    mock_get_secret.assert_called_once_with(mock_sm_client, mock_secret_name)
    mock_create_conn.assert_called_once_with({"dbname": "test_db", "user": "test_user"})
    mock_get_rows_columns.assert_any_call(
        mock_conn, "address", mock_last_checked_populated
    )
    mock_get_rows_columns.assert_any_call(
        mock_conn, "staff", mock_last_checked_populated
    )
    mock_write_table_to_s3.assert_any_call(
        mock_s3_client,
        mock_bucket_name,
        "address",
        [[1, "123 Northcode Road", "Leeds"], [2, "66 Fenor Drive", "Manchester"]],
        ["address_ID", "address", "city"],
        "20250723_000000",
    )
    mock_write_table_to_s3.assert_any_call(
        mock_s3_client,
        mock_bucket_name,
        "staff",
        [
            [1, "Connor", "Creed", "creedmoney@gmail.com"],
            [2, "Brendan", "Corbett", "yeaaboii@hotmail.co.uk"],
        ],
        ["staff_ID", "first_name", "last_name", "email"],
        "20250723_000000",
    )
    mock_log_file.assert_called_once_with(
        mock_s3_client,
        mock_bucket_name,
        s3_keys,
    )
    mock_close_db.assert_called_once_with(mock_conn)
    captured = capsys.readouterr()
    assert (
        captured.out
        == f"Log: Batch extraction completed - {test_date.strftime('%Y-%m-%d_%H-%M-%S')}\n"
    )


@patch("src.lambda_extract.get_rows_and_columns_from_table")
@patch("src.lambda_extract.create_conn")
def test_for_put_s3_error(
    mock_create_conn, mock_get_rows_columns, rows_columns_recentcheck, mock_conn
):
    # ARRANGE
    mock_create_conn.return_value = mock_conn
    mock_get_rows_columns.side_effect = rows_columns_recentcheck
    event = {}
    context = None
    # ACT
    result = lambda_handler(event, context)
    # ASSERT
    assert "Batch extraction job failed" in result["message"]
