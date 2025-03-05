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



# patch the namespaces of all of the util functions that lamba handler uses
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
    capsys,
):

    # ASSEMBLE:

    mock_get_secret.return_value = {"dbname": "test_db", "user": "test_user"}
    # create some table names that will serve as the return from create_conn
    # mock create conn return value
    mock_conn = MagicMock()
    mock_conn.run.return_value = [("address",), ("staff",)]

    mock_create_conn.return_value = mock_conn

    # mock the output of get_rows_and_columns_from_tables in the handler for loop. This will be called twice in this mock,
    # once for 'addess' and 'staff'. Side_effect enables mocking of multiple calls to a function.
    mock_get_rows_columns.side_effect = [
        (  # address table
            [[1, "123 Northcode Road", "Leeds"], [2, "66 Fenor Drive", "Manchester"]],
            ["address_ID", "address", "city"],
        ),
        (  # staff table
            [
                [1, "Connor", "Creed", "creedmoney@gmail.com"],
                [2, "Brendan", "Corbett", "yeaaboii@hotmail.co.uk"],
            ],
            ["staff_ID", "first_name", "last_name", "email"],
        ),
    ]

    # mock the two return values from write_table_to_s3_client in the for loop for our two table names
    # this util returns the key at which the table has been stored in s3
    mock_write_table_to_s3.side_effect = [
        "data/2025/03/28_11-15-28/address.json",
        "data/2025/03/28_11-15-31/staff.json",
    ]

    # define an empty event and a null context to pass into the lamda handler
    event = {}
    context = None

    # create a mock s3 client to pass into write_table_to_s3 and log_file
    # this could just be an empty mock() as at won't actually be interacting with boto3 at all
    mock_s3_client = boto3.client("s3")

    bucket_name = "test-bucket"

    # ACT:
    
    with patch("src.lambda_extract.s3_client", mock_s3_client), \
         patch("src.lambda_extract.bucket_name", bucket_name), \
         patch("src.lambda_extract.datetime") as mock_datetime:
            test_date = date(2025, 7, 23)
            mock_datetime.today.return_value = test_date
            mock_datetime.side_effect = lambda *args, **kw: datetime.strftime(
                *args, **kw
            )

            # call the handler with the above patches inplace
            result = lambda_handler(event, context)

    # ASSERT:

    assert result == {"message": "Batch extraction job completed"}

    mock_create_conn.assert_called_once()

    mock_get_rows_columns.assert_any_call(mock_conn, "address")
    mock_get_rows_columns.assert_any_call(mock_conn, "staff")
    mock_write_table_to_s3.assert_any_call(
        mock_s3_client,
        bucket_name,
        "address",
        [[1, "123 Northcode Road", "Leeds"], [2, "66 Fenor Drive", "Manchester"]],
        ["address_ID", "address", "city"],
        '20250723_000000',
    )
    mock_write_table_to_s3.assert_any_call(
        mock_s3_client,
        bucket_name,
        "staff",
        [
            [1, "Connor", "Creed", "creedmoney@gmail.com"],
            [2, "Brendan", "Corbett", "yeaaboii@hotmail.co.uk"],
        ],
        ["staff_ID", "first_name", "last_name", "email"],
        '20250723_000000',
    )

    mock_log_file.assert_called_once()
    mock_log_file.assert_called_with(
        mock_s3_client,
        bucket_name,
        [
            "data/2025/03/28_11-15-28/address.json",
            "data/2025/03/28_11-15-31/staff.json",
        ],
    )

    mock_close_db.assert_called_once()
    mock_close_db.assert_called_with(mock_conn)

    captured = capsys.readouterr()
    assert (
        captured.out
        == f"Log: Batch extraction completed - {test_date.strftime('%Y-%m-%d_%H-%M-%S')}\n"
    )


@patch("src.lambda_extract.get_rows_and_columns_from_table")
@patch("src.lambda_extract.create_conn")
def test_using_return_for_put_s3_error(mock_create_conn, mock_get_rows_columns):
    mock_conn = MagicMock()
    mock_conn.run.return_value = [("address",), ("staff",)]

    mock_create_conn.return_value = mock_conn

    mock_get_rows_columns.side_effect = [
        (  # address table
            [[1, "123 Northcode Road", "Leeds"], [2, "66 Fenor Drive", "Manchester"]],
            ["address_ID", "address", "city"],
        ),
        (  # staff table
            [
                [1, "Connor", "Creed", "creedmoney@gmail.com"],
                [2, "Brendan", "Corbett", "yeaaboii@hotmail.co.uk"],
            ],
            ["staff_ID", "first_name", "last_name", "email"],
        ),
    ]

    event = {}
    context = None
    print("got to line 437")
    result = lambda_handler(event, context)
    print("got to line 439")
    print(result)
    assert "Batch extraction job failed" in result["message"]

