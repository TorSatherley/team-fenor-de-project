import os
import json
import pytest
import boto3
from datetime import datetime, date
from unittest.mock import MagicMock, Mock, patch
from src.lambda_extract import (
    get_secret,
    create_conn,
    close_db,
    get_rows_and_columns_from_table,
    write_table_to_s3,
    log_file,
    lambda_handler,
)


get_secret
def test_get_secret():
    mock_secret = {
        "username": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "port": "5432",
        "engine": "postgres",
        "host": "test_host",
    }
    
    with patch("boto3.client") as mock_boto3_client, patch.dict("os.environ", {"SECRET_NAME": "test_secret"}):
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(mock_secret)
        }
        mock_boto3_client.return_value = mock_client
        secret = get_secret(mock_client)
    assert secret == {
        "secret": {
            "username": "test_user",
            "password": "test_password",
            "dbname": "test_db",
            "port": "5432",
            "engine": "postgres",
            "host": "test_host",
        }
    }


def test_create_conn():
    mock_get_secret_return_value = {
        "secret": {
            "username": "test_user",
            "password": "test_password",
            "dbname": "test_db",
            "port": "5432",
            "engine": "postgres",
            "host": "test_host",
        }
    }
    with patch("src.lambda_extract.get_secret") as mock_get_secret, \
        patch ("src.lambda_extract.Connection") as mock_Connection:
        mock_get_secret.return_value = mock_get_secret_return_value
        mock_conn = Mock()
        mock_Connection.return_value = mock_conn
        conn = create_conn()
    assert conn == mock_conn




# close_db
def test_close_db():
    mock_conn = Mock()
    close_db(mock_conn)
    mock_conn.close.assert_called_once()



# get_rows_and_columns_from_table
def test_get_rows_and_columns_from_table():
    # Arrange
    mock_conn = Mock()
    mock_conn.run.side_effect = [
        [["col_1"], ["col_2"]],
        [["val1", "val2"], ["val3", "val4"]]
    ]
    mock_table = "test_table"

    # Act
    rows, columns = get_rows_and_columns_from_table(mock_conn, mock_table)

    # Assert
    assert columns == ["col_1", "col_2"]
    assert rows == [["val1", "val2"], ["val3", "val4"]]


# @pytest.fixture
# @patch("src.lambda_extract.create_conn")
# def mock_totesys_connection(mock_conn):
#     mock_conn = Mock()
#     return mock_conn

# patch the namespaces of all of the util functions that lamba handler uses
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
    capsys
    ):

    # ASSEMBLE:

    # create some table names that will serve as the return from create_conn
    # mock create conn return value
    mock_conn = MagicMock()
    mock_conn.run.return_value = [("address",), ("staff",)]

    mock_create_conn.return_value = mock_conn
    # print('mock_create_conn return_value:', mock_create_conn.run.return_value)
    # print(mock_conn().run(), '<<<< create_conn().run()')
    # print("Mock return value for create_conn.run:", mock_conn.run())



    # mock the output of get_rows_and_columns_from_tables in the handler for loop. This will be called twice in this mock,
    # once for 'addess' and 'staff'. Side_effect enables mocking of multiple calls to a function. 
    mock_get_rows_columns.side_effect = [
        ( # address table
            [
                [1, "123 Northcode Road", "Leeds"],
                [2, "66 Fenor Drive", "Manchester"]
            ],
            ["address_ID", "address", "city"]
        ),
        ( # staff table
            [
                [1, "Connor", "Creed", "creedmoney@gmail.com"],
                [2, "Brendan", "Corbett", "yeaaboii@hotmail.co.uk"]
            ],
            ["staff_ID", "first_name", "last_name", "email"]
        )
    ]

    # mock the two return values from write_table_to_s3_client in the for loop for our two table names
    # this util returns the key at which the table has been stored in s3
    mock_write_table_to_s3.side_effect = [
        "data/2025/03/28_11-15-28/address.json",
        "data/2025/03/28_11-15-31/staff.json"
        ]

    # define an empty event and a null context to pass into the lamda handler
    event = {}
    context = None

    # create a mock s3 client to pass into write_table_to_s3 and log_file
    # this could just be an empty mock() as at won't actually be interacting with boto3 at all
    mock_s3_client = boto3.client("s3")

    # ACT:
    mock_conn = MagicMock()
    mock_conn.run.return_value = [("address",), ("staff",)]

    mock_create_conn.return_value = mock_conn


    with patch("src.lambda_extract.s3_client", mock_s3_client):
        with patch("src.lambda_extract.datetime") as mock_datetime:
            test_date = date(2025, 7, 23)
            mock_datetime.today.return_value = test_date
            # .today.strftime
            mock_datetime.side_effect = lambda *args, **kw: datetime.strftime(*args, **kw)
            
            # call the handler with the above patches inplace
            result = lambda_handler(event, context)

    # ASSERT:

    assert result == {"message": "Batch extraction job completed"}

    mock_create_conn.assert_called_once()
    
    mock_get_rows_columns.assert_any_call(mock_conn, "address")
    mock_get_rows_columns.assert_any_call(mock_conn, "staff")

    mock_write_table_to_s3.assert_any_call(mock_s3_client, 'address', [[1, '123 Northcode Road', 'Leeds'], [2, '66 Fenor Drive', 'Manchester']], ["address_ID", "address", "city"])
    mock_write_table_to_s3.assert_any_call(mock_s3_client, 'staff', [[1, 'Connor', 'Creed', 'creedmoney@gmail.com'], [2, 'Brendan', 'Corbett', 'yeaaboii@hotmail.co.uk']], ['staff_ID', 'first_name', 'last_name', 'email'])

    mock_log_file.assert_called_once()
    mock_log_file.assert_called_with(mock_s3_client, ["data/2025/03/28_11-15-28/address.json", "data/2025/03/28_11-15-31/staff.json"])

    mock_close_db.assert_called_once()
    mock_close_db.assert_called_with(mock_conn)

    captured = capsys.readouterr()
    assert captured.out == f"Log: Batch extraction completed - {test_date.strftime('%Y-%m-%d_%H-%M-%S')}\n"




