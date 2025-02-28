import os
import json
from datetime import datetime
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


# get_secret
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

# create_conn
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
    with patch("src.lambda_ingest_dummy.get_secret") as mock_get_secret, \
        patch ("src.lambda_ingest_dummy.Connection") as mock_Connection:
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


@patch("lambda_extract.create_conn")
@patch("lambda_extract.get_rows_and_columns_from_table")
@patch("lambda_extract.write_table_to_s3")
@patch("lambda_extract.log_file")
@patch("lambda_extract.close_db")
def test_lambda_handler():
    pass