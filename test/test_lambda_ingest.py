import pytest
import boto3
from moto import mock_aws
import io
import csv
import os
from unittest.mock import patch, MagicMock
import pg8000.native
from lambda_ingest import connect_to_db, close_db_connection, get_rows_and_columns_from_table, write_table_to_s3


@pytest.fixture
def mock_connection():
    mock_conn = MagicMock()
    mock_conn.run.return_value = [(1, "Brendan"), (2, "Connor"), (3, "Fabio"), (4, "Tor"), (5, "Vincent")]
    mock_conn.columns = [{"name": "id"}, {"name": "name"}]
    return mock_conn

@patch("pg8000.native.Connection")
def test_connect_to_db(mock_pg_connection):
    mock_pg_connection.return_value = MagicMock()

    DB_USER="test_user"
    DB_PASSWORD="test_password"
    DB_NAME="test_db"
    DB_HOST="test_host"
    DB_PORT=5432
    
    conn = connect_to_db(DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT)

    mock_pg_connection.assert_called_once_with(
        user="test_user",
        password="test_password",
        database="test_db",
        host="test_host",
        port=5432
    )
    assert conn == mock_pg_connection.return_value

def test_close_db_connection(mock_connection):
    close_db_connection(mock_connection)
    mock_connection.close.assert_called_once()

def test_get_rows_and_columns_from_table(mock_connection):
    table_name = "users"
    
    rows, columns = get_rows_and_columns_from_table(mock_connection, table_name)

    mock_connection.run.assert_called_once_with(f"SELECT * FROM {table_name}")
    assert rows == [(1, "Brendan"), (2, "Connor"), (3, "Fabio"), (4, "Tor"), (5, "Vincent")]
    assert columns == ["id", "name"]

@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1") # I had to use us-east-1 for it to work ???
        client.create_bucket(Bucket="test-bucket")
        yield client

def test_write_table_to_s3(s3_client):
    rows = [(1, "Brendan"), (2, "Connor"), (3, "Fabio"), (4, "Tor"), (5, "Vincent")]
    cols = ["id", "name"]
    bucket = "test-bucket"
    key = "test.csv"

    write_table_to_s3(s3_client, rows, cols, bucket, key)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    
    csv_reader = csv.reader(io.StringIO(content))
    output = list(csv_reader)

    assert output == [["id", "name"], ["1", "Brendan"], ["2", "Connor"], ["3", "Fabio"], ["4", "Tor"], ["5", "Vincent"]]
