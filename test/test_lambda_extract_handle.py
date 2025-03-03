import os
import json
import botocore.exceptions
import pytest
import boto3
from moto import mock_aws
from dotenv import load_dotenv
import pg8000.native
import botocore
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
        "host": "localhost",
        "port": "test_5432",
        "engine": "test_engine",
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


class TestGetSecret:
    @pytest.mark.it("Secret is retrieved")
    def test_get_secret_retrieves_secret(self, mock_secrets_client, mock_secret):
        mock_secrets_client.create_secret(
            Name="test-secret", SecretString=json.dumps(mock_secret)
        )
        response = get_secret(mock_secrets_client)

        assert response == mock_secret

    @pytest.mark.it("Getting a secret returns a client error")
    def test_get_secret_returns_client_error(self, mock_secrets_client):
        with pytest.raises(botocore.exceptions.ClientError):
            get_secret(client=mock_secrets_client)

    @pytest.mark.it("Error is shown if secret does not exist")
    def test_secret_does_not_exist(self, mock_secrets_client):
        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            result = get_secret(mock_secrets_client)
            mock_secrets_client.get_secret_value(SecretId="not-here")
            assert "Secrets Manager can't find the specified secret" in str(
                excinfo["Error"]["Message"]
            )
            assert result == "Error: Secrets Manager can't find the specified secret."

    @pytest.mark.it("Environment variable is wrong or does not exist for secret")
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"SECRET_NAME": "wrong_env"})
    def test_wrong_or_no_dotenv_variable(self, mock_dotenv, mock_secrets_client):

        load_dotenv()

        secret_id = os.getenv("SECRET_NAME")
        assert secret_id == "wrong_env"

        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            result = get_secret(mock_secrets_client)
            assert "Secrets Manager can't find the specified secret" in str(
                excinfo["Error"]["Message"]
            )
            assert result == "Error: Secrets Manager can't find the specified secret."


class TestConnection:
    @pytest.mark.it("Connection to database is established and retrieves data")
    @patch("src.lambda_extract.get_secret")
    def test_connection_to_database_is_established(
        self, mock_get_secret, mock_secret, mock_totesys_connection
    ):
        mock_get_secret.return_value = mock_secret
        mock_totesys_connection.run = Mock(
            side_effect=[{"sales_order_id": 2, "unit_price": 3.94}]
        )
        result = mock_totesys_connection.run(
            "SELECT sales_order_id, unit_price FROM sales"
        )
        assert result == {"sales_order_id": 2, "unit_price": 3.94}

    @pytest.mark.it(
        "Connection to database is established and retrieves data - Vincent"
    )
    @patch("src.lambda_extract.get_secret")
    @patch("src.lambda_extract.Connection")
    def test_create_conn(
        self, mock_Connection, mock_get_secret, mock_secret, mock_totesys_connection
    ):
        mock_get_secret.return_value = mock_secret
        mock_Connection.return_value = mock_totesys_connection

        conn = create_conn()

        mock_get_secret.assert_called_once()  # "get_secret" is called
        mock_Connection.assert_called_once()  # "Connection" is called
        assert (
            conn == mock_totesys_connection
        )  # "Connection" output is returned by create_conn()

    @pytest.mark.it("Connection to database is successfully closed")
    def test_connection_to_database_is_successfully_closed_with_close_db(
        self, mock_totesys_connection
    ):
        close_db(mock_totesys_connection)
        mock_totesys_connection.close.assert_called_once()


class TestRowsAndColumns:
    @pytest.mark.it("Rows are retreieved from database")
    @patch("src.lambda_extract.get_secret")
    def test_rows_are_retrieved_from_connection_to_database(
        self, mock_get_secret, mock_secret, mock_totesys_connection
    ):
        mock_get_secret.return_value = mock_secret
        mock_totesys_connection.run = Mock(
            side_effect=[
                {"sales_order_id": 2, "unit_price": 3.94},
                {"sales_order_id": 3, "unit_price": 4.20},
            ]
        )
        result = mock_totesys_connection.run("SELECT sales_order_id FROM sales")

        assert result == {"sales_order_id": 2, "unit_price": 3.94}, {
            "sales_order_id": 3,
            "unit_price": 4.20,
        }

    @pytest.mark.it("Column names are retreieved from database")
    @patch("src.lambda_extract.get_secret")
    def test_columns_are_retrieved_from_connection_to_database(
        self, mock_get_secret, mock_secret, mock_totesys_connection
    ):
        mock_get_secret.return_value = mock_secret
        mock_totesys_connection.run = Mock(
            side_effect=[["sales_order_id", "unit_price"]]
        )
        result = mock_totesys_connection.run(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = sales"
        )
        assert result == ["sales_order_id", "unit_price"]

    def test_get_rows_and_columns_from_table(self, mock_totesys_connection):
        # Arrange
        mock_totesys_connection.run.side_effect = [
            [["col_1"], ["col_2"]],
            [["val1", "val2"], ["val3", "val4"]],
        ]
        mock_table = "test_table"

        # Act
        rows, columns = get_rows_and_columns_from_table(
            mock_totesys_connection, mock_table
        )

        # Assert
        assert mock_totesys_connection.run.call_count == 2
        assert rows == [["val1", "val2"], ["val3", "val4"]]
        assert columns == ["col_1", "col_2"]


class TestWriteTableToS3:
    @patch("src.lambda_extract.pd.DataFrame")
    @patch("src.lambda_extract.datetime")
    def test_write_table_to_s3(self, mock_datetime, mock_pd_DataFrame):
        mock_s3_client = Mock()
        mock_table = "test_table"
        mock_rows = [["val1", "val2"], ["val3", "val4"]]
        mock_columns = ["col_1", "col_2"]

        mock_df = Mock()
        mock_pd_DataFrame.return_value = mock_df
        mock_df.to_json.return_value = Mock()
        mock_datetime.now.return_value.strftime = Mock(
            side_effect=["14-46", "2001-09-11"]
        )
        mock_s3_client.put_object.return_value = Mock()

        key = write_table_to_s3(mock_s3_client, mock_table, mock_rows, mock_columns)

        mock_pd_DataFrame.assert_called_once_with(data=mock_rows, columns=mock_columns)
        mock_df.to_json.assert_called_once_with(
            orient="records", lines=False, date_format="iso"
        )
        mock_datetime.now.return_value.strftime.assert_any_call("%H%M")
        mock_datetime.now.return_value.strftime.assert_any_call("%Y%m%d")
        mock_s3_client.put_object.assert_called_with(
            Bucket="totesys-ingestion-zone-fenor",
            Key=key,
            Body=mock_df.to_json.return_value,
        )
        assert key == "data/2001-09-11_14-46/test_table.json"


class TestLogFile:
    @patch("src.lambda_extract.datetime")
    def test_log_file(self, mock_datetime):
        mock_s3_client = Mock()
        mock_keys = ["key_1", "key_2", "key_3", "key_4", "key_5"]

        mock_datetime.now.return_value = "2001-09-11_14-46-00"
        mock_datetime.today.return_value.strftime.return_value = "2001-09-11_14-46-00"
        mock_s3_client.put_object.return_value = Mock()

        message = log_file(mock_s3_client, mock_keys)

        mock_datetime.now.assert_any_call()
        mock_datetime.today.return_value.strftime.assert_any_call("%Y-%m-%d_%H-%M-%S")
        mock_s3_client.put_object.assert_called_once()
        assert message == {
            "message": "Files Processed: Batch Lambda Transform complete"
        }


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
    capsys,
):

    # ASSEMBLE:

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

    # ACT:

    with patch("src.lambda_extract.s3_client", mock_s3_client):
        with patch("src.lambda_extract.datetime") as mock_datetime:
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
        "address",
        [[1, "123 Northcode Road", "Leeds"], [2, "66 Fenor Drive", "Manchester"]],
        ["address_ID", "address", "city"],
    )
    mock_write_table_to_s3.assert_any_call(
        mock_s3_client,
        "staff",
        [
            [1, "Connor", "Creed", "creedmoney@gmail.com"],
            [2, "Brendan", "Corbett", "yeaaboii@hotmail.co.uk"],
        ],
        ["staff_ID", "first_name", "last_name", "email"],
    )

    mock_log_file.assert_called_once()
    mock_log_file.assert_called_with(
        mock_s3_client,
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


# def test_lambda_handler_for_client_error():
#     event = {}
#     context = None
#     with pytest.raises(botocore.exceptions.ClientError):
#         lambda_handler(event, context)


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
    result = lambda_handler(event, context)
    print(result)
    assert (
        "An error occurred (AccessDenied) when calling the PutObject operation: Access Denied"
        in result["error"]
    )
