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
        mock_get_secret_value = MagicMock()
        mock_secrets_client.get_secret_value = mock_get_secret_value

        mock_get_secret_value.return_value = {
            "SecretString": json.dumps(mock_secret)
        }
        mock_secret_name = "test-secret"
        response = get_secret(mock_secrets_client, mock_secret_name)
        assert response == mock_secret

    @pytest.mark.it("Getting a secret returns a client error")
    def test_get_secret_returns_client_error(self, mock_secrets_client):
        mock_secret_name = "test-secret"
        with pytest.raises(botocore.exceptions.ClientError):
            get_secret(mock_secrets_client, mock_secret_name)

    @pytest.mark.it("Error is shown if secret does not exist")
    def test_secret_does_not_exist(self, mock_secrets_client):
        mock_secret_name = "test-secret"
        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            result = get_secret(mock_secrets_client, mock_secret_name)
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
        mock_secret_name = "test-secret"
        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            result = get_secret(mock_secrets_client, mock_secret_name)
            assert "Secrets Manager can't find the specified secret" in str(
                excinfo["Error"]["Message"]
            )
            assert result == "Error: Secrets Manager can't find the specified secret."

    def test_get_secret_no_secret_name(self):
        """Test when secret_name is None."""
        sm_client = MagicMock()
        with pytest.raises(
            ValueError, match="SECRET_NAME environment variable is not set."
        ):
            get_secret(sm_client, None)

    def test_get_secret_invalid_json(self):
        """Test JSONDecodeError when secret string is not valid JSON."""
        sm_client = MagicMock()
        sm_client.get_secret_value.return_value = {"SecretString": "{invalid-json"}

        with pytest.raises(json.JSONDecodeError):
            get_secret(sm_client, "my-secret")

    def test_get_secret_key_error(self):
        """Test KeyError when SecretString is missing from response."""
        sm_client = MagicMock()
        sm_client.get_secret_value.return_value = {}

        with pytest.raises(KeyError):
            get_secret(sm_client, "my-secret")


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

    @pytest.mark.it("Connection to database is established and retrieves data")
    @patch("src.lambda_extract.get_secret")
    @patch("src.lambda_extract.Connection")
    def test_create_conn(
        self, mock_Connection, mock_get_secret, mock_secret, mock_totesys_connection
    ):
        mock_get_secret.return_value = mock_secret
        mock_Connection.return_value = mock_totesys_connection
        with mock_aws():
            mock_sm_client = boto3.client(
                service_name="secretsmanager", region_name="eu-west-2"
            )
            conn = create_conn(mock_sm_client)

        mock_get_secret.assert_called_once()
        mock_Connection.assert_called_once()
        assert conn == mock_totesys_connection

    @pytest.mark.it("Connection to database is successfully closed")
    def test_connection_to_database_is_successfully_closed_with_close_db(
        self, mock_totesys_connection
    ):
        close_db(mock_totesys_connection)
        mock_totesys_connection.close.assert_called_once()

    def test_close_db_exception(self):
        """Test exception when closing the database connection."""
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Close failed")  # Simulate an error

        with pytest.raises(Exception, match="Close failed"):
            close_db(mock_conn)

    @patch("src.lambda_extract.get_secret", autospec=True)
    def test_create_conn_missing_key(self, mock_get_secret):
        """Test KeyError when required fields are missing in credentials."""
        mock_get_secret.return_value = {
            "username": "admin",
            "password": "1234",
            "host": "localhost",
            # Missing "dbname"
        }

        sm_client = MagicMock()

        with pytest.raises(KeyError):
            create_conn(sm_client)

    @patch("src.lambda_extract.get_secret", autospec=True)
    def test_create_conn_aws_client_error(self, mock_get_secret):
        """Test AWS ClientError when retrieving secret fails."""
        mock_get_secret.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Secret not found",
                }
            },
            "GetSecretValue",
        )

        sm_client = MagicMock()

        with pytest.raises(ClientError):
            create_conn(sm_client)

    @patch("src.lambda_extract.Connection", autospec=True)
    @patch("src.lambda_extract.get_secret", autospec=True)
    def test_create_conn_unexpected_exception(self, mock_get_secret, mock_connection):
        """Test unexpected exception during connection creation."""
        mock_get_secret.return_value = {
            "dbname": "test_db",
            "username": "admin",
            "password": "1234",
            "host": "localhost",
        }

        mock_connection.side_effect = Exception("Database connection failed")

        sm_client = MagicMock()

        with pytest.raises(Exception, match="Database connection failed"):
            create_conn(sm_client)


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

    def test_get_rows_and_columns_empty_table(self):
        """Test handling when table exists but has no rows."""
        mock_conn = MagicMock()

        mock_conn.run.side_effect = [
            [("id",), ("name",), ("age",)],
            [],
        ]

        rows, columns = get_rows_and_columns_from_table(mock_conn, "users")

        assert columns == ["id", "name", "age"]
        assert rows == []
        assert mock_conn.run.call_count == 2

    def test_get_rows_and_columns_table_not_found(self):
        """Test handling when the table does not exist."""
        mock_conn = MagicMock()

        mock_conn.run.side_effect = Exception("relation 'non_existent' does not exist")

        rows, columns = get_rows_and_columns_from_table(mock_conn, "non_existent")

        assert columns == []
        assert rows == []
        assert mock_conn.run.call_count == 1

    def test_get_rows_and_columns_unexpected_exception(self):
        """Test handling of an unexpected exception."""
        mock_conn = MagicMock()

        mock_conn.run.side_effect = Exception("Unexpected error")

        rows, columns = get_rows_and_columns_from_table(mock_conn, "users")

        assert columns == []
        assert rows == []
        assert mock_conn.run.call_count == 1


class TestWriteTableToS3:
    @patch("src.lambda_extract.pd.DataFrame")
    @patch("src.lambda_extract.datetime")
    def test_write_table_to_s3(self, mock_datetime, mock_pd_DataFrame):
        mock_s3_client = Mock()
        mock_table = "test_table"
        mock_bucket = "test_bucket"
        mock_rows = [["val1", "val2"], ["val3", "val4"]]
        mock_columns = ["col_1", "col_2"]
        mock_date_and_time = "2001-09-11_14-46-00"

        mock_df = Mock()
        mock_pd_DataFrame.return_value = mock_df
        mock_df.to_json.return_value = Mock()
        mock_datetime.now.return_value.strftime = Mock(
            side_effect=["14-46-00", "2001-09-11"]
        )
        mock_s3_client.put_object.return_value = Mock()

        key = write_table_to_s3(
            mock_s3_client,
            mock_bucket,
            mock_table,
            mock_rows,
            mock_columns,
            mock_date_and_time,
        )

        mock_pd_DataFrame.assert_called_once_with(data=mock_rows, columns=mock_columns)
        mock_df.to_json.assert_called_once_with(
            orient="records", lines=False, date_format="iso"
        )
        # mock_datetime.now.return_value.strftime.assert_any_call("%H%M")
        # mock_datetime.now.return_value.strftime.assert_any_call("%Y%m%d")
        mock_s3_client.put_object.assert_called_with(
            Bucket="test_bucket",
            Key=key,
            Body=mock_df.to_json.return_value,
        )
        assert key == "data/2001-09-11_14-46-00/test_table.jsonl"

    def test_write_table_to_s3_empty_data(self):
        """Test handling when no data is present (should return None)."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        table = "empty_table"
        date_and_time = "2024-03-03"

        assert (
            write_table_to_s3(s3_client, bucket_name, table, [], ["id"], date_and_time)
            is None
        )
        assert (
            write_table_to_s3(
                s3_client, bucket_name, table, [(1, "Alice")], [], date_and_time
            )
            is None
        )

        s3_client.put_object.assert_not_called()

    def test_write_table_to_s3_client_error(self):
        """Test handling of AWS ClientError during S3 upload."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        table = "users"
        rows = [(1, "Alice")]
        columns = ["id", "name"]
        date_and_time = "2024-03-03"

        s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "PutObject"
        )

        assert (
            write_table_to_s3(
                s3_client, bucket_name, table, rows, columns, date_and_time
            )
            is None
        )

    def test_write_table_to_s3_unexpected_exception(self):
        """Test handling of unexpected exceptions."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        table = "users"
        rows = [(1, "Alice")]
        columns = ["id", "name"]
        date_and_time = "2024-03-03"

        with patch(
            "src.lambda_extract.pd.DataFrame", side_effect=Exception("Unexpected error")
        ):
            assert (
                write_table_to_s3(
                    s3_client, bucket_name, table, rows, columns, date_and_time
                )
                is None
            )


class TestLogFile:
    @patch("src.lambda_extract.datetime")
    def test_log_file_success(self, mock_datetime):
        mock_s3_client = Mock()
        mock_bucket_name = "test_bucket"
        mock_keys = ["key_1", "key_2", "key_3", "key_4", "key_5"]

        mock_datetime.now.return_value = "2001-09-11_14-46-00"
        mock_datetime.today.return_value.strftime.return_value = "2001-09-11_14-46-00"
        mock_s3_client.put_object.return_value = Mock()

        message = log_file(mock_s3_client, mock_bucket_name, mock_keys)

        mock_datetime.now.assert_any_call()
        mock_datetime.today.return_value.strftime.assert_any_call("%Y-%m-%d_%H-%M-%S")
        mock_s3_client.put_object.assert_called_once()
        assert message == {
            "message": "Files Processed: Batch Lambda Transform complete"
        }

    def test_log_file_no_keys(self):
        """Test case when no files are provided to log."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        keys = []

        response = log_file(s3_client, bucket_name, keys)

        s3_client.put_object.assert_not_called()
        assert response is None

    def test_log_file_client_error(self):
        """Test AWS ClientError during S3 upload."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        keys = ["file1.txt"]

        s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "PutObject"
        )

        response = log_file(s3_client, bucket_name, keys)

        s3_client.put_object.assert_called_once()
        assert response is None

    def test_log_file_log_formatting(self):
        """Test if the log formatting works correctly."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        keys = ["file1.txt", "file2.txt", "file3.txt"]

        log_contents = []
        for key in keys:
            log_contents.append(f"Uploaded: {key} at {datetime.now()}")
        formatted_log = "\n".join(log_contents)

        with patch("src.lambda_extract.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 3, 3, 12, 0, 0)
            mock_datetime.today.return_value = datetime(2024, 3, 3, 12, 0, 0)

            log_file(s3_client, bucket_name, keys)

        expected_log = (
            "Uploaded: file1.txt at 2024-03-03 12:00:00\n"
            "Uploaded: file2.txt at 2024-03-03 12:00:00\n"
            "Uploaded: file3.txt at 2024-03-03 12:00:00"
        )

        s3_client.put_object.assert_called_once_with(
            Body=str.encode(expected_log),
            Bucket=bucket_name,
            Key="logs/2024-03-03_12-00-00.log",
        )


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
        None,
        "address",
        [[1, "123 Northcode Road", "Leeds"], [2, "66 Fenor Drive", "Manchester"]],
        ["address_ID", "address", "city"],
        "2025-07-23_00-00-00",
    )
    mock_write_table_to_s3.assert_any_call(
        mock_s3_client,
        None,
        "staff",
        [
            [1, "Connor", "Creed", "creedmoney@gmail.com"],
            [2, "Brendan", "Corbett", "yeaaboii@hotmail.co.uk"],
        ],
        ["staff_ID", "first_name", "last_name", "email"],
        "2025-07-23_00-00-00",
    )

    mock_log_file.assert_called_once()
    mock_log_file.assert_called_with(
        mock_s3_client,
        None,
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
