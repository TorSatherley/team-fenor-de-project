import os
import json
import pytest
import boto3
from moto import mock_aws
from pg8000.exceptions import DatabaseError
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch
import tempfile
from src.utils import (
    get_secret,
    create_conn,
    close_db,
    get_rows_and_columns_from_table,
    write_table_to_s3,
    log_file,
    json_to_pg8000_output,
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
@patch("src.utils.create_conn")
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
        """Test successful retrieval of secret"""
        mock_get_secret_value = MagicMock()
        mock_secrets_client.get_secret_value = mock_get_secret_value
        mock_secret_name = "test-secret"
        mock_get_secret_value.return_value = {"SecretString": json.dumps(mock_secret)}
        response = get_secret(mock_secrets_client, mock_secret_name)
        assert response == mock_secret

    @pytest.mark.it("Getting a secret returns a client error")
    def test_get_secret_returns_client_error(self, mock_secrets_client):
        """Test AWS ClientError is correctly raised"""
        mock_secret_name = "test-secret"
        with pytest.raises(ClientError):
            get_secret(mock_secrets_client, mock_secret_name)

    @pytest.mark.it("Error is shown if secret does not exist")
    def test_secret_does_not_exist(self, mock_secrets_client):
        """Test handling of missing secret"""
        mock_secret_name = "test-secret"
        with pytest.raises(ClientError) as excinfo:
            result = get_secret(mock_secrets_client, mock_secret_name)
            mock_secrets_client.get_secret_value(SecretId="not-here")
            assert "Secrets Manager can't find the specified secret" in str(
                excinfo["Error"]["Message"]
            )
            assert result == "Error: Secrets Manager can't find the specified secret."

    @pytest.mark.it("Raises NoCredentialsError if AWS credentials are missing")
    def test_get_secret_no_credentials(self, mock_secrets_client):
        """Test NoCredentialsError when AWS credentials are missing"""
        mock_secrets_client = Mock()
        mock_secret_name = "test-secret"
        mock_secrets_client.get_secret_value.side_effect = NoCredentialsError
        with pytest.raises(NoCredentialsError):
            get_secret(mock_secrets_client, mock_secret_name)

    @pytest.mark.it("Raises ValueError if secret_name is None")
    def test_get_secret_no_secret_name(self):
        """Test when secret_name is None."""
        sm_client = MagicMock()
        with pytest.raises(
            ValueError, match="SECRET_NAME environment variable is not set."
        ):
            get_secret(sm_client, None)

    @pytest.mark.it("Raises JSONDecodeError if secret string is not valid JSON")
    def test_get_secret_invalid_json(self):
        """Test JSONDecodeError when secret string is not valid JSON."""
        mock_secret_name = "test-secret"
        sm_client = MagicMock()
        sm_client.get_secret_value.return_value = {"SecretString": "{invalid-json"}
        with pytest.raises(json.JSONDecodeError):
            get_secret(sm_client, mock_secret_name)

    @pytest.mark.it("Raises KeyError if SecretString is missing from response")
    def test_get_secret_key_error(self):
        """Test KeyError when SecretString is missing from response."""
        sm_client = MagicMock()
        mock_secret_name = "test-secret"
        sm_client.get_secret_value.return_value = {}
        with pytest.raises(KeyError):
            get_secret(sm_client, mock_secret_name)

    @pytest.mark.it("Raises generic Exception for unexpected errors")
    def test_get_secret_unexpected_error(self):
        """Test handling of unexpected errors"""
        mock_secrets_client = Mock()
        mock_secrets_client.get_secret_value.side_effect = Exception(
            "Unexpected failure"
        )
        mock_secret_name = "test-secret"
        with pytest.raises(Exception, match="Unexpected failure"):
            get_secret(mock_secrets_client, mock_secret_name)

    @pytest.mark.it("Environment variable is wrong or does not exist for secret")
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"SECRET_NAME": "wrong_env"})
    def test_wrong_or_no_dotenv_variable(self, mock_dotenv, mock_secrets_client):
        """Test handling of wrong environment variable"""
        secret_id = os.getenv("SECRET_NAME")
        assert secret_id == "wrong_env"
        mock_secret_name = "test-secret"
        with pytest.raises(ClientError) as excinfo:
            result = get_secret(mock_secrets_client, mock_secret_name)
            assert "Secrets Manager can't find the specified secret" in str(
                excinfo["Error"]["Message"]
            )
            assert result == "Error: Secrets Manager can't find the specified secret."


class TestConnection:
    @pytest.mark.it("Connection to database is established and retrieves data")
    @patch("src.utils.Connection")
    def test_create_conn(self, mock_Connection, mock_secret, mock_totesys_connection):
        mock_Connection.return_value = mock_totesys_connection
        conn = create_conn(mock_secret)
        mock_Connection.assert_called_once()
        assert conn == mock_totesys_connection

    @pytest.mark.it("Raises TypeError when db_credentials is None")
    @patch("src.utils.Connection")
    def test_create_conn_none_credentials(self, mock_Connection):
        """Test TypeError when db_credentials is None."""
        with pytest.raises(TypeError):
            create_conn(None)

    @pytest.mark.it("Raises DatabaseError when database connection fails")
    @patch("src.utils.Connection")
    def test_create_conn_database_error(self, mock_Connection, mock_secret):
        mock_Connection.side_effect = DatabaseError("Mock database error")
        with pytest.raises(DatabaseError, match="Mock database error"):
            create_conn(mock_secret)

    @pytest.mark.it("Raises KeyError when required fields are missing")
    @patch("src.utils.Connection")
    def test_create_conn_missing_key(
        self, mock_Connection, mock_secret, mock_totesys_connection
    ):
        """Test KeyError when required fields are missing in credentials."""
        mock_Connection.return_value = mock_totesys_connection
        mock_secret = {
            "username": "admin",
            "password": "1234",
            "host": "localhost",
            # Missing "dbname"
        }
        with pytest.raises(KeyError):
            create_conn(mock_secret)

    @pytest.mark.it("Handles unexpected exceptions during connection creation")
    @patch("src.utils.Connection", autospec=True)
    def test_create_conn_unexpected_exception(self, mock_connection, mock_secret):
        """Test unexpected exception during connection creation."""
        mock_connection.side_effect = Exception("Database connection failed")
        with pytest.raises(Exception, match="Database connection failed"):
            create_conn(mock_secret)

    @pytest.mark.it("Connection to database is successfully closed")
    def test_connection_to_database_is_successfully_closed_with_close_db(
        self, mock_totesys_connection
    ):
        """Test that close_db successfully closes the connection."""
        close_db(mock_totesys_connection)
        mock_totesys_connection.close.assert_called_once()

    @pytest.mark.it("Handles None connection gracefully in close_db")
    def test_close_db_with_none(self):
        """Test calling close_db with None (should raise AttributeError)."""
        with pytest.raises(AttributeError):
            close_db(None)

    @pytest.mark.it("Raises an exception when closing the connection fails")
    def test_close_db_exception(self):
        """Test exception when closing the database connection."""
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Close failed")
        with pytest.raises(Exception, match="Close failed"):
            close_db(mock_conn)


class TestGetRowsAndColumnsFromTable:
    @pytest.mark.it("Fetches rows and columns from a valid table")
    def test_get_rows_and_columns_from_table(self, mock_totesys_connection):
        """Test successful retrieval of rows and column names."""
        mock_totesys_connection.run.side_effect = [
            [("col_1",), ("col_2",)],
            [["val1", "val2"], ["val3", "val4"]],
        ]
        mock_table = "test_table"
        rows, columns = get_rows_and_columns_from_table(
            mock_totesys_connection, mock_table
        )
        assert mock_totesys_connection.run.call_count == 2
        assert rows == [["val1", "val2"], ["val3", "val4"]]
        assert columns == ["col_1", "col_2"]

    @pytest.mark.it("Handles empty table (no rows but columns exist)")
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

    @pytest.mark.it("Handles table not found error gracefully")
    def test_get_rows_and_columns_table_not_found(self):
        """Test handling when the table does not exist."""
        mock_conn = MagicMock()
        mock_conn.run.side_effect = Exception("relation 'non_existent' does not exist")
        rows, columns = get_rows_and_columns_from_table(mock_conn, "non_existent")
        assert columns == []
        assert rows == []
        assert mock_conn.run.call_count == 1

    @pytest.mark.it("Handles unexpected database errors")
    def test_get_rows_and_columns_unexpected_exception(self):
        """Test handling of an unexpected exception."""
        mock_conn = MagicMock()
        mock_conn.run.side_effect = Exception("Unexpected error")
        rows, columns = get_rows_and_columns_from_table(mock_conn, "users")
        assert columns == []
        assert rows == []
        assert mock_conn.run.call_count == 1


class TestWriteTableToS3:
    @pytest.mark.it("Uploads table data as JSON to S3")
    @patch("src.utils.pd.DataFrame")
    def test_write_table_to_s3(self, mock_pd_DataFrame):
        """Test successful JSON upload to S3."""
        mock_s3_client = Mock()
        mock_table = "test_table"
        mock_bucket = "test_bucket"
        mock_rows = [["val1", "val2"], ["val3", "val4"]]
        mock_columns = ["col_1", "col_2"]
        mock_date_and_time = "20021011_112233"

        mock_df = Mock()
        mock_pd_DataFrame.return_value = mock_df
        mock_df.to_json.return_value = Mock()
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
        mock_s3_client.put_object.assert_called_with(
            Bucket="test_bucket",
            Key=key,
            Body=mock_df.to_json.return_value,
        )
        assert key == "data/20021011_112233/test_table.jsonl"

    @pytest.mark.it("Handles empty data gracefully and skips upload")
    def test_write_table_to_s3_empty_data(self):
        """Test handling when no data is present (should return None)."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        table = "empty_table"
        date_and_time = "2024-03-03"

        key_1 = write_table_to_s3(
            s3_client, bucket_name, table, [], ["id"], date_and_time
        )
        key_2 = write_table_to_s3(
            s3_client, bucket_name, table, [(1, "NorthCoders")], [], date_and_time
        )
        assert key_1 is None
        assert key_2 is None
        s3_client.put_object.assert_not_called()

    @pytest.mark.it("Handles AWS ClientError during S3 upload")
    def test_write_table_to_s3_client_error(self):
        """Test handling of AWS ClientError during S3 upload."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        table = "users"
        rows = [(1, "NorthCoders")]
        columns = ["id", "name"]
        date_and_time = "2024-03-03"

        s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "PutObject"
        )

        key = write_table_to_s3(
            s3_client, bucket_name, table, rows, columns, date_and_time
        )
        assert key is None

    @pytest.mark.it("Handles unexpected exceptions during S3 upload")
    @patch("src.utils.pd.DataFrame")
    def test_write_table_to_s3_unexpected_exception(self, mock_pd_DataFrame):
        """Test handling of unexpected exceptions."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        table = "users"
        rows = [(1, "NorthCoders")]
        columns = ["id", "name"]
        date_and_time = "2024-03-03"

        mock_pd_DataFrame.side_effect = Exception("Unexpected error")
        key = write_table_to_s3(
            s3_client, bucket_name, table, rows, columns, date_and_time
        )
        assert key is None


class TestLogFile:
    @pytest.mark.it("Successfully logs file upload details to S3")
    @patch("src.utils.datetime")
    def test_log_file_success(self, mock_datetime):
        """Test successful log file upload to S3."""
        mock_s3_client = Mock()
        mock_bucket_name = "test_bucket"
        mock_keys = ["key_1", "key_2", "key_3", "key_4", "key_5"]

        mock_datetime.now.return_value = "20021011_112233"
        mock_datetime.today.return_value.strftime.return_value = "20021011_112233"
        mock_s3_client.put_object.return_value = Mock()

        message = log_file(mock_s3_client, mock_bucket_name, mock_keys)

        mock_datetime.now.assert_any_call()
        mock_datetime.today.return_value.strftime.assert_any_call("%Y-%m-%d_%H-%M-%S")
        mock_s3_client.put_object.assert_called_once()
        assert message == {
            "message": "Files Processed: Batch Lambda Transform complete"
        }

    @pytest.mark.it("Handles case when no files are uploaded")
    def test_log_file_no_keys(self):
        """Test case when no files are provided to log."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        keys = []

        response = log_file(s3_client, bucket_name, keys)

        s3_client.put_object.assert_not_called()
        assert response is None

    @pytest.mark.it("Handles AWS ClientError during log file upload")
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

    @pytest.mark.it("Correctly formats log entries before uploading")
    @patch("src.utils.datetime")
    def test_log_file_log_formatting(self, mock_datetime):
        """Test if the log formatting works correctly."""
        s3_client = MagicMock()
        bucket_name = "test-bucket"
        keys = ["file1.txt", "file2.txt", "file3.txt"]

        mock_datetime.now.return_value = datetime(2024, 3, 3, 12, 0, 0)
        mock_datetime.today.return_value = datetime(2024, 3, 3, 12, 0, 0)
        expected_log = (
            "Uploaded: file1.txt at 2024-03-03 12:00:00\n"
            "Uploaded: file2.txt at 2024-03-03 12:00:00\n"
            "Uploaded: file3.txt at 2024-03-03 12:00:00"
        )

        log_file(s3_client, bucket_name, keys)

        s3_client.put_object.assert_called_once_with(
            Body=str.encode(expected_log),
            Bucket=bucket_name,
            Key="logs/2024-03-03_12-00-00.log",
        )


class TestJsonToPg8000Output:
    @pytest.mark.it(
        "Should correctly convert JSON data to pg8000-style nested list format"
    )
    def test_json_to_pg8000_output(self):
        """Test that JSON is correctly converted to pg8000-style output."""
        sample_data = [
            {"id": 1, "name": "NorthCoders"},
            {"id": 2, "name": "SouthCoders"},
        ]

        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
            json.dump(sample_data, temp_file)
            temp_file_path = temp_file.name

        expected_output = [[1, "NorthCoders"], [2, "SouthCoders"]]
        expected_cols = ["id", "name"]

        output, cols = json_to_pg8000_output(temp_file_path)

        assert output == expected_output, "Output does not match expected output"
        assert cols == expected_cols, "Column names do not match expected columns"
