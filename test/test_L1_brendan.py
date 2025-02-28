import json
import os
import pytest
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from moto import mock_aws
from unittest.mock import patch, Mock
from src.lambda_extract import get_secret, lambda_handler, close_db
from datetime import datetime


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
        with pytest.raises(ClientError):
            get_secret(client=mock_secrets_client)

    @pytest.mark.it("Error is shown if secret does not exist")
    def test_secret_does_not_exist(self, mock_secrets_client):
        with pytest.raises(ClientError) as excinfo:
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

        with pytest.raises(ClientError) as excinfo:
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
