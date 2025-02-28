import json
import os
import boto3
from decimal import Decimal
import pytest
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import pg8000.native
from unittest.mock import patch, Mock
from terraform.extract_module.src.lambda_extract import get_secret, get_rows_and_columns_from_table
import datetime
from seed import create_conn

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
            assert "Secrets Manager can't find the specified secret" in str(excinfo['Error']['Message'])
            assert result == "Error: Secrets Manager can't find the specified secret."


    @pytest.mark.it('Environment variable is wrong or does not exist for secret')
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"SECRET_NAME": "wrong_env"})
    def test_wrong_or_no_dotenv_variable(self, mock_dotenv, mock_secrets_client):

        load_dotenv()

        secret_id = os.getenv('SECRET_NAME')
        assert secret_id == "wrong_env"

        with pytest.raises(ClientError) as excinfo:
            result = get_secret(mock_secrets_client)
            assert "Secrets Manager can't find the specified secret" in str(excinfo['Error']['Message'])
            assert result == "Error: Secrets Manager can't find the specified secret."

class TestConnection:
    @pytest.mark.it("Connection to database is established and retrieves data")
    def test_connection(self):
        conn = create_conn()
        result = conn.run("SELECT * FROM test_db LIMIT 1")

        assert result == [[2, datetime.datetime(2022, 11, 3, 14, 20, 52, 186000), datetime.datetime(2022, 11, 3, 14, 20, 52, 186000), 3, 19, 8, 42927, Decimal('3.94'), 2, datetime.date(2022, 11, 7), datetime.date(2022, 11, 8), 8]]

    @pytest.mark.it("Connection to database is successfully closed")
    def test_connection_to_database_is_successfully_closed_with_close_db(self):
        conn = create_conn()
        conn.close()
        with pytest.raises(pg8000.exceptions.InterfaceError, match="connection is closed"):
            conn.close()



class TestRowsAndColumns:
    @pytest.mark.it("Rows are rceived from DB")
    def test_multiple_rows_are_received_from_DB(self):
        conn = create_conn()
        columns = list(get_rows_and_columns_from_table(conn, "test_db"))

        assert columns[1] == ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']