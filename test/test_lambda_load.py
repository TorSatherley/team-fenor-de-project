import os
import pytest
import boto3
import psycopg2
from unittest.mock import MagicMock, patch
from moto import mock_aws
import pandas as pd

from src.lambda_load import (
    load_connection,
    lambda_handler,
    dw_credentials
)

@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    os.environ["BUCKET_NAME"] = "test_bucket"
    os.environ["SECRET_NAME"] = "test-secret"

LATEST_FOLDER="data/"

def create_test_db():
    conn = psycopg2.connect(dbname='postgres', user='postgres', password='password', host='localhost')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS test_db;")
    cursor.execute("CREATE DATABASE test_db;")
    cursor.close()
    conn.close()

def drop_test_db():
    conn = psycopg2.connect(dbname='postgres', user='postgres', password='password', host='localhost')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS test_db;")
    cursor.close()
    conn.close()

@pytest.fixture(scope="function")
def postgres_test_db():
    create_test_db()
    conn = psycopg2.connect(dbname='test_db', user='postgres', password='password', host='localhost')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE dim_staff (
            staff_id SERIAL PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            department_name VARCHAR,
            location VARCHAR,
            email_address VARCHAR
        );
    ''')
    conn.commit()
    
    # Insert sample data
    cursor.execute('''
        INSERT INTO dim_staff 
        (first_name, last_name, department_name, location, email_address)
        VALUES 
        ('John', 'Doe', 'Development', 'Leeds', 'john.doe@example.com'),
        ('Jane', 'Smith', 'Marketing', 'Manchester', 'jane.smith@example.com');
    ''')
    conn.commit()
    
    yield conn
    cursor.close()
    conn.close()
    drop_test_db()



@pytest.fixture(scope="function")
def mock_s3():
    """create a connection to a mocked s3 client"""
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket="totesys-processed-zone-fenor-v2",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filepath = f"{os.getcwd()}/test/parquet/dim_staff.parquet"
        key = "data/dim_staff.parquet"
        s3_client.upload_file(filepath, "totesys-processed-zone-fenor-v2", key)

        yield s3_client


class TestDbConnection:
    # def test_db_is_a_connection_object(self):
    #     conn = load_connection()

    #     assert isinstance(conn, psycopg2.extensions.connection)
    #     assert conn.closed == 0

    def test_db_exception_if_failure_of_credentials(self):
        dw_access = { 
        'dbname': 'test',
        'user': 'postgres',
        'password': 'test',
        'host': 'test',
        'port': 5432
         }
        conn = load_connection()
        print(conn)

        assert False


    # def test_lambda_handler(self, postgres_test_db, mock_s3):
    #     event = {}
    #     event['datetime_string'] = 'data/'
    #     test_result = lambda_handler(event,{})

    #     assert test_result['message'] == 'Successfully uploaded to data warehouse'