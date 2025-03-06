import os
import pytest
import boto3
import testing.postgresql
import sqlalchemy
import psycopg2
from unittest.mock import MagicMock, patch
from moto import mock_aws
import pandas as pd
import pyarrow
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, VARCHAR, inspect
from src.lambda_load import (
    lambda_handler,
    upload_to_warehouse
)


LATEST_FOLDER="data/20250305_092045/"


# https://www.atlassian.com/data/notebook/how-to-execute-raw-sql-in-sqlalchemy
# FIXTURE FOR TEST DATABASE - Lives only for the scope of the function
@pytest.fixture(scope="function")
def postgres_test_db():
    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        metadata = MetaData()
  
        dim_staff = Table(
            'dim_staff', metadata,
            Column('staff_id', Integer, primary_key=True),
            Column('first_name', String),
            Column('last_name', String),
            Column('department_name', String),
            Column('location', String),
            Column('email_address', String),
        )
        
        dim_currency = Table(
            'dim_currency', metadata,
            Column('currency_id', Integer, primary_key=True),
            Column('currency_code', String),
            Column('currency_name', String),
        )

        dim_counterparty = Table(
            'dim_counterparty', metadata,
            Column('counterparty_id', Integer, primary_key=True),
            Column('counerparty_legal_name', String),
            Column('counerparty_legal_address_line_1', String),
            Column('counerparty_legal_address_line_2', String),
            Column('counerparty_legal_district', String),
            Column('counerparty_legal_city', String),
            Column('counerparty_legal_postal_code', String),
            Column('counerparty_legal_country', String),
            Column('counerparty_legal_phone_number', String),
            
        )
        metadata.create_all(engine)
        yield engine


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


@pytest.fixture(scope="function")
def mock_s3():
    """create a connection to a mocked s3 client"""
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def mock_populated_s3_client(mock_s3):
    """use the mocked s3 client to create and populate a mocked s3 bucket with
    parquet files ready to be used for testing"""
    mock_s3.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    filepath_1 = f"{os.getcwd()}/test/parquet/dim_counterparty.parquet"
    key_1 = "data/20250305_093915/dim_counterparty.parquet"
    mock_s3.upload_file(filepath_1, "test_bucket", key_1)

    filepath_2 = f"{os.getcwd()}/test/parquet/fact_sales_order.parquet"
    key_2 = "data/20250305_093915/fact_sales_order.parquet"
    mock_s3.upload_file(filepath_2, "test_bucket", key_2)

    filepath_3 = f"{os.getcwd()}/test/parquet/dim_currency.parquet"
    key_3 = "data/20250305_092045/dim_currency.parquet"
    mock_s3.upload_file(filepath_3, "test_bucket", key_3)

    filepath_4 = f"{os.getcwd()}/test/parquet/dim_staff.parquet"
    key_4 = "data/20250305_092045/dim_staff.parquet"
    mock_s3.upload_file(filepath_4, "test_bucket", key_4)

    yield mock_s3

@pytest.fixture(scope="function")
def list_of_dataframes():
    """returns the correct dataframes for the two parquet files used for testing"""
    dfs = {}
    prefix = './test/parquet/'
    for filename in os.listdir(prefix):
        basename = os.path.splitext(filename)[0]
        full_path = prefix + filename
        dfs[basename] = pd.read_parquet(full_path)
    return dfs

    # parquet_file_1 = f"{os.getcwd()}/test/parquet/dim_staff.parquet"
    # parquet_file_2 = f"{os.getcwd()}/test/parquet/dim_currency.parquet"
    # df_1 = pd.read_parquet(parquet_file_1, engine="pyarrow")
    # df_2 = pd.read_parquet(parquet_file_2, engine="pyarrow")
    # return [df_1, df_2]

@pytest.fixture(scope="function")
def list_of_s3_files(mock_populated_s3_client):
    response = mock_populated_s3_client.list_objects_v2(Bucket='test_bucket', Prefix=LATEST_FOLDER)
    formatted_response = [file['Key'] for file in response['Contents']]
    list_of_filenames = []
    for file in formatted_response:
        name = os.path.basename(file)
        list_of_filenames.append(name)

    return list_of_filenames



# class TestConvertParquetsToDataframes:

#     def test_function_returns_dataframe_list_for_s3_filepath(
#         self, mock_populated_s3_client, list_of_dataframes
#     ):
#         latest_folder_s3_filepath = "data/20250305_093915/"
#         result = convert_parquets_to_dataframes(
#             mock_populated_s3_client, latest_folder_s3_filepath, "test_bucket"
#         )
#         pd.testing.assert_frame_equal(result[0], list_of_dataframes[0])
#         pd.testing.assert_frame_equal(result[1], list_of_dataframes[1])

#     def test_function_isnt_collecting_items_from_any_other_folders(
#         self, mock_populated_s3_client
#     ):
#         latest_folder_s3_filepath = "data/20250305_092045/"
#         result = convert_parquets_to_dataframes(
#             mock_populated_s3_client, latest_folder_s3_filepath, "test_bucket"
#         )
#         assert len(result) == 2

#     def test_function_handles_exceptions_suitably(self, mock_populated_s3_client):
#         latest_folder_s3_filepath = "data/invalidpath/20250305_092045/"
#         result = convert_parquets_to_dataframes(
#             mock_populated_s3_client, latest_folder_s3_filepath, "test_bucket"
#         )
#         assert "Error:" in result["message"]


    

class TestUploadDfToWarehouse:
    def test_upload_to_df_warehouse_uploads_to_mock_data_warehouse(self, postgres_test_db, list_of_dataframes):


        dfs = list_of_dataframes
        print(dfs)

        # with engine.connect() as conn:
        #     for file in s3_files:
        #             for key, value in list_of_dataframes.items():
        #                 if key in file:
        #                     print(file, key)
        #                     upload = value.to_sql(key, engine, if_exists='append', index=False)

        engine = postgres_test_db
        print(engine)
        with engine.connect() as conn:
            result = upload_to_warehouse()
            print(result)

            dim_currency_data = conn.execute(text('SELECT * FROM dim_currency LIMIT 1'))
            dim_rows = [row for row in dim_currency_data]
            print("DIM ROWS", dim_rows)

            # dim_staff_data = conn.execute(text('SELECT * FROM dim_staff LIMIT 1'))
            # dim_rows = [row for row in dim_staff_data]
            # assert dim_rows[0] == (1, 'GBP', 'British Pound Sterling')

        # filepaths = [file['Key'] for file in s3_latest['Contents']]

        # for filepath in filepaths: 
        #     print(pd.read_parquet(filepath))


        # for file in list_of_s3_files:
        #     # print(file)
        #     with engine.connect() as conn:
        #         query_result = conn.execute(text(f'SELECT * FROM {file} LIMIT 10'))
        #         data = [row for row in query_result]
        #         print(f"{file}:", data)
        # assert False

        # # DB upload
        # engine = postgres_test_db
        # # inspector = inspect(engine)
        # # result = inspector.get_columns('dim_counterparty')
        # # columns = [column['name'] for column in result]
        # # print(columns)

        #     upload_dfs_to_warehouse()
            
        #     dim_counterparty = conn.execute(text("SELECT * FROM dim_counterparty"))
        #     dim_counterparty_data = [row for row in dim_counterparty]

        #     # print(dim_counterparty_data)
        #     assert False

        




    # test working function returns success message
    # test that tables have been updated

    # ARRANGE
    # spin up a local version of a database (fixture)
    # create a test dataframe list (fixture)
    # mock conn = MagicMock() (fixture)
    # create a mocked SQLchemy engine
    # make connection to test database

    # ACT
    # call upload_dfs_to_warehouse with mocked / patched connection and test dataframe list

    # ASSERT
    # assert table have been updated with the expected data from dfs
    # assert success message returned


# class TestLambdaHandler:

#     @patch('src.lambda_load.secret_name')
#     @patch("src.lambda_load.sm_client")
#     @patch("src.lambda_load.bucket_name")
#     @patch("src.lambda_load.s3_client")
#     @patch("src.lambda_load.close_db")
#     @patch("src.lambda_load.upload_dfs_to_warehouse")
#     @patch("src.lambda_load.create_conn")
#     @patch("src.lambda_load.convert_parquets_to_dataframes")
#     def test_all_utils_called_correctly(
#         self,
#         mock_convert_parquets,
#         mock_create_conn,
#         mock_upload_to_wh,
#         mock_close_db,
#         mock_s3_client,
#         mock_bucket_name,
#         mock_sm_client,
#         mock_secret_name,
#         list_of_dataframes
#     ):
#         # ARRANGE:
#         event = {"s3_file_path": "data/20250305_092045/"}
#         context = None
#         mock_convert_parquets.return_value = list_of_dataframes
#         mock_create_conn.return_value = 'conn'
#         # ACT:
#         result = lambda_handler(event, context)
#         # ASSERT:
#         mock_convert_parquets.assert_called_once_with(
#             mock_s3_client, "data/20250305_092045/", mock_bucket_name
#         )
#         mock_create_conn.assert_called_once_with(mock_sm_client, mock_secret_name)
#         mock_upload_to_wh.assert_called_once_with('conn', list_of_dataframes)
#         mock_close_db.assert_called_once_with('conn')
#         assert result == {"message": "Successfully uploaded to database"}

#     def test_handler_handles_event_errors_effectively(self):
#         event = {}
#         context = None
#         result = lambda_handler(event, context)
#         assert 'Failure' in result['message']

#     def test_handler_handles_client_errors_effectivley(self):
#         event = {"s3_file_path": "data/20250305_092045/"}
#         context = None
#         result = lambda_handler(event, context)
#         assert 'Failure' in result['message']


