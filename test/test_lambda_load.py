from src.lambda_load import (
    lambda_handler,
    convert_parquets_to_dataframes,
    upload_dfs_to_warehouse,
)
import os
import pytest
import boto3
from unittest.mock import MagicMock, patch
from moto import mock_aws
import pandas as pd
import pyarrow


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


@pytest.fixture(scope='function')
def list_of_dataframes():
    '''returns the correct dataframes for the two parquet files used for testing'''
    parquet_file_1 = f"{os.getcwd()}/test/parquet/dim_counterparty.parquet"
    parquet_file_2 = f"{os.getcwd()}/test/parquet/fact_sales_order.parquet"
    df_1 = pd.read_parquet(parquet_file_1, engine='pyarrow')
    df_2 = pd.read_parquet(parquet_file_2, engine='pyarrow')
    return [df_1, df_2]



class TestConvertParquetsToDataframes:
    def test_function_returns_dataframe_list_for_s3_filepath(self, mock_populated_s3_client, list_of_dataframes):
        # ARRANGE
        latest_folder_s3_filepath = 'data/20250305_093915/'
        # ACT
        result = convert_parquets_to_dataframes(mock_populated_s3_client, latest_folder_s3_filepath, 'test_bucket')
        # ASSERT
        pd.testing.assert_frame_equal(result[0], list_of_dataframes[0])
        pd.testing.assert_frame_equal(result[1], list_of_dataframes[1])

    # test function isnt collecting items from any other folders
    # test function handles exceptions suitably (client and pandas)

    # ARRANGE
    # mock aws s3 client
    # put some test parquet files and folders into it
    # make a test latest_folder folder path

    # ACT
    # call get_list_of_parquets with mocked s3_client and test latest_folder filepath

    # ASSERT
    # assert funct returns the correct list of dataframes
    pass


class TestUploadDfToWarehouse:
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
    pass


class TestLambdaHandler:
    pass