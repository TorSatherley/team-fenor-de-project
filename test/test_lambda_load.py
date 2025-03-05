from src.lambda_load import lambda_handler, get_list_of_parquets, parquet_to_pandas_df, upload_df_to_warehouse
import os
import pytest
import boto3
from unittest.mock import MagicMock, patch
from moto import mock_aws


@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    os.environ["SECRET_NAME"] = "test-secret"


@pytest.fixture(scope="function")
def mock_s3():
    '''create a connection to a mocked s3 client'''
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope='function')
def mock_populated_s3(mock_s3):
    '''create and populate the mocked s3 bucket with two parquet files ready to be
    used for testing'''
    mock_s3.create_bucket(
        Bucket='test_bucket',
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    filepath_1 = f'{os.getcwd()}/parquet/dim_counterparty.parquet'
    key_1 = 'data/2025_03_05__0939/dim_counterparty.parquet'
    with open(filepath_1, 'r') as f:
        data_1 = f.read()
        mock_s3.put_object(Body=data_1, Bucket='test_bucket', key=key_1)

    filepath_2 = f'{os.getcwd()}/parquet/fact_sales_order.parquet'
    key_2 = 'data/2025_03_05__0939/fact_sales_order.parquet'
    with open(filepath_2, 'r') as f:
        data_2 = f.read()
        mock_s3.put_object(Body=data_2, Bucket='test_bucket', key=key_2)
    yield mock_s3



class ConvertParquetsToDataframes:
    def test_function_returns_dataframe_list_for_s3_filepath(self, mock_s3):
        pass
    # test working funct returns a correct dataframe list from all parquets in latest folder
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