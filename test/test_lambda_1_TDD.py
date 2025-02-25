import pytest
import boto3
from moto import mock_aws
import json
from unittest.mock import Mock, patch
from pprint import pprint
from src.lambda_ingest import write_variable_to_s3, write_table_to_s3, connect_to_db, close_db_connection, get_rows_and_columns_from_table, return_object_s3_key_injection_bucket

"""
test_lambda_1_TDD.py
Author: Fabio Greenwood

Intro:

    This module contains unit tests for the lambda_ingest.py module.
    It uses pytest for testing.

    Currently the first draft of tests are being written, the major point of this file is to lay out a set of habits like the use of fixtures


Functions tested:
    - write_file_to_s3
        test this!!!

    - connect_to_db?
    - close_db_connection?
        unsure how and if I should test these (maybe check what was done on other exercises)

    - get_rows_and_columns_from_table??
        maybe

PS:
    Please check the Trello for the over arching To-Do list!

Actions:
    - Remove the hardcoding
"""



#%% Placeholder Variables and Functions - These are hard-coded currently and may need to be made more programmic

target_bucket_name = "injestion zone"
list_of_toteSys_tables = ["tableA", "tableB"] # Action: obviously this isn't the list and should be replaced with an postGRESS function that lists all the tables in a database


def some_function_that_returns_a_table_from_Postgres():
    pass



#%%

@pytest.fixture()
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=target_bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        yield s3

@pytest.fixture()
def hardcoded_variables():
    hardcoded_variables = {}
    hardcoded_variables["target_bucket_name"] = "injestion zone"
    hardcoded_variables["list_of_toteSys_tables"] = ["tableA", "tableB"]
    return hardcoded_variables

@pytest.fixture()
def example_table_A_from_Postgres():
    example_table_A_from_Postgres = some_function_that_returns_a_table_from_Postgres()
    return example_table_A_from_Postgres


@pytest.mark.timeout(10)
class Test_write_file_to_s3:
    def test_1_expected_file_names_are_added_to_blank_s3(self, s3_client, hardcoded_variables, example_table_A_from_Postgres):
        """
        This test verifies that the write_variable_to_s3 function adds a table to the s3 bucket.

        Expected behavior:
        - write_variable_to_s3(s3_client, variable, bucket_name, object_key)
            should:
            then populate said variable (in this case table data returned from postgres) in the object_key location specified
        """
        # assemble
        object_key = return_object_s3_key_injection_bucket(example_table_A_from_Postgres)
        expected_file_key_list = [example_table_A_from_Postgres.__name__] # Action: I reckon there is a way to extract the table name from the variable metadata
        
        # act
        response = write_variable_to_s3(s3_client, example_table_A_from_Postgres, hardcoded_variables["target_bucket_name"], object_key)
        
        # assert
        file_list_response = s3_client.list_objects_v2(Bucket=hardcoded_variables["target_bucket_name"])
        actual_file_key_list = [i['Key'] for i in file_list_response['Contents']]
        pprint(response)
        assert expected_file_key_list == actual_file_key_list

    def test_2_second_entry_is_added_and_interacts_with_first_entry_ok(self, s3_client, hardcoded_variables, example_table_A_from_Postgres):
        pass






