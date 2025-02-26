import pytest
import boto3
from moto import mock_aws
import json
from unittest.mock import Mock, patch
from pprint import pprint
from src.dummy_lambda_ingest import write_table_to_s3, lambda_handler
from datetime import datetime
from src.util import json_to_pg8000_output
import os
from unittest import mock

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
    - ensure that development uses environmental variable to assign bucket name
    - populate: placeholder_function_for_checking_data_placed_into_s3_folder
"""



#%% Placeholder Variables and Functions - These are hard-coded currently and may need to be made more programmic

class DummyContext:
    pass

MOCK_ENVIROMENT = True


#%% fixtures


@pytest.fixture()
def hardcoded_variables():
    hardcoded_variables = {}
    hardcoded_variables["target_bucket_name"] = "totesys-ingestion-zone-fenor-dummy-test"
    hardcoded_variables["list_of_toteSys_tables"] = ["tableA", "tableB"]
    hardcoded_variables["AccountId"] = "AccountId"
    hardcoded_variables["list_of_tables"] = ["address", "counterparty", "currency", "department", "design", "payment_type", "payment", "purchase_order", "sales_order", "staff", "transaction"]
    hardcoded_variables["dict_table_snapshot_filepaths"] = {
    "address"       : "data/json_files/address.json", 
    "counterparty"  : "data/json_files/counterparty.json", 
    "currency"      : "data/json_files/currency.json", 
    "department"    : "data/json_files/department.json", 
    "design"        : "data/json_files/design.json", 
    "payment_type"  : "data/json_files/payment_type.json", 
    "payment"       : "data/json_files/payment.json", 
    "purchase_order": "data/json_files/purchase_order.json", 
    "sales_order"   : "data/json_files/sales_order.json", 
    "staff"         : "data/json_files/staff.json", 
    "transaction"   : "data/json_files/transaction.json"
    }
    
    return hardcoded_variables


@pytest.fixture()
def s3_client(hardcoded_variables):
    global MOCK_ENVIROMENT
    if MOCK_ENVIROMENT == True:
        with mock_aws():
            s3 = boto3.client("s3")
            s3.create_bucket(
                Bucket=hardcoded_variables["target_bucket_name"],
                CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
            yield s3
    elif MOCK_ENVIROMENT == False:
        s3 = boto3.client('s3')
        response = s3.get_bucket(
            AccountId='string',
            Bucket=hardcoded_variables["target_bucket_name"])
        yield s3

@pytest.fixture()
def example_sales_order_table(hardcoded_variables):
    simulated_pg8000_output, simulated_pg8000_output_cols = json_to_pg8000_output(hardcoded_variables["dict_table_snapshot_filepaths"]["sales_order"], include_cols_in_output=True)
    return simulated_pg8000_output, simulated_pg8000_output_cols


@pytest.fixture
def mock_s3_bucket_name(monkeypatch, hardcoded_variables):
    monkeypatch.setenv("S3_BUCKET_INGESTION", hardcoded_variables["target_bucket_name"])


@pytest.fixture()
def snapshot_data_dict(hardcoded_variables):
    snapshot_data_dict = {}
    for key, path in zip(hardcoded_variables["dict_table_snapshot_filepaths"].keys(),
                         hardcoded_variables["dict_table_snapshot_filepaths"].values()):
        rows, cols = json_to_pg8000_output(path, include_cols_in_output=True)
        snapshot_data_dict[key] = {"rows": rows, "cols":cols}
        
    # TO DO: this will take all the snapshot data from the jsons and place them into dfs in a dict for later testing
    return snapshot_data_dict

def return_s3_key__injection_bucket(table_name):
    timestamp = datetime.now()
    year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute
    return f'data/{table_name}/{year}-{month}-{day}_{hour}-{minute}/{table_name}.json'
    



#%% Tests


#@pytest.mark.timeout(10)
class Test_write_table_to_s3:
    @pytest.mark.skip()
    def test_1_expected_file_names_are_added_to_blank_s3(self, s3_client, hardcoded_variables, example_sales_order_table):
        """
        This test verifies that the write_table_to_s3 function adds a table to the s3 bucket.

        Expected behavior:
        - write_variable_to_s3(s3_client, variable, bucket_name, object_key)
            should:
            then populate said variable (in this case table data returned from postgres) in the object_key location specified
        """
        # assemble
        pg8000_rows, pg8000_cols = example_sales_order_table
        table_name = "sales_order" # not sure if this is hardcoding
        expected_object_key = return_s3_key__injection_bucket(table_name) # Action: I reckon there is a way to extract the table name from the variable metadata
        
        # act
        response = write_table_to_s3(s3_client, pg8000_rows, pg8000_cols, hardcoded_variables["target_bucket_name"], table_name)
        
        # assert
        file_list_response = s3_client.list_objects_v2(Bucket=hardcoded_variables["target_bucket_name"])
        actual_file_key_list = [i['Key'] for i in file_list_response['Contents']]
        pprint(response)
        assert [expected_object_key] == actual_file_key_list
        
        
        


class Test_lambda_hander:
    #@mock.patch.dict(os.environ, {"S3_BUCKET_INGESTION": "totesys-ingestion-zone-fenor-dummy-test"}, clear=True)
    def test_2a_all_tables_are_digested_once__mocked(self, s3_client, hardcoded_variables, snapshot_data_dict, mock_s3_bucket_name):
        """
        This test verifies that the lambda handler when fed controlled values for the conn.run method, can populate a s3 bucket correctly.

        Expected behavior:
        - lambda_handler(event, DummyContext)
            should:
            then populate s3 bucket as logic is designed to do
        """

            
        # assemble  
        # s3_mock_client = boto3.client("s3")
        event = {}
        nested_list_of_pg8000_returned_rows = [snapshot_data_dict[table_name]["rows"] for table_name in hardcoded_variables["list_of_tables"]]
        nested_list_of_pg8000_returned_cols = [snapshot_data_dict[table_name]["cols"] for table_name in hardcoded_variables["list_of_tables"]]
        
        print("ddd")
        # act
        with patch("pg8000.native.Connection.run", side_effect=[hardcoded_variables["list_of_tables"]] + nested_list_of_pg8000_returned_rows):
            with patch("pg8000.native.Connection.columns", side_effect=nested_list_of_pg8000_returned_cols):
                lambda_handler(event, DummyContext)
                    
        # assert - do all the files exist?
        actual_list_of_s3_filepaths = s3_client.list_objects_v2(Bucket=hardcoded_variables["target_bucket_name"])
        pprint(actual_list_of_s3_filepaths)
        # pprint(set(return_s3_key__injection_bucket(table_name) for table_name in hardcoded_variables["list_of_tables"]))
        
        #assert set(actual_list_of_s3_filepaths) == set(return_s3_key__injection_bucket(table_name) for table_name in hardcoded_variables["list_of_tables"])
        #
        ## assert - does data match?
        #def placeholder_function_for_checking_data_placed_into_s3_folder(table_name):
        #    return True
        #for table_name in hardcoded_variables["list_of_toteSys_tables"]:
        #    assert placeholder_function_for_checking_data_placed_into_s3_folder(table_name) == True
                
                
                

    @pytest.mark.skip()
    def test_2a_all_tables_are_digested_twice__mocked():
        # this method should check that the s3 bucket stores both sets of data alongside if data is inserted TWICE
                
        pass
    
    @pytest.mark.skip()
    def test_2c_ensure_that_failures_are_logged():
        # adapt the below
        """Tests the lambda handler"""

        """@pytest.mark.it("unit test: writes to s3")
        @patch("src.quotes.get_quote")
        def test_handler_writes_quotes_to_s3(self, mock_quote, s3, bucket, caplog):
            mock_quote.return_value = (200, processed)
            event = {}
            context = DummyContext()
            with caplog.at_level(logging.INFO):
                lambda_handler(event, context)
                assert "Wrote quotes to S3" in caplog.text"""
        pass



