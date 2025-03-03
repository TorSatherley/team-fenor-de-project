import pytest
import boto3
from moto import mock_aws
import json
from unittest.mock import Mock, patch
from pprint import pprint
from datetime import datetime
from src.lambda_ingest_dummy import write_table_to_s3, lambda_handler
from datetime import datetime
from src.util import json_to_pg8000_output
from unittest import mock
from src.lambda_2 import read_s3_table_json, create_sales_table
from src.util import json_to_pg8000_output, return_datetime_string, simple_read_parquet_file_into_dataframe
import pandas as pd

"""
test_lambda_2_TDD.py
Author: Fabio Greenwood

Intro:
    This module contains unit tests for the second lambda function.
    It uses pytest for testing.

Requirement Spec:
    Lambda 2:
    1. Must transform data landing in the "ingestion" S3 bucket
        a. reads json from s3 bucket and converts to a pandas_df with correct data
        b. new pandas_df is transform into the right schema 
            bi. how do we check 20 table schemas without getting confused (do we have to)
    2. Must place the results in the "processed" S3 bucket. 
        a. formats of tables? maybe start at one then get more advanced
    
    3. Must stores the data in Parquet format in the "processed" S3 bucket.
    4. Data must conform to the warehouse schema (TASK: please insert diagram from git readme). 
    5. Must be triggered by either an S3 event triggered when data lands in the ingestion bucket, 
    6. Must also be triggered byor on a schedule. 
    7. Status must be logged to Cloudwatch, and an alert triggered if a serious error occurs.
    8. Errors must be logged to Cloudwatch, and an alert triggered if a serious error occurs.
    9. Populate the dimension and fact tables of a single "star" schema


Functions tested:
    - TO BE POPULATED

Actions:
    - maybe add tests to check column formats
    
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
    hardcoded_variables["ingestion_bucket_name"] = "totesys-ingestion-zone-fenor-dummy-test"
    hardcoded_variables["processing_bucket_name"] = "totesys-processing-zone-fenor-dummy-test"
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
    """returns a client with the two buckets needed for this test suite"""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=hardcoded_variables["ingestion_bucket_name"],
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        s3.create_bucket(
            Bucket=hardcoded_variables["processing_bucket_name"],
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        yield s3


@pytest.fixture()
def example_sales_order_table(hardcoded_variables):
    simulated_pg8000_output, simulated_pg8000_output_cols = json_to_pg8000_output(hardcoded_variables["dict_table_snapshot_filepaths"]["sales_order"], include_cols_in_output=True)
    return simulated_pg8000_output, simulated_pg8000_output_cols


@pytest.fixture
def mock_s3_bucket_name(monkeypatch, hardcoded_variables):
    monkeypatch.setenv("S3_BUCKET_INGESTION", hardcoded_variables["ingestion_bucket_name"])


@pytest.fixture()
def snapshot_data_dict(hardcoded_variables):
    snapshot_data_dict = {}
    for key, path in zip(hardcoded_variables["dict_table_snapshot_filepaths"].keys(),
                         hardcoded_variables["dict_table_snapshot_filepaths"].values()):
        rows, cols = json_to_pg8000_output(path, include_cols_in_output=True)
        snapshot_data_dict[key] = {"rows": rows, "cols":cols}
        
    # TO DO: this will take all the snapshot data from the jsons and place them into dfs in a dict for later testing
    return snapshot_data_dict


def s3_client__populated_bucket(s3_client, hardcoded_variables):
    # placeholder puesdocode
    
    placeholder_shadow_realm_bucket_name = "i dunno"
    
    for key in hardcoded_variables["dict_table_snapshot_filepaths"]:
        rows, cols = json_to_pg8000_output(hardcoded_variables["dict_table_snapshot_filepaths"][key])
        write_table_to_s3(s3_client, rows, cols, placeholder_shadow_realm_bucket_name, key)
    
    yield s3_client


def return_s3_key__injection_bucket(table_name, original_invocation_time_string):
    """ this will return what should be the key for a table of a given name in the injestion bucket"""
    #timestamp = datetime.now()
    #year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute
    return f'data/{table_name}/{original_invocation_time_string}/{table_name}.json'
    #return f'data/{table_name}/{year}-{month}-{day}_{hour}-{minute}/{table_name}.json'
    
def return_list_of_cities_in_address_df():
    list_of_cities = ["New Patienceburgh", "Aliso Viejo", "Lake Charles", "Olsonside", "Fort Shadburgh", "Kendraburgh", "North Deshaun", "Suffolk", "New Tyra", "Beaulahcester", "Corpus Christi", "Pricetown", "Shanahanview", "Maggiofurt", "East Bobbie", "South Wyatt", "Hackensack", "Lake Arne", "West Briellecester", "Pueblo", "Fresno", "Sayreville","Derekport", "New Torrance", "East Arvel", "Napa", "Oakland Park", "Utica", "Bartellview", "Lake Myrlfurt"]
    return list_of_cities
    

def sales_order_table_columns():
    list_sales_order_columns = ["created_at", "last_updated", "design_id", "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"]
    return list_sales_order_columns

def return_s3_key__injection_bucket(table_name): # TODO: This needs refactoring
    timestamp = datetime.now()
    year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute
    return f'data/{table_name}/{year}-{month}-{day}_{hour}-{minute}/{table_name}.json'
    


#%% Tests

# pg8000_rows, pg8000_cols = example_sales_order_table
# table_name = "sales_order" # not sure if this is hardcoding
# expected_object_key = return_s3_key__injection_bucket(table_name) # Action: I reckon there is a way to extract the table name from the variable metadata

#@pytest.mark.timeout(10)
class Test_read_s3_table_json:

    @pytest.mark.skip()
    def test_1_can_read_s3_json(self, s3_client, hardcoded_variables, return_list_of_cities_in_address_df):
        """
        this particially addresses:
            1. Must transform data landing in the "ingestion" S3 bucket
                a. we will have to simulate the previous S3 bucket (i think we need a fixture)
                b. we will have to read the data and get it into pandas?
        
        This test verifies that we can read the json and convert to pandas ready for next step

        Expected behavior:
        - read_s3_table_json(s3_client, s3_key)
            should:
            be able to see a pandas table with the rows and columns populated ready for use
        """
        # assemble
        list_table_names = hardcoded_variables["dict_table_snapshot_filepaths"].keys()
        target_table_name = list_table_names[0]
        now = datetime.now()
        original_invocation_time_string = now.strftime("%m/%d/%Y, %H:%M:%S")
        expected_cities = return_list_of_cities_in_address_df
        
        
        # act
        actual_df = read_s3_table_json(s3_client, return_s3_key__injection_bucket(target_table_name, original_invocation_time_string), hardcoded_variables["ingestion_bucket_name"], hardcoded_variables["processing_bucket_name"])
        
        
        # assert - the cities are extracting correctly
        assert list(actual_df["city"].values) == expected_cities
        
        # assert other things
        assert isinstance(actual_df, pd.Dataframe)
        
    def test_2_target_sales_table_is_created(s3_client, sales_order_table_columns, hardcoded_variables):
        """
        this particially addresses:
            1. Must place the results in the "processed" S3 bucket. 
                a. formats of tables? maybe start at one then get more advanced
                
        This test verifies that the parquet for the sales table is created, this may have to be refactored lol

        Expected behavior:
        - create_sales_table(s3_client, datetime_string)
            should:
            be able to see and read a parquet table with the rows and columns populated ready for later use
        """
        
        
        # assemble
        datetime_string = return_datetime_string()
        final_s3_key = return_s3_key__injection_bucket("sales_table", datetime_string)
        
        # act
        response = create_sales_table(s3_client, datetime_string)
        
        # assert - sales table exists
        respose_actual_tables_keys = s3_client.list_objects_v2(Bucket=hardcoded_variables["processing_bucket_name"])
        actual_tables_keys = [i["Key"] for i in respose_actual_tables_keys["Contents"]]
        assert actual_tables_keys == [final_s3_key]
            
        # assert TODO - sales table has the right columns
        actual_df_sales = simple_read_parquet_file_into_dataframe(hardcoded_variables["processing_bucket_name"], final_s3_key, s3_client)        
        actual_columns = actual_df_sales.columns.values()
        assert actual_columns == sales_order_table_columns
        
        # assert TODO - sales table has at least one matching columns values (all rows match) to our expected snapshot
        print("")
        
        # assert TODO - that the file is parquet
        print("")
        
    

        
        
        