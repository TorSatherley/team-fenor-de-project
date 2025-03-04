import pytest
import boto3
from moto import mock_aws
import json
from unittest.mock import Mock, patch
from pprint import pprint
from datetime import datetime
from src.lambda_ingest_dummy import write_table_to_s3, lambda_handler
from datetime import datetime
from src.util import json_to_pg8000_output, return_s3_key
from unittest import mock
from src.lambda_2 import read_s3_table_json, _return_df_dim_dates, populate_parquet_file
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
        a. start off with just doing one simple table (this will imform your decisions)
    
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
def s3_client_ingestion_populated_with_totesys_sales_order_jsonl_inc_datetime_str(s3_client, hardcoded_variables):
    # just populates a single jsonl file into a mock bucket for unit testing
    datetime_str = return_datetime_string()
    key = return_s3_key("sales_order", datetime_str)
    
    # act
    with open("data/json_lines_s3_format/sales_order.jsonl", "rb") as file:
        s3_client.put_object(Bucket=hardcoded_variables["ingestion_bucket_name"], Key=key, Body=file.read())
    
    with open("data/json_lines_s3_format/address.jsonl", "rb") as file:
            s3_client.put_object(Bucket=hardcoded_variables["ingestion_bucket_name"], Key=inj_file_key, Body=file.read())
    
    
    
    yield s3_client, datetime_str

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

@pytest.fixture()
def s3_client__populated_bucket(s3_client, hardcoded_variables):
    # placeholder puesdocode
    
    placeholder_shadow_realm_bucket_name = "i dunno"
    
    for key in hardcoded_variables["dict_table_snapshot_filepaths"]:
        rows, cols = json_to_pg8000_output(hardcoded_variables["dict_table_snapshot_filepaths"][key])
        write_table_to_s3(s3_client, rows, cols, placeholder_shadow_realm_bucket_name, key)
    
    yield s3_client


@pytest.fixture()    
def return_list_of_cities_in_address_df():
    list_of_cities = ["New Patienceburgh", "Aliso Viejo", "Lake Charles", "Olsonside", "Fort Shadburgh", "Kendraburgh", "North Deshaun", "Suffolk", "New Tyra", "Beaulahcester", "Corpus Christi", "Pricetown", "Shanahanview", "Maggiofurt", "East Bobbie", "South Wyatt", "Hackensack", "Lake Arne", "West Briellecester", "Pueblo", "Fresno", "Sayreville","Derekport", "New Torrance", "East Arvel", "Napa", "Oakland Park", "Utica", "Bartellview", "Lake Myrlfurt"]
    return list_of_cities
    
@pytest.fixture()
def sales_order_table_columns():
    list_sales_order_columns = ["created_at", "last_updated", "design_id", "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"]
    return list_sales_order_columns



#%% Tests

# pg8000_rows, pg8000_cols = example_sales_order_table
# table_name = "sales_order" # not sure if this is hardcoding
# expected_object_key = return_s3_key__injection_bucket(table_name) # Action: I reckon there is a way to extract the table name from the variable metadata

#@pytest.mark.timeout(10)
class Test_read_s3_table_json:

    #@pytest.mark.skip
    def test_1a_can_read_s3_json(self, s3_client, hardcoded_variables, return_list_of_cities_in_address_df):
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
        list_table_names = list(hardcoded_variables["dict_table_snapshot_filepaths"].keys())
        target_table_name = list_table_names[0] # this is likely the "address" table name
        now = datetime.now()
        original_invocation_time_string = now.strftime("%m/%d/%Y, %H:%M:%S")
        expected_cities = return_list_of_cities_in_address_df
        inj_file_key = return_s3_key(target_table_name, original_invocation_time_string)
        with open("data/json_lines_s3_format/address.jsonl", "rb") as file:
            s3_client.put_object(Bucket=hardcoded_variables["ingestion_bucket_name"], Key=inj_file_key, Body=file.read())
        
        # act
        actual_df_addresses_table = read_s3_table_json(s3_client, inj_file_key, hardcoded_variables["ingestion_bucket_name"])
        
        
        # assert - the cities are extracting correctly
        assert list(actual_df_addresses_table["city"].values) == expected_cities
        
        # assert other things
        assert isinstance(actual_df_addresses_table, pd.DataFrame)



class Test_create_designs_table:
    """
    This is a test to see if we can create (and test) the creation of the dim_designs table with the "Sales" schema
    
    One could argue that this table is not very hard to transform
    
    However it is an initial experiment into transforming the data(and making sure we can test it with mock_AWS)
    
    Post test there will be more complex/comprehensive test sfor the larger requested star schemas (reusing much of this code)
    
    """
    def test_2a_dim_dates_table_is_created_in_correct_position(self, s3_client_ingestion_populated_with_totesys_sales_order_jsonl_inc_datetime_str, sales_order_table_columns, hardcoded_variables):
        """
        this particially addresses:
            1. Must place the results in the "processed" S3 bucket. 
                a. create dim_dates table and is in the correct_position
                
        This test verifies that the parquet for the sales table is created, this may have to be refactored lol

        Expected behavior:
        input: df
        
        
        - read_s3_table_json()
        
        
        XXXXXXXXXXXXXXXXXX NEXT ACTION XXXXXXXXXXXXXXXXXX:
         - make a ficture with s3_popualuted_design_table
        
            
        """
        # assemble
        s3_client, datetime_string = s3_client_ingestion_populated_with_totesys_sales_order_jsonl_inc_datetime_str
        inj_file_key = return_s3_key("sales_order", datetime_string)
        df_totesys_sales_order = read_s3_table_json(s3_client, inj_file_key, hardcoded_variables["ingestion_bucket_name"])
        df_dim_dates_name = "dim_dates"
        hardcode_limit = 10 # this limits the size of the imported sales table so that a human can hardcode the expected values
        
        print(" ------- df_totesys_sales_order ------- ")
        print(df_totesys_sales_order[:hardcode_limit])
        
        # act
        df_dim_dates = _return_df_dim_dates(df_totesys_sales_order[:hardcode_limit])
        response     = populate_parquet_file(s3_client, datetime_string, df_dim_dates_name, df_dim_dates, hardcoded_variables["processing_bucket_name"])
        
        print("  ")
        print(" ------- df_dim_dates ------- ")
        print(df_dim_dates.head(hardcode_limit))
        
        
        # assert - df_dim_dates type
        assert isinstance(df_dim_dates, pd.DataFrame)
        
        
        # # assert - response good
        # assert response["message"] == "success"
        # 
        # # assert - design parquet file exists
        # respose_actual_tables_keys = s3_client.list_objects_v2(Bucket=hardcoded_variables["processing_bucket_name"])
        # actual_tables_keys = [i["Key"] for i in respose_actual_tables_keys["Contents"]]
        # assert actual_tables_keys == [return_s3_key(df_dim_dates_name, datetime_string)]
            
        # assert TODO - parquet has right columns
        
        # assert TODO - parquet table has at least one matching columns values (all rows match) to our expected snapshot
        print("")
        
        # assert TODO - that the file is parquet
        print("")
        
    
