import pytest
import boto3
from moto import mock_aws
from unittest.mock import Mock, patch
from datetime import datetime
from src.lambda_transform import lambda_handler
from datetime import datetime
from src.utils import json_to_pg8000_output, return_s3_key
from unittest import mock
from src.lambda_transform_utils import read_s3_table_json, _return_df_dim_dates, _return_df_dim_design,  populate_parquet_file, _return_df_dim_location, _return_df_dim_staff, _return_df_dim_currency, _return_df_fact_sales_order, _return_df_dim_counterparty
from src.utils import json_to_pg8000_output, return_datetime_string, write_table_to_s3, return_week
import pandas as pd
import io
from _pytest.monkeypatch import MonkeyPatch

"""
test_lambda_2_TDD.py
Author(s): Fabio Greenwood and Connor Creed

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
    - fix date columns(pressing on to sort other tables out)
    - Remove the hardcoding
    - ensure that development uses environmental variable to assign bucket name
    - populate: placeholder_function_for_checking_data_placed_into_s3_folder
"""




#%% Placeholder Variables and Functions - These are hard-coded currently and may need to be made more programmic

class DummyContext:
    pass

MOCK_ENVIROMENT = True


@pytest.fixture()
def hardcoded_variables():
    hardcoded_variables = {}
    hardcoded_variables["ingestion_bucket_name"] = "totesys-ingestion-zone-fenor-dummy-test-TDD-fabio-and-connor"
    hardcoded_variables["processing_bucket_name"] = "totesys-processing-zone-fenor-dummy-test-TDD-fabio-and-connor"
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


#%% fixtures


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
    
    yield s3_client, datetime_str

@pytest.fixture()
def s3_client_ingestion_populated_with_totesys_jsonl(s3_client, hardcoded_variables):
    # just populates a jsonl file into a mock bucket for unit testing
    datetime_str = return_datetime_string()
    jsonl_list = ["address","counterparty","currency","department","design","sales_order","staff"]
    for jsonl_file in jsonl_list:
        key = return_s3_key(jsonl_file, datetime_str)
        with open(f"data/json_lines_s3_format/{jsonl_file}.jsonl", "rb") as file:
            s3_client.put_object(Bucket=hardcoded_variables["ingestion_bucket_name"], Key=key, Body=file.read())
    
    
    yield s3_client, datetime_str

@pytest.fixture()
def example_sales_order_table(hardcoded_variables):
    simulated_pg8000_output, simulated_pg8000_output_cols = json_to_pg8000_output(hardcoded_variables["dict_table_snapshot_filepaths"]["sales_order"], include_cols_in_output=True)
    return simulated_pg8000_output, simulated_pg8000_output_cols

@pytest.fixture
def mock_s3_env_vars(monkeypatch, hardcoded_variables):
    monkeypatch = MonkeyPatch()
    monkeypatch.setenv("INJESTION_BUCKET_NAME", hardcoded_variables["ingestion_bucket_name"])
    monkeypatch.setenv("PROCESSED_BUCKET_NAME", hardcoded_variables["processing_bucket_name"])
    
        

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

@pytest.fixture()
def return_unique_dates_mentioned_in_first_10_rows_of_sale_table():
    # Fabio and Connor printed out the first 10 rows of the sample sales table and manually extracted the unique dates mentioned
    # this includes dates mentioned in datetimes
    list_unique_dates = ["2022-11-03", "2022-11-04", "2022-11-05", "2022-11-06", "2022-11-07", "2022-11-08", "2022-11-09", "2022-11-10", "2022-11-11", "2022-11-13"]
    return list_unique_dates



#%% tests

# pg8000_rows, pg8000_cols = example_sales_order_table
# table_name = "sales_order" # not sure if this is hardcoding
# expected_object_key = return_s3_key__injection_bucket(table_name) # Action: I reckon there is a way to extract the table name from the variable metadata

#@pytest.mark.timeout(10)
class TestReads3TableJson:
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


class TestCreateDateTable:
    """
    This is a test to see if we can create (and test) the creation of the dim_designs table with the "Sales" schema
    
    One could argue that this table is not very hard to transform
    
    However it is an initial experiment into transforming the data(and making sure we can test it with mock_AWS)
    
    Post test there will be more complex/comprehensive test sfor the larger requested star schemas (reusing much of this code)
    
    """
    
    def test_2a_dim_dates_table_is_created_in_correct_position(self, s3_client_ingestion_populated_with_totesys_sales_order_jsonl_inc_datetime_str, sales_order_table_columns, hardcoded_variables, return_unique_dates_mentioned_in_first_10_rows_of_sale_table):
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
        expected_dates = return_unique_dates_mentioned_in_first_10_rows_of_sale_table
        
        
        # act
        df_dim_dates = _return_df_dim_dates(df_totesys_sales_order[:hardcode_limit])
        response     = populate_parquet_file(s3_client, datetime_string, df_dim_dates_name, df_dim_dates, hardcoded_variables["processing_bucket_name"])
        
        # assert - df_dim_dates type
        assert isinstance(df_dim_dates, pd.DataFrame)
        
        # assert_unique_dates_exist
        
        
        
        actual_dates_stored = set(f"{YYYY}-{int(MM):02d}-{int(DD):02d}" for YYYY, MM, DD in zip(df_dim_dates["year"].values, 
                                                                              df_dim_dates["month"].values, 
                                                                              df_dim_dates["day"].values))
        assert set(expected_dates) == actual_dates_stored
        
        expected_year = [int(d[:4]) for d in expected_dates]
        expected_month = [int(d[5:7]) for d in expected_dates]
        expected_day= [int(d[8:10]) for d in expected_dates]

        _week = list(map(return_week, expected_dates))
        expected_day_of_the_week = [i[0] for i in _week]
        expected_day_name = [i[1] for i in _week]
        
        expected_month_name = ["november", "november", "november", "november", "november", "november", "november", "november", "november", "november"]
        expected_quater = [4, 4, 4, 4, 4, 4, 4, 4, 4, 4]

                # assert_correct_data
        ## index
        assert all(expected_dates == df_dim_dates.index.values)
        ## values
        assert all(expected_year == df_dim_dates['year'])
        assert all(expected_month == df_dim_dates["month"])
        assert all(expected_day == df_dim_dates["day"])
        assert all(expected_day_of_the_week == df_dim_dates["day_of_week"])
        assert all(expected_day_name == df_dim_dates["day_name"])
        assert all(expected_month_name == df_dim_dates["month_name"])
        assert all(expected_quater == df_dim_dates["quarter"].values)

        
class TestCreateDesignTables:
    
    def test_3a_dim_design_table_is_created_in_correct_position(self, s3_client_ingestion_populated_with_totesys_jsonl, hardcoded_variables):
        s3_client, datetime_string = s3_client_ingestion_populated_with_totesys_jsonl
        inj_file_key = return_s3_key("design", datetime_string)
        df_totesys_design = read_s3_table_json(s3_client, inj_file_key, hardcoded_variables["ingestion_bucket_name"])
        df_dim_design_name = "dim_design"
        hardcode_limit = 10 # this limits the size of the imported sales table so that a human can hardcode the expected values
        
        expected_design_id_values = [8, 51, 69, 16, 54, 10, 57, 41, 45, 2]
        expected_design_name_values = ["Wooden", "Bronze", "Bronze", "Soft", "Plastic", "Soft", "Cotton", "Granite", "Frozen", "Steel"]
        expected_file_location_values = ["\/usr", "\/private", "\/lost+found", "\/System", "\/usr\/ports", "\/usr\/share", "\/etc\/periodic", "\/usr\/X11R6", "\/Users", "\/etc\/periodic"]
        expected_file_name_values = ["wooden-20220717-npgz.json", "bronze-20221024-4dds.json", "bronze-20230102-r904.json", "soft-20211001-cjaz.json", "plastic-20221206-bw3l.json", "soft-20220201-hzz1.json", "cotton-20220527-vn4b.json", "granite-20220125-ifwa.json", "frozen-20221021-bjqs.json", "steel-20210725-fcxq.json"]
        
        # act
        df_dim_design = _return_df_dim_design(df_totesys_design[:hardcode_limit])
        response      = populate_parquet_file(s3_client, datetime_string, df_dim_design_name, df_dim_design, hardcoded_variables["processing_bucket_name"])

        # assert - df_dim_design type
        assert isinstance(df_dim_design, pd.DataFrame)
        
        # assert_correct_data
        ## index
        assert all(expected_design_id_values == df_dim_design.index.values)
        ## values
        assert all(expected_design_name_values == df_dim_design['design_name'])
        assert all(expected_file_location_values == df_dim_design["file_location"])
        assert all(expected_file_name_values == df_dim_design["file_name"])
        
        
        # # assert - response good
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        # assert - design parquet file exists
        response_list_of_s3_filepaths = s3_client.list_objects_v2(Bucket=hardcoded_variables["processing_bucket_name"])
        actual_s3_file_key_list = [i['Key'] for i in response_list_of_s3_filepaths['Contents']]
        assert set(actual_s3_file_key_list) == set(return_s3_key(table_name, datetime_string, extension=".parquet") for table_name in [df_dim_design_name])
        
        # assert - can be read as dataframe (and is saved as parquet)
        obj = s3_client.get_object(Bucket=hardcoded_variables["processing_bucket_name"], Key=return_s3_key(df_dim_design_name, datetime_string, extension=".parquet"))
        s3_file = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        # assert - df_dim_design type
        assert isinstance(s3_file, pd.DataFrame)

        #assert - correct data in s3 bucket  
        ## index 
        assert all(expected_design_id_values == s3_file.index.values)

        ## values
        assert all(expected_design_name_values == s3_file['design_name'])
        assert all(expected_file_location_values == s3_file["file_location"])
        assert all(expected_file_name_values == s3_file["file_name"])
         

class TestCreateLocationTables:
    def test_4a_dim_location_table_is_created_in_correct_position(self, s3_client_ingestion_populated_with_totesys_jsonl, hardcoded_variables):

        s3_client, datetime_string = s3_client_ingestion_populated_with_totesys_jsonl
        inj_file_key = return_s3_key("address", datetime_string)
        df_totesys_address = read_s3_table_json(s3_client, inj_file_key, hardcoded_variables["ingestion_bucket_name"])
        df_dim_location_name = "dim_location"
        hardcode_limit = 10 # this limits the size of the imported sales table so that a human can hardcode the expected values
        
        expected_adress_id_values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        expected_address_line_1_value = ["6826 Herzog Via", "179 Alexie Cliffs", "148 Sincere Fort", "6102 Rogahn Skyway", "34177 Upton Track", "846 Kailey Island", "75653 Ernestine Ways", "0579 Durgan Common", "644 Edward Garden", "49967 Kaylah Flat"]
        expected_address_line_2_value = [None, None, None, None, None, None, None, None, None, "Tremaine Circles"]
        expected_distric_value = ["Avon", None, None, "Bedfordshire", None, None, "Buckinghamshire", None, "Borders", "Bedfordshire"]
        expected_city = ["New Patienceburgh", "Aliso Viejo", "Lake Charles", "Olsonside", "Fort Shadburgh", "Kendraburgh", "North Deshaun", "Suffolk", "New Tyra", "Beaulahcester"]
        expected_postal_code_value = ["28441", "99305-7380", "89360", "47518", "55993-8850", "08841", "02813", "56693-0660", "30825-5672", "89470"]
        expected_country_value = ["Turkey", "San Marino", "Samoa", "Republic of Korea", "Bosnia and Herzegovina", "Zimbabwe", "Faroe Islands", "United Kingdom", "Australia", "Democratic People's Republic of Korea"]
        expected_phone_value = ["1803 637401", "9621 880720", "0730 783349", "1239 706295", "0081 009772", "0447 798320", "1373 796260", "8935 157571", "0768 748652", "4949 998070"]

        
        # act
        df_dim_location = _return_df_dim_location(df_totesys_address[:hardcode_limit])
        response        = populate_parquet_file(s3_client, datetime_string, df_dim_location_name, df_dim_location, hardcoded_variables["processing_bucket_name"])

        # assert - df_dim_location type
        assert isinstance(df_dim_location, pd.DataFrame)
        
        # assert_correct_data
        ## index
        assert all(expected_adress_id_values == df_dim_location.index.values)
        ## values
        assert all(expected_address_line_1_value == df_dim_location['address_line_1'])
        assert all(expected_address_line_2_value == df_dim_location["address_line_2"].values)
        assert all(expected_distric_value == df_dim_location["district"].values)
        assert all(expected_city == df_dim_location["city"])
        assert all(expected_postal_code_value == df_dim_location["postal_code"])
        assert all(expected_country_value == df_dim_location["country"])
        assert all(expected_phone_value == df_dim_location["phone"])
        

class TestCreateCounterpartyTables:

    def test_5a_dim_counterparty_is_created_in_correct_position_with_correct_data(self, s3_client_ingestion_populated_with_totesys_jsonl, hardcoded_variables):
        
        s3_client, datetime_string = s3_client_ingestion_populated_with_totesys_jsonl
        
        inj_file_key_counterparty   = return_s3_key("counterparty", datetime_string)
        inj_file_key_address       = return_s3_key("address", datetime_string)
        
        df_totesys_counterparty = read_s3_table_json(s3_client, inj_file_key_counterparty,  hardcoded_variables["ingestion_bucket_name"])
        df_dim_address          = read_s3_table_json(s3_client, inj_file_key_address,       hardcoded_variables["ingestion_bucket_name"])
        
        
        df_dim_counterparty_name = "dim_counterparty"
        hardcode_limit = 6 # this limits the size of the imported sales table so that a human can hardcode the expected values
        
        expected_counterparty_id_values = [1, 2, 3, 4, 5, 6]
        expected_counterparty_legal_name_values = ["Fahey and Sons", "Leannon, Predovic and Morar","Armstrong Inc","Kohler Inc","Frami, Yundt and Macejkovic", "Mraz LLC"]
        expected_counterparty_legal_address_line_1_values = ["605 Haskell Trafficway","079 Horacio Landing","179 Alexie Cliffs","37736 Heathcote Lock", "364 Goodwin Streets", "822 Providenci Spring"]
        expected_counterparty_legal_address_line_2_values = ["Axel Freeway", None, None, "Noemy Pines", None, None]
        expected_counterparty_legal_district_values = [None, None, None, None, None, "Berkshire"]
        expected_counterparty_legal_city_values = ["East Bobbie", "Utica", "Aliso Viejo", "Bartellview", "Sayreville", "Derekport"]
        expected_counterparty_legal_postal_code_values = ["88253-4257", "93045", "99305-7380", "42400-5199", "85544-4254", "25541"]
        expected_counterparty_legal_country_values = ["Heard Island and McDonald Islands", "Austria",  "San Marino", "Congo", "Svalbard & Jan Mayen Islands", "Papua New Guinea"]
        expected_counterparty_legal_phone_number_values = ["9687 937447", "7772 084705", "9621 880720", "1684 702261", "0847 468066", "4111 801405"]
        
        
        # act
        df_dim_counterparty = _return_df_dim_counterparty(df_totesys_counterparty[:hardcode_limit], df_dim_address)
        response            = populate_parquet_file(s3_client, datetime_string, df_dim_counterparty_name, df_dim_counterparty, hardcoded_variables["processing_bucket_name"])

        # assert - df_dim_design type
        assert isinstance(df_dim_counterparty, pd.DataFrame)
        
        
        # assert_correct_data
        ## index
        assert all(expected_counterparty_id_values    == df_dim_counterparty.index.values)
        ## values
        assert all(expected_counterparty_legal_name_values              == df_dim_counterparty["counterparty_legal_name"])
        assert all(expected_counterparty_legal_address_line_1_values    == df_dim_counterparty["counterparty_legal_address_line_1"])
        assert all(expected_counterparty_legal_address_line_2_values    == df_dim_counterparty["counterparty_legal_address_line_2"].values)
        assert all(expected_counterparty_legal_district_values          == df_dim_counterparty["counterparty_legal_district"].values)
        assert all(expected_counterparty_legal_city_values              == df_dim_counterparty["counterparty_legal_city"])
        assert all(expected_counterparty_legal_postal_code_values       == df_dim_counterparty["counterparty_legal_postal_code"])
        assert all(expected_counterparty_legal_country_values           == df_dim_counterparty["counterparty_legal_country"])
        assert all(expected_counterparty_legal_phone_number_values      == df_dim_counterparty["counterparty_legal_phone_number"])
        
        

        # # assert - response good
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        # assert - design parquet file exists
        response_list_of_s3_filepaths = s3_client.list_objects_v2(Bucket=hardcoded_variables["processing_bucket_name"])
        actual_s3_file_key_list = [i['Key'] for i in response_list_of_s3_filepaths['Contents']]
        assert set(actual_s3_file_key_list) == set(return_s3_key(table_name, datetime_string, extension=".parquet") for table_name in [df_dim_counterparty_name])
        
        # assert - can be read as dataframe (and is saved as parquet)
        obj = s3_client.get_object(Bucket=hardcoded_variables["processing_bucket_name"], Key=return_s3_key(df_dim_counterparty_name, datetime_string, extension=".parquet"))
        s3_file = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        # assert - df_dim_design type
        assert isinstance(s3_file, pd.DataFrame)

        #assert - correct data in s3 bucket  
        ## index 
        assert all(expected_counterparty_id_values == s3_file.index.values)

        ## values
                
        # assert_correct_data
        ## index
        assert all(expected_counterparty_id_values    == df_dim_counterparty.index.values)
        ## values
        assert all(expected_counterparty_legal_name_values              == s3_file["counterparty_legal_name"])
        assert all(expected_counterparty_legal_address_line_1_values    == s3_file["counterparty_legal_address_line_1"])
        assert all(expected_counterparty_legal_address_line_2_values    == s3_file["counterparty_legal_address_line_2"].values)
        assert all(expected_counterparty_legal_district_values          == s3_file["counterparty_legal_district"].values)
        assert all(expected_counterparty_legal_city_values              == s3_file["counterparty_legal_city"])
        assert all(expected_counterparty_legal_postal_code_values       == s3_file["counterparty_legal_postal_code"])
        assert all(expected_counterparty_legal_country_values           == s3_file["counterparty_legal_country"])
        assert all(expected_counterparty_legal_phone_number_values      == s3_file["counterparty_legal_phone_number"])
        
         
class TestCreatestaffTables:
    def test_6a_dim_staff_table_is_created_in_correct_position(self, s3_client_ingestion_populated_with_totesys_jsonl, hardcoded_variables):

        s3_client, datetime_string = s3_client_ingestion_populated_with_totesys_jsonl
        inj_file_key_staff = return_s3_key("staff", datetime_string)
        df_totesys_staff = read_s3_table_json(s3_client, inj_file_key_staff, hardcoded_variables["ingestion_bucket_name"])
        inj_file_key_department = return_s3_key("department", datetime_string)
        df_totesys_department = read_s3_table_json(s3_client, inj_file_key_department, hardcoded_variables["ingestion_bucket_name"])
        df_dim_staff_name = "dim_staff"
        hardcode_limit = 5 # this limits the size of the imported sales table so that a human can hardcode the expected values
        
        expected_staff_id_values = [1, 2, 3, 4, 5]
        expected_staff_first_name_values = ["Jeremie", "Deron", "Jeanette", "Ana", "Magdalena"]
        expected_staff_last_name_values = ["Franey", "Beier", "Erdman", "Glover", "Zieme",]
        expected_email_address_values = ["jeremie.franey@terrifictotes.com", "deron.beier@terrifictotes.com", "jeanette.erdman@terrifictotes.com", "ana.glover@terrifictotes.com", "magdalena.zieme@terrifictotes.com",]
        expected_location = ["Manchester","Manchester","Manchester", "Leeds", "Leeds"]
        expected_department_name = ["Purchasing", "Facilities", "Facilities", "Production",  "HR"]
        
        # act
        df_dim_staff = _return_df_dim_staff(df_totesys_staff[:hardcode_limit], df_totesys_department)
        response      = populate_parquet_file(s3_client, datetime_string, df_dim_staff_name, df_dim_staff, hardcoded_variables["processing_bucket_name"])

        # assert - df_dim_staff type
        assert isinstance(df_dim_staff, pd.DataFrame)

        # assert_correct_data
        ## index
        assert all(expected_staff_id_values == df_dim_staff.index.values)
        ## values
        assert all(expected_staff_first_name_values == df_dim_staff['first_name'])
        assert all(expected_staff_last_name_values == df_dim_staff["last_name"])
        assert all(expected_email_address_values == df_dim_staff["email_address"])
        assert all(expected_location == df_dim_staff["location"])
        assert all(expected_department_name == df_dim_staff["department_name"])
        

        # # assert - response good
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        # assert - design parquet file exists
        response_list_of_s3_filepaths = s3_client.list_objects_v2(Bucket=hardcoded_variables["processing_bucket_name"])
        actual_s3_file_key_list = [i['Key'] for i in response_list_of_s3_filepaths['Contents']]
        assert set(actual_s3_file_key_list) == set(return_s3_key(table_name, datetime_string, extension=".parquet") for table_name in [df_dim_staff_name])
        
        # assert - can be read as dataframe (and is saved as parquet)
        obj = s3_client.get_object(Bucket=hardcoded_variables["processing_bucket_name"], Key=return_s3_key(df_dim_staff_name, datetime_string, extension=".parquet"))
        s3_file = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        # assert - df_dim_design type
        assert isinstance(s3_file, pd.DataFrame)

        #assert - correct data in s3 bucket  
        ## index 
        assert all(expected_staff_id_values == s3_file.index.values)

        ## values
        assert all(expected_staff_first_name_values == s3_file['first_name'])
        assert all(expected_staff_last_name_values == s3_file["last_name"])
        assert all(expected_email_address_values == s3_file["email_address"])
        assert all(expected_location == s3_file["location"])
        
    
class TestCreatescurrencyTables:
    def test_7a_dim_currency_table_is_created_in_correct_position(self, s3_client_ingestion_populated_with_totesys_jsonl, hardcoded_variables):

        s3_client, datetime_string = s3_client_ingestion_populated_with_totesys_jsonl
        inj_file_key_staff = return_s3_key("currency", datetime_string)
        df_totesys_currency = read_s3_table_json(s3_client, inj_file_key_staff, hardcoded_variables["ingestion_bucket_name"])
        df_dim_currency_name = "dim_currency"
        
        
        expected_currency_id = [1, 2, 3]
        expected_currency_code = ["GBP", "USD", "EUR"]
        expected_currency_name = ["Great British Pounds", "United States Dollars", "Euro"]

        # act
        df_dim_currency = _return_df_dim_currency(df_totesys_currency)
        response      = populate_parquet_file(s3_client, datetime_string, df_dim_currency_name, df_dim_currency, hardcoded_variables["processing_bucket_name"])

        # assert - df_dim_staff type
        assert isinstance(df_dim_currency, pd.DataFrame)
        
        # assert_correct_data
        ## index
        assert all(expected_currency_id == df_dim_currency.index.values)
        ## values
        assert all(expected_currency_code == df_dim_currency['currency_code'])
        assert all(expected_currency_name == df_dim_currency["currency_name"])
        

        # # assert - response good
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        # assert - design parquet file exists
        response_list_of_s3_filepaths = s3_client.list_objects_v2(Bucket=hardcoded_variables["processing_bucket_name"])
        actual_s3_file_key_list = [i['Key'] for i in response_list_of_s3_filepaths['Contents']]
        
        assert set(actual_s3_file_key_list) == set(return_s3_key(table_name, datetime_string, extension=".parquet") for table_name in [df_dim_currency_name])
        # assert - can be read as dataframe (and is saved as parquet)
        obj = s3_client.get_object(Bucket=hardcoded_variables["processing_bucket_name"], Key=return_s3_key(df_dim_currency_name, datetime_string, extension=".parquet"))

        s3_file = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        # assert - df_dim_design type
        assert isinstance(s3_file, pd.DataFrame)

        #assert - correct data in s3 bucket  

        ## index
        assert all(expected_currency_id == s3_file.index.values)
        ## values
        assert all(expected_currency_code == s3_file['currency_code'])
        assert all(expected_currency_name == s3_file["currency_name"])


class TestCreatessalesOrderTables:
    def test_8a_fact_sales_order_table_is_created_in_correct_position(self, s3_client_ingestion_populated_with_totesys_jsonl, hardcoded_variables):

        s3_client, datetime_string = s3_client_ingestion_populated_with_totesys_jsonl
        inj_file_key_staff = return_s3_key("sales_order", datetime_string)
        df_totesys_sales_order = read_s3_table_json(s3_client, inj_file_key_staff, hardcoded_variables["ingestion_bucket_name"])
        df_fact_sales_order_name = "fact_sales_order"
        hardcode_limit = 10 # this limits the size of the imported sales table so that a human can hardcode the expected values
        
        expected_fact_sales_order_id = [1,2,3,4,5,6,7,8,9,10]
        expected_sales_order_id = [2, 3, 4, 5, 6, 7, 8, 10, 11, 12]
        expected_created_at = ["2022-11-03", "2022-11-03", "2022-11-03", "2022-11-03", "2022-11-04", "2022-11-04", "2022-11-04", "2022-11-07", "2022-11-07", "2022-11-09",]
        expected_created_time = ["14:20:52.186", "14:20:52.188", "14:20:52.188", "14:20:52.186", "11:37:10.341", "12:57:09.926", "13:45:10.306", "09:07:10.485", "15:53:10.153", "10:20:09.912"]
        expected_last_update = ["2022-11-03", "2022-11-03", "2022-11-03", "2022-11-03", "2022-11-04", "2022-11-04", "2022-11-04", "2022-11-07", "2022-11-07", "2022-11-09"]
        expected_last_update_time = ["14:20:52.186", "14:20:52.188", "14:20:52.188", "14:20:52.186", "11:37:10.341", "12:57:09.926", "13:45:10.306", "09:07:10.485", "15:53:10.153", "10:20:09.912"]
        expected_staff_id = [19, 10, 10, 18, 13, 11, 11, 16, 14, 8]
        expected_couterparty_id = [8, 4, 16, 4, 18, 10, 20, 12, 12, 12]
        expected_units_sold = [42972, 65839, 32069, 49659, 83908, 65453, 20381, 61620, 35227, 7693]
        expected_unit_price = [3.94, 2.91, 3.89, 2.41, 3.99, 2.89, 2.22, 3.86, 3.41, 3.88]
        expected_currency_id = [2, 3, 2, 3, 3, 2, 2, 2, 2, 2]
        expected_design_id = [3, 4, 4, 7, 3, 7, 2, 3, 9, 2]
        expected_agreed_delivery_date = ["2022-11-07", "2022-11-06", "2022-11-05", "2022-11-05", "2022-11-04", "2022-11-04", "2022-11-06", "2022-11-09", "2022-11-08", "2022-11-13"]
        expected_agreed_payment_date = ["2022-11-08", "2022-11-07", "2022-11-07", "2022-11-08", "2022-11-07", "2022-11-09", "2022-11-07", "2022-11-10", "2022-11-13", "2022-11-11"]
        expected_agreed_delivery_location_id = [8, 19, 15, 25, 17, 28, 8, 20, 13, 15]


        # act
        df_fact_sales_order = _return_df_fact_sales_order(df_totesys_sales_order[:hardcode_limit])
        response      = populate_parquet_file(s3_client, datetime_string, df_fact_sales_order_name, df_fact_sales_order, hardcoded_variables["processing_bucket_name"])

        # assert - df_fact_staff type
        assert isinstance(df_fact_sales_order, pd.DataFrame)
        
        # assert_correct_data
        ## index
        assert all(expected_fact_sales_order_id == df_fact_sales_order.index.values)
        ## values
        assert all(expected_sales_order_id == df_fact_sales_order['sales_order_id'])
        assert all(expected_created_at == df_fact_sales_order['created_date'])
        assert all(expected_created_time == df_fact_sales_order["created_time"])
        assert all(expected_last_update == df_fact_sales_order["last_updated_date"])
        assert all(expected_last_update_time == df_fact_sales_order["last_updated_time"])
        assert all(expected_staff_id == df_fact_sales_order["sales_staff_id"])
        assert all(expected_couterparty_id == df_fact_sales_order["counterparty_id"])
        assert all(expected_units_sold == df_fact_sales_order["units_sold"])
        assert all(expected_unit_price == df_fact_sales_order["unit_price"])
        assert all(expected_currency_id == df_fact_sales_order["currency_id"])
        assert all(expected_design_id == df_fact_sales_order["design_id"])
        assert all(expected_agreed_delivery_date == df_fact_sales_order["agreed_delivery_date"])
        assert all(expected_agreed_payment_date == df_fact_sales_order["agreed_payment_date"])
        assert all(expected_agreed_delivery_location_id == df_fact_sales_order["agreed_delivery_location_id"])
        

        # # assert - response good
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        # assert - design parquet file exists
        response_list_of_s3_filepaths = s3_client.list_objects_v2(Bucket=hardcoded_variables["processing_bucket_name"])
        actual_s3_file_key_list = [i['Key'] for i in response_list_of_s3_filepaths['Contents']]
        assert set(actual_s3_file_key_list) == set(return_s3_key(table_name, datetime_string, extension=".parquet") for table_name in [df_fact_sales_order_name])
        
        # assert - can be read as dataframe (and is saved as parquet)
        import io

        obj = s3_client.get_object(Bucket=hardcoded_variables["processing_bucket_name"], Key=return_s3_key(df_fact_sales_order_name, datetime_string, extension=".parquet"))
        s3_file = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        # assert - df_fact_design type
        assert isinstance(s3_file, pd.DataFrame)

        #assert - correct data in s3 bucket  
        ## index
        ## index
        assert all(expected_fact_sales_order_id == s3_file.index.values)
        ## values
        assert all(expected_created_at == s3_file['created_date'])
        assert all(expected_created_time == s3_file["created_time"])
        assert all(expected_last_update == s3_file["last_updated_date"])
        assert all(expected_last_update_time == s3_file["last_updated_time"])
        assert all(expected_staff_id == s3_file["sales_staff_id"])
        assert all(expected_couterparty_id == s3_file["counterparty_id"])
        assert all(expected_units_sold == s3_file["units_sold"])
        assert all(expected_unit_price == s3_file["unit_price"])
        assert all(expected_currency_id == s3_file["currency_id"])
        assert all(expected_design_id == s3_file["design_id"])
        assert all(expected_agreed_delivery_date == s3_file["agreed_delivery_date"])
        assert all(expected_agreed_payment_date == s3_file["agreed_payment_date"])
        assert all(expected_agreed_delivery_location_id == s3_file["agreed_delivery_location_id"])


class TestLambdaHandler_2:
    def test_9a_check_all_required_parquet_files_are_populated(self, s3_client_ingestion_populated_with_totesys_jsonl, hardcoded_variables, mock_s3_env_vars):
        
        s3_client, datetime_string = s3_client_ingestion_populated_with_totesys_jsonl
        
        # assemble
        expected_tables_list = ["fact_sales_order", "dim_date", "dim_staff", "dim_location", "dim_currency", "dim_design", "dim_counterparty"] #taken from sales schema code #https://dbdiagram.io/d/Copy-of-SampleDW-Sales-67cb1e50263d6cf9a09da951
        expected_file_keys = [return_s3_key(table_name, datetime_string, extension=".parquet") for table_name in expected_tables_list]
        event = {"datetime_string":datetime_string, "testing_client":s3_client}
        
        # act
        response = lambda_handler(event, DummyContext)
        response_list_of_s3_filepaths = s3_client.list_objects_v2(Bucket=hardcoded_variables["processing_bucket_name"])
        actual_s3_file_keys = [i['Key'] for i in response_list_of_s3_filepaths['Contents']]
        
        # assert
        assert response['statusCode'] == 200
        assert set(expected_file_keys) == set(actual_s3_file_keys)
        



