#import dask.dataframe as dd
import io
import pandas as pd
import json
import datetime
from src.util import return_week, return_s3_key
from copy import copy
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from io import BytesIO



def read_s3_table_json(s3_client, s3_key, ingestion_bucket_name):
    """
    targets give json table in the injestion table and returns a df
    
    return s3_df
    """
    response = s3_client.get_object(Bucket=ingestion_bucket_name, Key=s3_key)
    jsonl_data = response['Body'].read().decode('utf-8')
    df = pd.DataFrame([json.loads(line.replace("\\", "\\\\")) for line in jsonl_data.strip().split("\n")])
    
    return df


def populate_parquet_file(s3_client, datetime_string, table_name, df_file, bucket_name):
    
    try:
        key = return_s3_key(table_name, datetime_string)
        table = pa.Table.from_pandas(df_file)
    
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)  # Reset buffer positio
        response = s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())

        return response
    except ClientError as e:
        return {"message": "Error", "details": str(e)}


def _return_df_dim_dates(df_totesys_sales_order):
    #%% produce unique dates mentioned
    
    # reduce to just datetime and date columns
    list_target_columns = ["created_at", "last_updated", "agreed_delivery_date", "agreed_payment_date"    ]
    df_reduced = df_totesys_sales_order.loc[:, list_target_columns]
    
    # trim off datetimes
    for col in list_target_columns:
        df_reduced[col] = df_reduced[col].apply(lambda x: x[:10])
        #df_reduced[col] = df_reduced[col].astype('date64') # slow, also date64 likely wrong spelling
    
    # finalise dates in table
    all_values = []
    for x in list(df_reduced.values):
        all_values += list(x)
    unique_list_of_dates = list(set(all_values))
    unique_list_of_dates.sort()
    
    #%% produce entries for this date
    
    months_dict = {1:"january",2:"february",3:"march",4:"april",5:"may",6:"june",7:"july",8:"august",9:"september",10:"october",11:"november",12:"december"}
    data = []
    for i, d in enumerate(unique_list_of_dates):
        weekday_num, weekday_name = return_week(d)
        months_name = months_dict[int(d[5:7])]
        quarter_int = (int(d[5:7]) // 4) + 1
        data += [[i, int(d[:4]), f"{int(d[5:7]):02d}", f"{int(d[8:10]):02d}", weekday_num, weekday_name, months_name, quarter_int]]
    index = range(len(data))
    columns = ["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"]
    df_dim_dates = pd.DataFrame(data=data, columns=columns)
    
    df_dim_dates.set_index("date_id", inplace=True)
    return df_dim_dates
    
    
def _return_df_dim_design(df_totesys_design):
    
    columns = ['design_id', 'design_name', "file_location", "file_name"]
    df_design_copy = copy(df_totesys_design)
    df_reduced = df_design_copy.loc[:,columns]
    df_reduced.set_index("design_id", inplace=True)
    
    return df_reduced


def _return_df_dim_location(df_totesys_address):

    columns = ['address_id', 'address_line_1', "address_line_2", "district", "city", "postal_code", "country", "phone"]
    df_location_copy = copy(df_totesys_address)
    df_reduced = df_location_copy.loc[:,columns]
    df_reduced.set_index("address_id", inplace=True)
    
    return df_reduced

    
def _return_df_dim_counterparty(df_totesys_counterparty, df_dim_location):
        
    df_count    = copy(df_totesys_counterparty[["counterparty_id", "counterparty_legal_name", "legal_address_id"]])
    df_addy     = copy(df_dim_location[["address_id","address_line_1","address_line_2","district","city","postal_code","country","phone"]])
    df_merged = pd.merge(df_count, df_addy, left_on='legal_address_id', right_on='address_id')
    df_merged.rename(columns={"counterparty_id" : "counterparty_id", "counterparty_legal_name" : "counterparty_legal_name", "address_line_1" : "counterparty_legal_address_line_1", "address_line_2" : "counterparty_legal_address_line_2", "district" : "counterparty_legal_district", "city" : "counterparty_legal_city", "postal_code" : "counterparty_legal_postal_code", "country" : "counterparty_legal_country", "phone" : "counterparty_legal_phone_number"}, inplace=True)
    df_merged.drop(["address_id"], axis=1, inplace=True)
    
    df_merged.set_index("counterparty_id", inplace=True)
    
    df_merged.to_csv("test.csv")
    
    return df_merged

 
def _return_df_dim_staff(df_totesys_staff, df_totesys_department):
    # Ensure the required columns exist in both DataFrames
    expected_staff_columns = ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address']
    expected_department_columns = ['department_id', 'department_name', 'location']

    # Select and validate staff columns
    df_staff_copy = copy(df_totesys_staff)
    df_staff_reduced = df_staff_copy.loc[:,expected_staff_columns]

    # Select and validate department columns
    df_department_copy = copy(df_totesys_department)
    df_department_reduced = df_department_copy.loc[:,expected_department_columns]

    # Merge staff with department data
    df_merged = df_staff_reduced.merge(df_department_reduced, on="department_id")

    # Select required columns, ensuring they exist in the merged DataFrame
    selected_columns = ["staff_id", 'first_name', 'last_name', 'department_name', 'location', 'email_address']
    df_final = df_merged.loc[:,selected_columns]
    df_final.set_index("staff_id", inplace=True)

    return df_final


def _return_df_dim_currency(df_totesys_currency):
    columns = ["currency_id", "currency_code", "currency_name"]
    currency_name_values = {"GBP": "Great British Pounds","USD": "United States Dollars","EUR": "Euro"}
    df_currency_copy = copy(df_totesys_currency)
    df_currency_copy["currency_name"] = df_currency_copy["currency_code"].map(currency_name_values)
    df_reduced = df_currency_copy.loc[:,columns]
    df_reduced.set_index("currency_id", inplace=True)
    
    return df_reduced


def _return_df_fact_sales_order(df_totesys_sales_order):
    columns = ["sales_record_id", "sales_order_id", "created_date", "created_time", "last_updated_date", "last_updated_time", "sales_staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "design_id", "agreed_payment_date", "agreed_delivery_date", "agreed_delivery_location_id"]
    df_sales_order_copy = copy(df_totesys_sales_order) 

    df_sales_order_copy['created_at'] = pd.to_datetime(df_sales_order_copy['created_at'])
    df_sales_order_copy['last_updated'] = pd.to_datetime(df_sales_order_copy['last_updated'])
    df_sales_order_copy['sales_record_id'] = range(1, len(df_sales_order_copy)+1)

    df_sales_order_copy['created_date'] = df_sales_order_copy["created_at"].dt.date.astype(str)
    df_sales_order_copy['created_time'] = df_sales_order_copy["created_at"].dt.strftime('%H:%M:%S.%f').str[:-3]

    df_sales_order_copy['last_updated_date'] = df_sales_order_copy["last_updated"].dt.date.astype(str)
    df_sales_order_copy['last_updated_time'] = df_sales_order_copy["last_updated"].dt.strftime('%H:%M:%S.%f').str[:-3]
    df_sales_order_copy['sales_staff_id'] = df_sales_order_copy["staff_id"]

    df_reduced = df_sales_order_copy.loc[:,columns]
    df_reduced.set_index("sales_record_id", inplace=True)

    return df_reduced
    

