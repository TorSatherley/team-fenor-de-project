#import dask.dataframe as dd
import io
import pandas as pd
import json
import datetime
from src.util import return_week
from copy import copy


def read_s3_table_json(s3_client, s3_key, ingestion_bucket_name):
    """
    targets give json table in the injestion table and returns a df
    
    return s3_df
    """
    response = s3_client.get_object(Bucket=ingestion_bucket_name, Key=s3_key)
    jsonl_data = response['Body'].read().decode('utf-8')
     
    df = pd.DataFrame([json.loads(line) for line in jsonl_data.strip().split("\n")])
    
    return df



def _return_df_dim_dates(df_totesys_sales_order):
    """
    This is a placeholder
    
    initial draft of transformation processes
    
    this will be refactor but is a bit of a PoC (proof of concept)
    
    this method returns the df
    
    return df
    """
    
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
    
    # for col in columns:
    #     df_reduced[col] = df_reduced[col].apply(lambda x: x[:10])

    # data = []
    
    # df_dim_design = pd.DataFrame(data=df_totesys_design, columns=columns)
    # df_dim_design.set_index("design_id", inplace=True)
    return df_reduced

def populate_parquet_file(s3_client, datetime_string, table_name, df_file, bucket_name):
    pass
