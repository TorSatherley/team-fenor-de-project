#import dask.dataframe as dd
import io
import pandas as pd
import json



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
    pass 


def populate_parquet_file(s3_client, datetime_string, table_name, df_file, bucket_name):
    pass