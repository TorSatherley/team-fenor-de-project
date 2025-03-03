#import dask.dataframe as dd
import io
import pandas as pd



def read_s3_table_json(s3_client, s3_key, ingestion_bucket_name, processing_bucket_name):
    """
    targets give json table in the injestion table and returns a df
    
    return s3_df
    """
    response = s3_client.get_object(Bucket=ingestion_bucket_name, Key=s3_key)
    df = pd.read_table(io.BytesIO(response['Body'].read()))
    print(df)
    return df
    

def create_sales_table(s3_client, datetime_string):
    """initial draft of transformation processes
    
    this will be refactor but is a bit of a PoC (proof of concept)
    
    this method only returns a responce, it reads and populates directly the passed buckets
    
    return response
    """
    
    pass 

