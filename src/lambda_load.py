# import boto3
# import datetime

# s3 = boto3.client("s3")


# def lambda_handler(event, context):

#     print({"log": "Files Loaded successfully - {datetime.now()}"})
#     return {"message": f"Data is Loaded"}


########----------------------------

# trigger every time processed-bucket is changed (Terraform?)

# Idea: get lambda 2 to return the filename (key) of the folder that was just uploaded


from src.lambda_extract import get_secret, create_conn
import os
import boto3
import pandas as pd

# Get environment variables
secret_name = os.environ.get("SECRET_NAME")
bucket_name = os.environ.get("BUCKET_NAME")
sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")

# get newest folder (with all dims and facts) from S3-processed-bucket
# This is in the response of labda-2
# {message :fdfqd, latest_folder:"fsdqfds"}

def get_list_of_parquets(s3_client, latest_folder):
    """Returns a list of keys in side the latest folder"""
    pass
    #keys = [file_path for file_path in ]
    #return keys

# Convert from Parquet --> pd.DataFrame
# Alternative: input list of keys instead of single key
def parquet_to_pandas_df(parquet_file_key):
    pass
    df = pd.read_parquet(parquet_file_key)
    #df = pd.read_parquet(BytesIO(df_parquet_bytes))
    return df

# Upload each Parquet file to respective table in db (warehouse)

# Alternative: input list of dataframes instead of single dataframe
def upload_df_to_warehouse(sm_client, df):
    pass
    # return response


def lambda_handler(event, contenxt):
    try:
        s3_file_path = event["s3_file_path"]
        keys = get_list_of_parquets(s3_client, s3_file_path)
        dfs = parquet_to_pandas_df(keys)
        upload_df_to_warehouse(sm_client, dfs)
        return {"message": "Success"}
    except: # Choose exceptions to handle
        return {"message": "Failure"}