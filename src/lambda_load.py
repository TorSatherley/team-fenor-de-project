# import boto3
# import datetime

# s3 = boto3.client("s3")


# def lambda_handler(event, context):

#     print({"log": "Files Loaded successfully - {datetime.now()}"})
#     return {"message": f"Data is Loaded"}


########----------------------------

# trigger every time processed-bucket is changed (Terraform?)

# Idea: get lambda 2 to return the filename (key) of the folder that was just uploaded


from src.lambda_extract import get_secret, create_conn, close_db
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

def convert_parquets_to_dataframes(s3_client, latest_folder):
    """Returns a list of dataframes from each parquet in the latest folder"""
    pass
    # dataframe_list = []
    # for parq in parqs in latestfolder: << s3.listobjects prefixed with latest_folder name
    #   df = pd.read_parquet(parquet_file_keys, s3_client) << might be this?
    #   df = pd.read_parquet(BytesIO(df_parquet_bytes)) << maybe this one?
    #   dataframe_list.append(df)
    # return dataframe_list


# Upload each Parquet file to respective table in db (warehouse)
def upload_df_to_warehouse(conn, df_list):
    # for each dataframe in df_list:
    #   format the query with tablename
    #   upload that dataframe to the corresponding table on the db
    # return response
    pass


def lambda_handler(event, contenxt):
    try:
        s3_file_path = event["s3_file_path"]
        dfs = convert_parquets_to_dataframes(s3_client, s3_file_path)
        conn = create_conn(sm_client, secret_name)
        upload_df_to_warehouse(conn, dfs)
        close_db()
        return {"message": "Success"}
    except: # Choose exceptions to handle
        return {"message": "Failure"}