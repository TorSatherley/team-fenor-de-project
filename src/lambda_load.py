# trigger every time processed-bucket is changed (Terraform?)

from src.lambda_extract import get_secret, create_conn, close_db
import os
import boto3
import pandas as pd
from pprint import pprint
import tempfile


# Get environment variables
secret_name = os.environ.get("SECRET_NAME")
bucket_name = os.environ.get("BUCKET_NAME")
sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")


def get_latest_folder():
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='data/', Delimiter="/")
    
    # data_folders = [
    #     "data/240228_233055/",
    #     "data/240228_240042/",
    #     "data/240229_003004/",
    #     "data/240301_023003/",
    #     "data/240229_010032/",
    #     "data/240302_000040/",
    #     "data/240301_013037/",
    #     "data/240229_020028/",
    #     "data/240229_023010/",
    #     "data/240229_030001/",
    #     "data/240229_233000/",
    #     "data/240301_020020/",
    #     "data/240301_000019/",
    #     "data/240302_010046/",
    #     "data/240301_003036/",
    #     "data/240229_013047/",
    #     "data/240302_003038/",
    #     "data/240301_010059/",
    #     "data/240301_030048/",
    #     "data/240301_233048/"
    # ]

    folders = [folder['Prefix'] for folder in response['CommonPrefixes']]
    # folders = [folder for folder in data_folders]
    folders.sort(reverse=True)
    
    latest_folder = folders[0]
    return latest_folder


def convert_parquets_to_dataframes(s3_client, latest_folder, bucket_name):
    """Returns a list of dataframes from each parquet in the latest folder"""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=latest_folder)

        dataframe_list = []

        for item in response["Contents"]:
            with tempfile.NamedTemporaryFile() as temp_file:
                s3_client.download_file(bucket_name, item["Key"], temp_file.name)
                df = pd.read_parquet(temp_file.name)
                dataframe_list.append(df)

        return dataframe_list
    except Exception as e:
        return {'message': f'Error: {e}'}


def upload_dfs_to_warehouse():
    df_list = convert_parquets_to_dataframes()
    print(df_list)

    # for each dataframe in df_list:
    #   format the query with tablename
    #   upload that dataframe to the corresponding table on the db
    # return success or failure response
    pass


def lambda_handler(event, context):
    try:
        s3_file_path = event["s3_file_path"]
        dfs = convert_parquets_to_dataframes(s3_client, s3_file_path, bucket_name)
        conn = create_conn(sm_client, secret_name)
        upload_dfs_to_warehouse(conn, dfs)
        close_db(conn)
        return {"message": "Successfully uploaded to database"}
    except Exception as e:  
        return {"message": f"Failure: {e}"}

