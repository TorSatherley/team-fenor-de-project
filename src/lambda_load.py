# trigger every time processed-bucket is changed (Terraform?)

from src.lambda_extract import get_secret, create_conn, close_db
import os
import boto3
import psycopg2
import pandas as pd
from pprint import pprint
from sqlalchemy.sql import text
from sqlalchemy import create_engine


# Get environment variables
secret_name = os.environ.get("SECRET_NAME")
# bucket_name = os.environ.get("BUCKET_NAME")
bucket_name = 'totesys-test-processed'
sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")

LATEST_FOLDER="data/20250305_092045/"

def convert_parquets_to_dataframes():
    try:
        s3_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=LATEST_FOLDER)
        s3_files = [file['Key'] for file in s3_list['Contents']]

        dfs = {}

        for file in s3_files:
            basename = os.path.splitext(os.path.basename(file))[0]
            if basename == '':
                continue
            s3_path = f's3://{bucket_name}/{LATEST_FOLDER}{basename}.parquet'
            dfs[basename] = pd.read_parquet(s3_path)
        print("dataframes completed")
        return dfs
    except Exception as e:
        return {'message': f'Error: {e}'}  

def db_connection():
    dbname = 'fenor_data_warehouse'
    user='brendan'
    password='password'
    host='localhost'
    port=5432
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    return engine

def upload_to_warehouse():
    engine = db_connection()
    dfs = convert_parquets_to_dataframes()

    for table_name, df in dfs.items():
        print("processing_table: ", table_name)
        print("dataframe is: ", df.head())
        dataframe = dfs[table_name]
        dataframe.to_sql(table_name, engine, if_exists="replace", index=False)

    engine.dispose()
    return "success"

print(upload_to_warehouse())





   



# def convert_parquets_to_dataframes(s3_client, latest_folder, bucket_name):
#     """Returns a list of dataframes from each parquet in the latest folder"""
#     try:
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=latest_folder)

#         dataframe_list = []

#         for item in response["Contents"]:
#             with tempfile.NamedTemporaryFile() as temp_file:
#                 s3_client.download_file(bucket_name, item["Key"], temp_file.name)
#                 df = pd.read_parquet(temp_file.name)
#                 dataframe_list.append(df)

#         return dataframe_list
#     except Exception as e:
#         return {'message': f'Error: {e}'}
    




# dfs = convert_parquets_to_dataframes(s3_client, s3_file_path, bucket_name)


# def upload_dfs_to_warehouse(conn, dfs):
#     for df 


#     # for each dataframe in df_list:
#     #   format the query with tablename
#     #   upload that dataframe to the corresponding table on the db
#     # return success or failure response
#     return {"message": "Dataframes added"}


def lambda_handler(event, context):
    try:
        s3_file_path = event["s3_file_path"]
        conn = create_conn(sm_client, secret_name)

        close_db(conn)
        return {"message": "Successfully uploaded to database"}
    except Exception as e:  
        return {"message": f"Failure: {e}"}

