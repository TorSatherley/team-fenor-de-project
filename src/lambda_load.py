# trigger every time processed-bucket is changed (Terraform?)

from src.lambda_extract import get_secret, create_conn, close_db
from src.utils import get_secret
import os
from io import BytesIO
import boto3
import psycopg2
import pandas as pd
from pprint import pprint
from sqlalchemy.sql import text
from sqlalchemy import create_engine


secret_name = os.environ.get("SECRET_NAME")
bucket_name = os.environ.get("BUCKET_NAME")
sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")

# latest_folder="data/20250305_092045/"

# bucket_name = 'totesys-test-processed'
# s3_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=latest_folder)
# s3_files = [file['Key'] for file in s3_list['Contents']]

# def convert_parquets_to_dataframes():
#     try:

#         dfs = {}

#         for file in s3_files:
#             basename = os.path.splitext(os.path.basename(file))[0]
#             if basename == '':
#                 continue
#             s3_path = f's3://{bucket_name}/{latest_folder}{basename}.parquet'
#             dfs[basename] = pd.read_parquet(s3_path)
#         print("dataframes completed")
#         return dfs
#     except Exception as e:
#         return {'message': f'Error: {e}'}

def convert_parquets_to_dataframes(s3_client, bucket_name, latest_folder):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=latest_folder)
        dfs = []
        for file in response['Contents']:
            file_key = file['Key']
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            parquet_data = obj['Body'].read()
            df = pd.read_parquet(BytesIO(parquet_data))
            dfs.append(df)
        return dfs
    except Exception as e:
        return {"message": f"Error: {e}"}

# def db_connection(warehouse_credentials):
#     dbname = 'fenor_data_warehouse'
#     user='brendan'
#     password='password'
#     host='localhost'
#     port=5432
#     engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
#     return engine


def db_connection(warehouse_credentials):
    dbname = warehouse_credentials["database"]
    user = warehouse_credentials["user"]
    password = warehouse_credentials["password"]
    host = warehouse_credentials["host"]
    port = warehouse_credentials["port"]
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    return engine

def close_engine(engine):
    engine.dispose()




def upload_to_warehouse(engine, dfs):
    for table_name, df in dfs.items():
        df.to_sql(table_name, engine, if_exists="replace", index=False)
    
    return "success"

 
def lambda_handler(event, context):
    try:
        latest_folder = event["latest_folder"]
        dfs = convert_parquets_to_dataframes(s3_client, bucket_name, latest_folder)
        
        ### Real warehouse credentials
        warehouse_credentials = get_secret(sm_client, secret_name)
        ### Brendan warehouse crendentials
        #warehouse_credentials = { "user": 'brendan', "password": 'password', "database": 'fenor_data_warehouse', "port": 5432, "host": 'localhost'}

        engine = db_connection(warehouse_credentials)
        upload_to_warehouse(engine, dfs)
        close_engine(engine)
        return {"message": "Successfully uploaded to database"}
    except Exception as e:  
        return {"message": f"Failure: {e}"}

