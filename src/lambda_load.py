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
sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")

latest_folder="data/20250305_092045/"

bucket_name = 'totesys-test-processed'
s3_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=latest_folder)
s3_files = [file['Key'] for file in s3_list['Contents']]

def convert_parquets_to_dataframes():
    try:

        dfs = {}

        for file in s3_files:
            basename = os.path.splitext(os.path.basename(file))[0]
            if basename == '':
                continue
            s3_path = f's3://{bucket_name}/{latest_folder}{basename}.parquet'
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

engine = db_connection()
dfs = convert_parquets_to_dataframes()

def upload_to_warehouse(engine):
    for table_name, df in dfs.items():
        df.to_sql(table_name, engine, if_exists="replace", index=False)
    engine.dispose()
    return "success"

 
def lambda_handler(event, context):
    try:

        s3_file_path = event["s3_file_path"]
        conn = create_conn(sm_client, secret_name)
        upload_to_warehouse(engine)
        close_db(conn)
        return {"message": "Successfully uploaded to database"}
    except Exception as e:  
        return {"message": f"Failure: {e}"}

