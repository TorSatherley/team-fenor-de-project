import boto3
import io
import os
import psycopg2
import pandas as pd
import json
import time 

from src.lambda_transform_utils import (return_s3_key)

def load_connection():
    try:
        conn = psycopg2.connect(
            dbname = 'postgres',
            user = 'project_team_6',
            password = 'maSaxIhJnmv4bOk',
            host = 'nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com',
            port=5432
            )
        return conn
    except Exception as e:
        return {"message": str(e)}
    
def dw_cleanup():
    conn = load_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fact_sales_order")
    cursor.execute("DELETE FROM dim_counterparty")
    cursor.execute("DELETE FROM dim_currency")
    cursor.execute("DELETE FROM dim_date")
    cursor.execute("DELETE FROM dim_design")
    cursor.execute("DELETE FROM dim_location")
    cursor.execute("DELETE FROM dim_staff")
    conn.commit()
    return {"message": "Date Warehouse restored to default state."}


def lambda_handler(event, context):
    try:
        # Connection
        dw_cleanup()
        print("Loading started...")
        start_time = time.time()
        conn = load_connection()
        cursor = conn.cursor()

        bucket_name = 'totesys-processed-zone-fenor'
        latest_folder = 'data/'
        s3_client = boto3.client("s3", region_name="eu-west-2")
        #s3_list = s3_client.list_objects_v2(Bucket=bucket_name)
        #s3_files = [file['Key'] for file in s3_list['Contents']]

        list_of_tables = ["dim_date", "dim_design", "dim_location", "dim_counterparty", "dim_staff", "dim_currency", "fact_sales_order"]

        # Insert statement
        for file in list_of_tables:
            #print(f"s3_files: {s3_files}")
            #table_name = os.path.splitext(os.path.basename(file))[0]
            #s3_key = f'{latest_folder}{table_name}.parquet'
            #if s3_key == 'data/.parquet':
            #    continue
            s3_key = return_s3_key(file, event["datetime_string"], extension=".parquet")
            print(f"s3_key:{s3_key}")
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            # Read parquet file
            parquet_data = s3_response["Body"].read()
            df = pd.read_parquet(io.BytesIO(parquet_data))
            df = df.reset_index()

            # Limit for time factor
            if file == "sales_order":
                df = df.head(6000)

            # Build Insert query
            df_columns = df.columns.tolist()
            df_columns = ", ".join(df_columns)
            id_column = df_columns.split(",")[0].strip()
            df_placeholders = ", ".join(["%s"] * len(df.columns))

            insert_query = f"""
            INSERT INTO {file} 
            ({df_columns})
            VALUES 
            ({df_placeholders})
            """
            print(f"insert_query:{insert_query}")

            for _, row in df.iterrows():
                row_dict = row.to_dict()
                cursor.execute(insert_query, tuple(row_dict.values()))
            conn.commit()

        cursor.close()
        conn.close()
        end_time = time.time()
        execution_time = end_time - start_time 
        print(execution_time / 60)
        return {"message": "Successfully uploaded to data warehouse"}
    except Exception as e:
        return {'message': f'Error: {e}'}
    
#print(lambda_handler({"datetime_string":"20250311_151013"},{}))