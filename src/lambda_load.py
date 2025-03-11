import boto3
import io
import os
import psycopg2
import pandas as pd
import json
import time 

def load_connection():
    try:
        region_name = "eu-west-2"
        sm_client = boto3.client('secretsmanager', region_name=region_name)
        get_secret_value_response = sm_client.get_secret_value(
            SecretId="data-warehouse-credentials"
        )

        creds = get_secret_value_response['SecretString']
        dw_access = json.loads(creds)

        conn = psycopg2.connect(
            dbname=dw_access['database'], 
            user=dw_access['user'], 
            password=dw_access['password'], 
            host=dw_access['host']
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
        # event['datetime_string'] = 'data/20250305_092045/'
        bucket_name = 'totesys-test-processed'
        latest_folder = 'data/20250305_092045/'
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=latest_folder)
        s3_files = [file['Key'] for file in s3_list['Contents']]
        # Insert statement
        for file in s3_files:
            table_name = os.path.splitext(os.path.basename(file))[0]
            if table_name == '':
                continue
            s3_key = f'{latest_folder}{table_name}.parquet'
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            
            # Read parquet file
            parquet_data = s3_response["Body"].read()
            df = pd.read_parquet(io.BytesIO(parquet_data))

            df = df.reset_index()

            # Limit for time factor
            if table_name == "fact_sales_order":
                df = df.head(6000)

            # Build Insert query
            df_columns = df.columns.tolist()
            df_columns = ", ".join(df_columns)
            id_column = df_columns.split(",")[0].strip()
            df_placeholders = ", ".join(["%s"] * len(df.columns))
            insert_query = f"""
            INSERT INTO {table_name} 
            ({df_columns})
            VALUES 
            ({df_placeholders})
            """


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
    
print(lambda_handler({},{}))