import boto3
import json
import os
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime
from pg8000.native import Connection


client = boto3.client(service_name='secretsmanager', region_name="eu-west-2")
bucket_name = "totesys-ingestion-zone-fenor"

def get_secret(client):

    load_dotenv()

    secret_name = os.environ.get('SECRET_NAME')

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    secrets = json.loads(secret)

    return { "secret": {
        "username": secrets['username'],
        "password": secrets['password'],
        "dbname": secrets['dbname'],
        "port": secrets['port'],
        "engine": secrets['engine'],
        "host": secrets['host']
    }}

def create_conn():

    db_credentials = get_secret(client)

    db_connection = Connection(
        database=db_credentials['secret']['dbname'],
        user=db_credentials['secret']['username'],
        password=db_credentials['secret']['password'],
        host=db_credentials['secret']['host']
    )

    return db_connection

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Ingestion Lambda handler function
    Parameters:
        event: Dict containing the Lambda function event data
        context: Lambda runtime context
    Returns:
        Dict containing status message
    """
    conn = create_conn()
    
    try:

        keys = []

        # Loop through every tables in the database
        table_query = conn.run("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT LIKE '!_%' ESCAPE '!'")
        table_names =  [table[0] for table in table_query]
        
        for table in table_names:
            # Query the table
            columns_query = conn.run(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}'")
            columns = [column[0] for column in columns_query]
            col = conn.columns
            print(col)
            rows = conn.run(f"SELECT * FROM {table}")

            # Convert to pandas df, format JSON file, and upload file to S3 bucket
            key = write_table_to_s3(table, rows, columns)
            keys.append(key)
        
        # Write log file
        log_file(keys)

        conn.close()
        print(f"Log: Batch extraction completed - {datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}")
        return {"message": "Batch extraction job completed"}
    except ClientError as e:
        return {"error": e}

def write_table_to_s3(table, rows, columns):
    df = pd.DataFrame(data=rows, columns=columns)
    json_data = df.to_json(orient='records', lines=False, date_format='iso')

    static_time = datetime.now().strftime('%H%M')
    date_today = datetime.now().strftime('%Y%m%d')



    key = f"data/{date_today}_{static_time}/{table}.json"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
    return key



def log_file(keys):
    log_contents = []


    for key in keys:
        log_contents.append(f'Uploaded: {key} at {datetime.now()}')

    formatted_log = "\n".join(log_contents)
    bytes_log = str.encode(formatted_log)

    s3_client.put_object(Body=bytes_log, Bucket=bucket_name, Key=f"logs/{datetime.today().strftime('%Y-%m-%d_%H-%S')}.log")

    return {"message": "Files Processed: Batch Lambda Transform complete"}


