import os
import json
from pg8000.native import Connection
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError # To be used to handle errors when implementing try-excpet block in lambda_handler
import pandas as pd
import io
from datetime import datetime


client = boto3.client(service_name='secretsmanager', region_name="eu-west-2")
bucket_name = "totesys-ingestion-zone-fenor"

def get_secret(client):

    load_dotenv()

    secret_name = 'totesys-db-credentials'

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

    # Credentials
    db_credentials = get_secret(client)

    db_connection = Connection(
        database=db_credentials['secret']['dbname'],
        user=db_credentials['secret']['username'],
        password=db_credentials['secret']['password'],
        host=db_credentials['secret']['host']
    )

    return db_connection

# Create S3 client
s3_client = boto3.client('s3')
conn = create_conn()

def lambda_handler(event, context):
    """
    Ingestion Lambda handler function
    Parameters:
        event: Dict containing the Lambda function event data
        context: Lambda runtime context
    Returns:
        Dict containing status message
    """
    try:
        # Connect to RDS database (NorthCoders AWS)

        keys = []

        # Loop through every tables in the database
        table_query = conn.run("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT LIKE '!_%' ESCAPE '!'")
        table_names =  [table[0] for table in table_query]
        
        for table in table_names:
            # Query the table
            #columns_query = conn.run(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}'")
            #columns = [column[0] for column in columns_query]
            rows = conn.run(f"SELECT * FROM {table}")
            columns = conn.columns()

            # Convert to pandas df, format JSON file, and upload file to S3 bucket
            key = write_table_to_s3(s3_client, rows, columns, bucket_name, table)
            keys.append(key)
        
        # Write log file
        log_file(keys)

        conn.close()
        print("Lambda extracted successfully")
        return {"message": "Success"}
    except ClientError as e:
        return {"message": "Error"}

        
def write_table_to_s3(s3_client, rows, columns, bucket, table):
    timestamp = datetime.now()
    year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute
    key = f'data/{table}/{year}-{month}-{day}_{hour}-{minute}/{table}.json'
    df = pd.DataFrame(data=rows, columns=columns)
    json_buffer = io.StringIO()
    df.to_json(json_buffer, index=False, orient='records', lines=False, date_format='iso')
    s3_client.put_object(Bucket=bucket, Key=key, Body=json_buffer.getvalue())
    return key

def log_file(keys):
    log_contents = []

    for key in keys:
        log_contents.append(f'Uploaded: {key} at {datetime.now()}')

    formatted_log = "\n".join(log_contents)
    bytes_log = str.encode(formatted_log)

    s3_client.put_object(Body=bytes_log, Bucket=bucket_name, Key=f"logs/{datetime.today().strftime('%Y-%m-%d_%H-%S')}.log")