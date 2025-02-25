import os
import json
from dotenv import load_dotenv # Must remove for AWS Lambda
import pg8000.native
import boto3
from botocore.exceptions import ClientError # To be used to handle errors when implementing try-excpet block in lambda_handler
import logging # To be used for logging
import pandas as pd
import io


load_dotenv() # Must remove for AWS Lambda

# Get environment variables
S3_BUCKET_INGESTION = os.getenv('S3_BUCKET_INGESTION')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 5432))

# Create S3 client
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

    # Connect to RDS database (NorthCoders AWS)
    conn = connect_to_db(DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT)

    # Loop through every tables in the database
    tables = ["counterparty", "currency", "department", "design", "staff", "sales_order", "address", "payment", "purchase_order", "payment_type", "transaction"]
    for table in tables:
        # Query the table
        rows, columns = get_rows_and_columns_from_table(conn, table)

        # Convert to pandas df, format CSV file, and upload file to S3 bucket
        write_table_to_s3(s3_client, rows, columns, S3_BUCKET_INGESTION, f"{table}.csv")

    # Close the connection to RDS database
    close_db_connection(conn)
    

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


def connect_to_db(user, password, database, host, port):
    return pg8000.native.Connection(
        user=user, 
        password=password,
        database=database,
        host=host,
        port=port
    )

def close_db_connection(conn):
    conn.close()

def get_rows_and_columns_from_table(conn, table):
    rows = conn.run(f"SELECT * FROM {table}")
    columns = [col['name'] for col in conn.columns]
    return rows, columns

def write_table_to_s3(s3_client, rows, columns, bucket, key):
    df = pd.DataFrame(data=rows, columns=columns)
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())



if __name__  == "__main__": # Must remove for AWS Lambda
    lambda_handler(None, None) # Must remove for AWS Lambda