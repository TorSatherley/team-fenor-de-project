import json
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd
from pg8000.native import Connection


secret_name = os.environ.get("SECRET_NAME")
bucket_name = os.environ.get("BUCKET_NAME")
sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")


def get_secret(sm_client, secret_name):
    """Retrieves database secrets from AWS Secrets Manager."""
    if not secret_name:
        raise ValueError("SECRET_NAME environment variable is not set.")
    try:
        get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        secrets = json.loads(secret)
        return secrets
    except (ClientError, NoCredentialsError, json.JSONDecodeError, KeyError) as e:
        raise e


def create_conn(sm_client):
    """Establishes a connection to the database using secrets."""
    try:
        db_credentials = get_secret(sm_client, secret_name)
        db_connection = Connection(
            database=db_credentials["dbname"],
            user=db_credentials["username"],
            password=db_credentials["password"],
            host=db_credentials["host"],
        )
        return db_connection
    except (KeyError, ClientError, Exception) as e:
        print(f"Database connection error: {e}")
        raise e


def close_db(conn):
    """Closes the database connection."""
    try:
        conn.close()
    except Exception as e:
        print(f"Error closing database connection: {e}")
        raise e


def get_rows_and_columns_from_table(conn, table):
    """Fetches rows and column names from a database table."""
    try:
        columns_query = conn.run(
            f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}'"
        )
        columns = [column[0] for column in columns_query]
        rows = conn.run(f"SELECT * FROM {table}")
        return rows, columns
    except Exception as e:
        print(f"Error querying table {table}: {e}")
        return [], []


def write_table_to_s3(s3_client, bucket_name, table, rows, columns, date_and_time):
    """Converts table data to JSON and uploads it to S3."""
    try:
        if not rows or not columns:
            print(f"Skipping {table}: No data to upload.")
            return None
        df = pd.DataFrame(data=rows, columns=columns)
        json_data = df.to_json(orient="records", lines=False, date_format="iso")
        key = f"data/{date_and_time}/{table}.jsonl"
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
        return key
    except (ValueError, ClientError, Exception) as e:
        print(f"Error writing {table} to S3: {e}")
        return None


def log_file(s3_client, bucket_name, keys):
    """Logs file upload details and writes to S3."""
    try:
        if not keys:
            print("No files were uploaded to log.")
            return None
        log_contents = []
        for key in keys:
            log_contents.append(f"Uploaded: {key} at {datetime.now()}")
        formatted_log = "\n".join(log_contents)
        bytes_log = str.encode(formatted_log)
        s3_client.put_object(
            Body=bytes_log,
            Bucket=bucket_name,
            Key=f"logs/{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.log",
        )
        return {"message": "Files Processed: Batch Lambda Transform complete"}
    except ClientError as e:
        print(f"Error logging files to S3: {e}")


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
        # Create connection
        conn = create_conn(sm_client)

        keys = []

        # Get every table name in the database
        table_query = conn.run(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT LIKE '!_%' ESCAPE '!'"
        )
        table_names = [table[0] for table in table_query]

        date_and_time = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")

        for table in table_names:
            # Query the table
            rows, columns = get_rows_and_columns_from_table(conn, table)

            # Convert to pandas df, format JSON file, and upload file to S3 bucket
            key = write_table_to_s3(
                s3_client, bucket_name, table, rows, columns, date_and_time
            )
            keys.append(key)

        # Write log file to S3 bucket
        log_file(s3_client, bucket_name, keys)

        # Close connection
        close_db(conn)
        print(
            f"Log: Batch extraction completed - {datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}"
        )
        return {"message": "Batch extraction job completed"}
    except (ClientError, Exception) as e:
        print(f"Batch extraction job failed: {e}")
        return {"message": "Batch extraction job failed", "error": str(e)}
