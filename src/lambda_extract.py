import json
import os
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from pg8000.native import Connection


client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
bucket_name = "totesys-ingestion-zone-fenor"
s3_client = boto3.client("s3")


def get_secret(client):
    """Retrieves database secrets from AWS Secrets Manager."""
    secret_name = os.environ.get("SECRET_NAME")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response["SecretString"]
    secrets = json.loads(secret)

    return secrets


def create_conn():
    """Establishes a connection to the database using secrets."""
    db_credentials = get_secret(client)
    db_connection = Connection(
        database=db_credentials["dbname"],
        user=db_credentials["username"],
        password=db_credentials["password"],
        host=db_credentials["host"],
    )
    return db_connection


def close_db(conn):
    """Closes the database connection."""
    conn.close()


def get_rows_and_columns_from_table(conn, table):
    """Fetches rows and column names from a database table."""
    columns_query = conn.run(
        f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}'"
    )
    columns = [column[0] for column in columns_query]
    rows = conn.run(f"SELECT * FROM {table}")
    return rows, columns


def write_table_to_s3(s3_client, table, rows, columns):
    """Converts table data to JSON and uploads it to S3."""
    df = pd.DataFrame(data=rows, columns=columns)
    json_data = df.to_json(orient="records", lines=False, date_format="iso")
    static_time = datetime.now().strftime("%H%M")
    date_today = datetime.now().strftime("%Y%m%d")
    key = f"data/{date_today}_{static_time}/{table}.jsonl"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
    return key


def log_file(s3_client, keys):
    """Logs file upload details and writes to S3."""
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


def lambda_handler(event, context):
    """
    Ingestion Lambda handler function
    Parameters:
        event: Dict containing the Lambda function event data
        context: Lambda runtime context
    Returns:
        Dict containing status message
    """

    # Create connection
    conn = create_conn()
    try:
        keys = []

        # Loop through every tables in the database
        table_query = conn.run(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT LIKE '!_%' ESCAPE '!'"
        )
        table_names = [table[0] for table in table_query]
        for table in table_names:
            # Query the table
            rows, columns = get_rows_and_columns_from_table(conn, table)
            # Convert to pandas df, format JSON file, and upload file to S3 bucket
            key = write_table_to_s3(s3_client, table, rows, columns)
            keys.append(key)

        # Write log file to S3 bucket
        log_file(s3_client, keys)

        # Close connection
        close_db(conn)
        print(
            f"Log: Batch extraction completed - {datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}"
        )
        return {"message": "Batch extraction job completed"}
    except Exception as e:
        return {"message": "Batch extraction job failed", "error": str(e)}
