import io
import os
from datetime import datetime
import json

import boto3
from botocore.exceptions import ClientError
from pg8000.native import Connection
import pandas as pd
from dotenv import load_dotenv  # Must remove for AWS Lambda

# Load .env file
load_dotenv()  # Must remove for AWS Lambda


def get_secret(client):
    """Retrieves credentials from AWS Secrets Manager."""
    secret_name = os.environ.get("SECRET_NAME")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response["SecretString"]
    secrets = json.loads(secret)

    return {
        "secret": {
            "username": secrets["username"],
            "password": secrets["password"],
            "dbname": secrets["dbname"],
            "port": secrets["port"],
            "engine": secrets["engine"],
            "host": secrets["host"],
        }
    }


def create_conn():
    """Establishes a database connection using credentials from AWS Secrets Manager."""
    client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")

    # Credentials
    db_credentials = get_secret(client)

    db_connection = Connection(
        database=db_credentials["secret"]["dbname"],
        user=db_credentials["secret"]["username"],
        password=db_credentials["secret"]["password"],
        host=db_credentials["secret"]["host"],
    )

    return db_connection


def close_db(conn):
    """Closes the database connection."""
    conn.close()


def get_rows_and_columns_from_table(conn, table):
    """Fetches rows and column names from a database table."""
    rows = conn.run(f"SELECT * FROM {table}")
    columns = [col["name"] for col in conn.columns]
    return rows, columns


def write_table_to_s3(s3_client, rows, columns, bucket, table):
    """Writes table data to an S3 bucket in JSON format."""
    timestamp = datetime.now()
    year, month, day, hour, minute = (
        timestamp.year,
        timestamp.month,
        timestamp.day,
        timestamp.hour,
        timestamp.minute,
    )
    key = f"data/{table}/{year}/{month:02d}/{day:02d}/{table}_{hour:02d}-{minute:02d}.json"
    df = pd.DataFrame(data=rows, columns=columns)
    json_buffer = io.StringIO()
    df.to_json(
        json_buffer, index=False, orient="records", lines=False, date_format="iso"
    )
    s3_client.put_object(Bucket=bucket, Key=key, Body=json_buffer.getvalue())
    return key


def log_file(keys, bucket):
    """Creates a log file in S3 to track uploaded data files."""
    log_contents = []
    for key in keys:
        log_contents.append(f"Uploaded: {key} at {datetime.now()}")
    formatted_log = "\n".join(log_contents)
    bytes_log = str.encode(formatted_log)
    s3_client.put_object(
        Body=bytes_log,
        Bucket=bucket,
        Key=f"logs/{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.log",
    )


# Create S3 client and Secrets Manager client
s3_client = boto3.client("s3")
SM_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")

# Get environment variables
S3_BUCKET_INGESTION = os.getenv("S3_BUCKET_INGESTION")
secrets = get_secret(SM_client)["secret"]
DB_USER = secrets["username"]
DB_PASSWORD = secrets["password"]
DB_NAME = secrets["dbname"]
DB_HOST = secrets["host"]
DB_PORT = secrets["port"]


def lambda_handler(event, context):
    """AWS Lambda handler function to retrieve database tables and store them in S3."""
    try:
        # Connect to RDS database (NorthCoders AWS)
        conn = create_conn()

        # Save key name for each table
        keys = []

        # Loop through every tables in the database
        tables = [
            "counterparty",
            "currency",
            "department",
            "design",
            "staff",
            "sales_order",
            "address",
            "payment",
            "purchase_order",
            "payment_type",
            "transaction",
        ]
        for table in tables:
            # Query the table
            rows, columns = get_rows_and_columns_from_table(conn, table)

            # Convert to pandas df, format CSV file, and upload file to S3 bucket
            key = write_table_to_s3(
                s3_client, rows, columns, S3_BUCKET_INGESTION, table
            )
            keys.append(key)

        # Write log file
        log_file(keys, S3_BUCKET_INGESTION)

        # Close the connection to RDS database
        close_db(conn)
        return {"message": "Success"}
    except ClientError as e:
        return {"message": "Error", "details": str(e)}


if __name__ == "__main__":  # Must remove for AWS Lambda
    result = lambda_handler(None, None)  # Must remove for AWS Lambda
    print(result)  # Must remove for AWS Lambda
