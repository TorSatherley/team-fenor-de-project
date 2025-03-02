import boto3
import json
import os
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError, ParamValidationError
from pg8000.native import Connection
from dotenv import load_dotenv

def get_secret():
    secrets_client = boto3.client(
        service_name="secretsmanager", region_name="eu-west-2")
    secret_name = os.environ.get("SECRET_NAME")
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        db_credentials = json.loads(secret)
        return db_credentials
    except ClientError as e:
        return {"error": str(e)}


def db_connection():
    try:
        db_credentials = get_secret()

        db_connection = Connection(
            database=db_credentials['dbname'],
            user=db_credentials["username"],
            password=db_credentials["password"],
            host=db_credentials["host"],
        )
        return db_connection
    except KeyError as e:
        return {"error": "Missing credentials from DB connection"}

bucket_name = "totesys-ingestion-zone-fenor"
# bucket_name = "test-totesys"

def lambda_handler(event, context):
    """
    Lambda Extract handler function
    Parameters:
        event: Dict containing the Lambda function event data
        context: Lambda runtime context
    Returns:
        Dict containing status message
    """

    keys = []
    s3_client = boto3.client("s3")

    db = db_connection()

    static_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        # Loop through every table in the database
        table_query = db.run(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT LIKE '!_%' ESCAPE '!'"
        )
        table_names = [table[0] for table in table_query]

        for table in table_names:
            # Get columns and rows for each table
            columns_query = db.run(
                f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}'"
            )
            columns = [column[0] for column in columns_query]
            rows = db.run(f"SELECT * FROM {table}")

            # Convert to pandas df, format JSON file, and upload file to S3
            df = pd.DataFrame(data=rows, columns=columns)
            json_data = df.to_json(
                orient="records", lines=False, date_format="iso")
            key = f"data/{static_timestamp}/{table}.jsonl"

            s3_client.put_object(Bucket=bucket_name,
                                 Key=key, Body=json_data)

            keys.append(key)

        # Log file upload details and write to S3 (optional)
        log_contents = [f"Uploaded: {key} at {datetime.now()}" for key in keys]
        formatted_log = "\n".join(log_contents)
        bytes_log = str.encode(formatted_log)
        s3_client.put_object(
            Body=bytes_log,
            Bucket=bucket_name,
            Key=f"logs/{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.log",
        )

        db.close()
        print(
            f"Log: Batch extraction completed - {datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}")
        return {"message": "Batch extraction job completed"}
    except ClientError as e:
        return {"error": str(e)}
