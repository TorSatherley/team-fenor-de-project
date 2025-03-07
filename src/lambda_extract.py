import os
import json
from datetime import datetime, date
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pg8000.exceptions import DatabaseError

from src.utils import (
    get_secret,
    create_conn,
    close_db,
    get_rows_and_columns_from_table,
    write_table_to_s3,
    log_file,
)


secret_name = os.environ.get("SECRET_NAME")
bucket_name = os.environ.get("BUCKET_NAME")
sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")
# initialise last_checked dictionary to be populated in handler
last_checked = {}


def lambda_handler(event, context):
    """
    Ingestion Lambda handler function
    Collects data from totesys database and stores each table in .jsonl format
    in an s3 bucket.
    Triggered by a timed Eventbridge and collects only new or updated data
    from the database on each trigger.
    Parameters:
        event: Dict containing the Lambda function event data
        context: Lambda runtime context
    Returns:
        Dict containing status message
    """
    try:
        db_credentials = get_secret(sm_client, secret_name)
        conn = create_conn(db_credentials)
        keys = []
        # Get every table name in the database
        table_query = conn.run(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT LIKE '!_%' ESCAPE '!'"
        )
        table_names = [table[0] for table in table_query]
        date_and_time = datetime.today().strftime("%Y%m%d_%H%M%S")

        for table in table_names:
            # if the table doesnt have a last checked time, create it with old date so that it collects all data on first run
            if table not in last_checked:
                last_checked[table] = date(2000, 1, 1)
            # Query the table
            rows, columns, recent_check = get_rows_and_columns_from_table(
                conn, table, last_checked[table]
            )
            # update this table last checked with the most recent check timestamp
            last_checked[table] = recent_check
            # Convert to pandas df, format JSON file, and upload file to S3 bucket
            key = write_table_to_s3(
                s3_client, bucket_name, table, rows, columns, date_and_time
            )
            keys.append(key) 

        # Write log file to S3 bucket
        log_file(s3_client, bucket_name, keys)
        close_db(conn)
        print(
            f"Log: Batch extraction completed - {datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}"
        )
        return {"message": "Batch extraction job completed"}
    except (
        ClientError,
        NoCredentialsError,
        ValueError,
        json.JSONDecodeError,
        KeyError,
        DatabaseError,
        Exception,
    ) as e:
        print(f"Batch extraction job failed: {e}")
        return {"message": "Batch extraction job failed", "error": str(e)}
