import os
import boto3
import pandas as pd

INGESTION_BUCKET = os.environ.get("INGESTION_BUCKET")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET")
s3_client = boto3.client("s3")

def lambda_handler(event, context):
    
    response = s3_client.list_objects_v2(Bucket=INGESTION_BUCKET, Prefix="data/", Delimiter="/")
    print(response)
    folders = sorted(
        [content["Prefix"] for content in response.get("CommonPrefixes", [])], reverse=True
    )
    print(folders)
    latest_folder = folders[0]
    print(latest_folder)

    print({"log": "Files Transformed successfully - {datetime.now()}"})
    return {
        "message": f"Processed Files in s3://{PROCESSED_BUCKET}"
    }
