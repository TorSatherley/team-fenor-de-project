import boto3
import datetime
import json

def lambda_handler(event, context):
    s3 = boto3.client("s3")

    data = "Helo from Fenor"
    file_content_example = json.dumps(data, indent=2)
    
    bucket_name = "totesys-ingestion-zone-fenor"
    data_folder_key = "data_table/"
    log_folder_key = "log/"

    data = "Hello from FenorLand"
    data_batch_file = f"{data_folder_key}{datetime.datetime.now().isoformat()}.json"
    s3.put_object(Bucket=bucket_name, Key=data_batch_file, Body=data)

    list_s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix="data_table/")

    s3_contents = [s3_obj["Key"] for s3_obj in list_s3_objects['Contents']]

    # Example: if 2 files in bucket -> create a log file
    if len(s3_contents) >= 2:
        log_file_key = f"{log_folder_key}{datetime.datetime.now().isoformat()}.log"
        s3.put_object(Bucket=bucket_name, Key=log_file_key, Body=file_content_example)

    return {
        "message": "Files Processed: Batch Lambda Transform complete"
    }

print(lambda_handler({}, {}))