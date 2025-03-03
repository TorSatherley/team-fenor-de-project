import boto3
import datetime

bucket_name = "totesys-processed-zone-fenor"
s3 = boto3.client("s3")


def lambda_handler(event, context):

    data_folder_key = "data/"
    log_folder_key = "log/"

    data = "Hello from FenorLand"
    data_batch_file = f"{data_folder_key}{datetime.datetime.now().isoformat()}.json"
    s3.put_object(Bucket=bucket_name, Key=data_batch_file, Body=data)

    log_file = f"{log_folder_key}{datetime.datetime.now().isoformat()}.log"
    s3.put_object(Bucket=bucket_name, Key=log_file, Body=data)

    print({"log": "Files Transformed successfully - {datetime.now()}"})
    return {"message": f"Processed Files in s3://{bucket_name}"}
