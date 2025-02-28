import boto3
import datetime

s3 = boto3.client("s3")

def lambda_handler(event, context):
   
    print({"log": "Files Loaded successfully - {datetime.now()}"})
    return {
        "message": f"Data is Loaded"
    }