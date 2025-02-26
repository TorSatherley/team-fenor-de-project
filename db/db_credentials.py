import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import json

load_dotenv() # Must remove for AWS Lambda
client = boto3.client(service_name='secretsmanager', region_name="eu-west-2")


def get_secret(client):

    secret_name = os.environ.get('SECRET_NAME')
    region_name = "eu-west-2"


    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    secrets = json.loads(secret)

    return { "secret": {
        "username": secrets['username'],
        "password": secrets['password'],
        "dbname": secrets['dbname'],
        "port": secrets['port'],
        "engine": secrets['engine'],
        "host": secrets['host']
    }}


