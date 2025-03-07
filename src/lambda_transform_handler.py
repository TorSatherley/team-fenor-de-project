"""
File: lambda_2_handler.py (likely to be renamed)
Author: Connor Creed and Fabio Greenwood
Date: 2025-03-07
Description: Handler for function that transforms data from ingestion state to processed state (i.e. in-line with business requirements)
"""

import requests
import boto3
import os
import json
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime as dt
from random import random, randint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
URL = "https://zenquotes.io/api/random/"


def lambda_handler(event, context):
    """Main handler 
    
    inputs:
        event["timestamp"] <- timestamp used in previous operation
        
    actions:
        triggers all util functions, which in turn achieve all required goals
    """
    try:
        
        
        @populate_into_table
        produce_df_dim_whatsit()
        
        
        
        s3_client = boto3.client("s3")
        output_data = {"quotes": []}
        #timestamp = str(int(dt.timestamp(dt.now())))
        #quotes = [get_quote() for _ in range(3)]
        #output_data["quotes"] = [resp for status, resp in quotes if status == 200]
        #if random() < 0.1:
        #    quote = output_data["quotes"][randint(0, 2)]
        #    content = quote["content"]
        #    logger.info("[GREAT QUOTE] %s", content)
        #key = f"quote_{timestamp}.json"
        #write_result = write_to_s3(s3_client, output_data, BUCKET_NAME, key)
        if write_result:
            logger.info("Wrote quotes to S3")
        else:
            logger.info("There was a problem. Quotes not written.")
    except Exception as e:
        logger.info(f"Unexpected Exception: %s", str(e))