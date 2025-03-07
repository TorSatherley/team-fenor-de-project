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
from botocore.exceptions import ClientError, NoCredentialsError
from pg8000.exceptions import DatabaseError
import logging
from datetime import datetime
from random import random, randint
from lambda_transform_utils import read_s3_table_json, _return_df_dim_dates, _return_df_dim_design, _return_df_dim_location, populate_parquet_file, _return_df_dim_counterparty, _return_df_dim_staff, _return_df_dim_currency, _return_df_fact_sales_order, return_s3_key
from src.util import get_secret, create_conn, close_db, get_rows_and_columns_from_table, write_table_to_s3, log_file





logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


BUCKET_NAME = os.environ["S3_BUCKET_NAME"]











secret_name = os.environ.get("SECRET_NAME")
bucket_name = os.environ.get("BUCKET_NAME")
sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
s3_client = boto3.client("s3", region_name="eu-west-2")


def lambda_handler(event, context):
    """
    Ingestion Lambda handler function
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
            # Query the table
            rows, columns = get_rows_and_columns_from_table(conn, table)
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











ingestion_bucket_name = os.environ.get("INJESTION_BUCKET_NAME")
processed_bucket_name = os.environ.get("PROCESSED_BUCKET_NAME")
s3_client = boto3.client("s3", region_name="eu-west-2")



def lambda_handler(event, context):
    """Main handler 
    
    inputs:
        event["timestamp"] <- timestamp used in previous operation
        
    actions:
        triggers all util functions, which in turn achieve all required goals
        
    notes:
        if memory ever becomes an issue this can be refactored to load/delete dataframes as needed (i.e. >> del df_totesys_design)
        
    """
    try:
        # set up connection     
    
        #read_s3_table_json(s3_client, s3_key, ingestion_bucket_name)
        
        # variables prep
        datetime_string = event["date_and_time"]
        s3_client = boto3.client("s3")
        output_data = {"quotes": []}
        
        
        # read injestion files
        df_totesys_sales_order  = read_s3_table_json(s3_client, return_s3_key("sales_order",    datetime_string, extension=".parquet"), ingestion_bucket_name)
        df_totesys_design       = read_s3_table_json(s3_client, return_s3_key("design",         datetime_string, extension=".parquet"), ingestion_bucket_name)
        df_totesys_address      = read_s3_table_json(s3_client, return_s3_key("address",        datetime_string, extension=".parquet"), ingestion_bucket_name)
        df_totesys_counterparty = read_s3_table_json(s3_client, return_s3_key("counterparty",   datetime_string, extension=".parquet"), ingestion_bucket_name)
        df_totesys_staff        = read_s3_table_json(s3_client, return_s3_key("staff",          datetime_string, extension=".parquet"), ingestion_bucket_name)
        df_totesys_department   = read_s3_table_json(s3_client, return_s3_key("department",     datetime_string, extension=".parquet"), ingestion_bucket_name)
        df_totesys_currency     = read_s3_table_json(s3_client, return_s3_key("currency",       datetime_string, extension=".parquet"), ingestion_bucket_name)
        
        
        # produce and populate
        df_dim_dates = _return_df_dim_dates(df_totesys_sales_order)
        populate_parquet_file(s3_client, datetime_string, "dim_dates", df_dim_dates, processed_bucket_name)

        df_dim_design = _return_df_dim_design(df_totesys_design)
        populate_parquet_file(s3_client, datetime_string, "dim_design", df_dim_design, processed_bucket_name)

        df_dim_location = _return_df_dim_location(df_totesys_address)
        populate_parquet_file(s3_client, datetime_string, "dim_location", df_dim_location, processed_bucket_name)

        df_dim_counterparty = _return_df_dim_counterparty(df_totesys_counterparty, df_dim_location)
        populate_parquet_file(s3_client, datetime_string, "dim_counterparty", df_dim_counterparty, processed_bucket_name)

        df_dim_staff = _return_df_dim_staff(df_totesys_staff, df_totesys_department)
        populate_parquet_file(s3_client, datetime_string, "dim_staff", df_dim_staff, processed_bucket_name)

        df_dim_currency = _return_df_dim_currency(df_totesys_currency)
        populate_parquet_file(s3_client, datetime_string, "dim_currency", df_dim_currency, processed_bucket_name)

        df_fact_sales_order = _return_df_fact_sales_order(df_totesys_sales_order)
        populate_parquet_file(s3_client, datetime_string, "fact_sales_order", df_fact_sales_order, processed_bucket_name)


        
        
        
        
        
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