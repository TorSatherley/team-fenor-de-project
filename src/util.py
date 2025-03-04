import json
from  datetime import datetime, date
import boto3
import io
import pandas as pd



def json_to_pg8000_output(filepath, include_cols_in_output=True):
    """
    Reads the json and returns is as a nested list (the output format of p8000)
    Cols are also outputed as standard, as they will be needed as well

    """
    
    # Opening JSON file
    f = open(filepath,)
    simulated_pg8000_output = []
    simulated_pg8000_output_cols = []
    # returns JSON object as 
    # a dictionary
    data = json.load(f)
    
    for i in data:
        simulated_pg8000_output += [list(i.values())]
        
    for i in data[0].keys():
        simulated_pg8000_output_cols += [i]
    
    # Closing file
    f.close()
    
    
    return simulated_pg8000_output, simulated_pg8000_output_cols

def return_datetime_string():
    timestamp = datetime.now()
    year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute
    return f'{year}-{month}-{day}_{hour}-{minute}'

def return_s3_key(table_name, datetime_string):
    return f'data/{table_name}/{datetime_string}/{table_name}.json'


def simple_read_parquet_file_into_dataframe(bucket_name, key, s3_client):
    """source: https://stackoverflow.com/questions/51027645/how-to-read-a-single-parquet-file-in-s3-into-pandas-dataframe-using-boto3
    """
    # Read the parquet file
    buffer = io.BytesIO()
    object = s3_client.Object(bucket_name,key)
    object.download_fileobj(buffer)
    df = pd.read_parquet(buffer)
    return df
    
def return_week(date_isoformat_str):
    date_object = date.fromisoformat(date_isoformat_str)
    weekday_num = date_object.isoweekday()
    weekday_name_dict = {1 : "monday", 2 : "tuesday", 3 : "wednesday", 4 : "thursday", 5 : "friday", 6 : "saturday", 7 : "sunday"}
    weekday_name = weekday_name_dict[weekday_num]
    return weekday_num, weekday_name    

    