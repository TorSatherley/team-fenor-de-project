import os
import boto3
import csv
from db.connection import create_conn, close_db
from datetime import datetime
import json


conn = create_conn()

table_names = conn.run("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT LIKE '!_%' ESCAPE '!'")


for row in table_names:
    table_name = row[2]
    columns = [column['name'] for column in conn.columns]
    path_str = "data/test.json"
    request_string = f"""COPY (
      SELECT *
      FROM {table_name};
    ) to {path_str}';"""
    table_data = conn.run(request_string)

    
