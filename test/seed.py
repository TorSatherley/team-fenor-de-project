from pg8000.native import Connection
from dotenv import load_dotenv
import os
import json
import boto3


def seed_conn(database="postgres"):
    db_connection = Connection(
        user="postgres",
        database="postgres",
        password="password",
        host="localhost",
    )
    db_connection.run("REVOKE CONNECT ON DATABASE test_totesys FROM public;")
    db_connection.run("""SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = 'test_totesys';""")
    return db_connection

seeding = seed_conn()

def drop_database(seeding):
    seeding.run('DROP DATABASE IF EXISTS test_totesys;')
    seeding.run('CREATE DATABASE test_totesys;')
    print("Database created")

drop_database(seeding)

def get_secret_test(client):

    load_dotenv()

    # Add to .env
    # TEST_DB="test_totesys_secret"
    secret_name = os.environ.get('TEST_DB')


    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )


    secret = get_secret_value_response['SecretString']
    secrets = json.loads(secret)

    print(secrets)

    return secrets


def create_conn():
    load_dotenv()
    # db_credentials = get_secret_test(secrets_client)
    db_credentials = Connection(
        user=os.environ.get("TEST_DB_USER"),
        database="test_totesys",
        password=os.environ.get("TEST_DB_PASSWORD"),
        host="localhost",
    )
    return db_credentials

conn = create_conn()


def insert_data(conn):
    query = """
    DROP TABLE IF EXISTS test_db;
    CREATE TABLE test_db (
        sales_order_id INT PRIMARY KEY,
        created_at TIMESTAMP,
        last_updated TIMESTAMP,
        design_id INT,
        staff_id INT,
        counterparty_id INT,
        units_sold INT,
        unit_price NUMERIC(10, 2),
        currency_id INT,
        agreed_delivery_date DATE,
        agreed_payment_date DATE,
        agreed_delivery_location_id INT
    );

    INSERT INTO test_db (
        sales_order_id, created_at, last_updated, design_id, staff_id, counterparty_id, units_sold,
        unit_price, currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id
    ) VALUES
    (2, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 3, 19, 8, 42927, 3.94, 2, '2022-11-07', '2022-11-08', 8),
    (3, '2022-11-03 14:20:52.188', '2022-11-03 14:20:52.188', 4, 10, 4, 65839, 2.91, 3, '2022-11-06', '2022-11-07', 19),
    (4, '2022-11-03 14:20:52.188', '2022-11-03 14:20:52.188', 3, 10, 16, 32069, 3.89, 2, '2022-11-05', '2022-11-07', 15),
    (5, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186', 7, 13, 4, 49659, 2.41, 3, '2022-11-05', '2022-11-08', 12),
    (6, '2022-11-04 11:37:10.341', '2022-11-04 11:37:10.341', 3, 13, 18, 83908, 3.99, 2, '2022-11-04', '2022-11-10', 17),
    (7, '2022-11-04 12:57:09.926', '2022-11-04 12:57:09.926', 7, 11, 10, 65453, 2.89, 2, '2022-11-04', '2022-11-09', 28),
    (8, '2022-11-04 13:45:10.306', '2022-11-04 13:45:10.306', 2, 16, 20, 20381, 2.22, 2, '2022-11-06', '2022-11-07', 8),
    (10, '2022-11-07 09:07:10.485', '2022-11-07 09:07:10.485', 12, 12, 12, 61620, 3.86, 2, '2022-11-09', '2022-11-10', 20),
    (11, '2022-11-07 15:53:10.153', '2022-11-07 15:53:10.153', 3, 12, 3, 7693, 3.88, 2, '2022-11-13', '2022-11-13', 15),
    (12, '2022-11-09 10:20:09.912', '2022-11-09 10:20:09.912', 7, 17, 7, 2845, 2.97, 3, '2022-11-16', '2022-11-14', 3),
    (13, '2022-11-09 15:16:10.492', '2022-11-09 15:16:10.492', 7, 13, 5, 82159, 2.65, 3, '2022-11-15', '2022-11-14', 9),
    (14, '2022-11-10 13:18:09.926', '2022-11-10 13:18:09.926', 6, 11, 5, 74957, 3.25, 3, '2022-11-12', '2022-11-15', 28),
    (15, '2022-11-11 08:51:10.286', '2022-11-11 08:51:10.286', 9, 7, 13, 36220, 2.00, 3, '2022-11-20', '2022-11-19', 2);
    """
    conn.run(query)
    print("Database information added")    


insert_data(conn)

print("tables inserted")
conn.close()