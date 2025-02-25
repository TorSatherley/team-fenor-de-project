import os
from pg8000.native import Connection
from db_credentials import get_secret


def create_conn():

    # Credentials
    db_credentials = get_secret()

    db_connection = Connection(
        database=db_credentials["secret"]["dbname"],
        user=db_credentials["secret"]["username"],
        password=db_credentials["secret"]["password"],
        host=db_credentials["secret"]["host"],
    )

    return db_connection


# Create a close_db function that closes a passed database connection object #
conn = create_conn()


def close_db(conn):
    conn.close()


close_db(conn)
