import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

def connect_to_db():
    return mysql.connector.connect(
        host=os.getenv("DATABASE_HOST"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        database=os.getenv("DATABASE_NAME")
    )
    
def create_tables(cursor, model_file):
    with open(model_file, "r") as file:
        cursor.execute(file.read())

def insert_data(cursor, data):
    sql = """
    INSERT INTO places (original_source_id, source, place_name, place_type, latitude, longitude, parent_id, alternate_names) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(sql, data)

def close_db(cursor, connection):
    cursor.close()
    connection.close()

