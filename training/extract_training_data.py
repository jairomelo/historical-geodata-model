import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

import dbmanager.dbmanage as db
import subprocess

import logging

logging.basicConfig(level=logging.INFO, filename="logs/extract_training_data.log", encoding="utf-8")
logger = logging.getLogger(__name__)

def extract_training_data():
    connection = db.connect_to_db()
    cursor = connection.cursor()
    try:
        db.execute_sql(cursor, "dbmanager/sql/filterdata.sql")
        connection.commit()
        logger.info("Training data successfully extracted")
    except Exception as e:
        logger.error(f"Error extracting training data: {e}")
        connection.rollback()
    finally:
        db.close_db(cursor, connection)

def move_data_to_training_dir():
    subprocess.run(["mv", "/var/lib/mysql-files/filtered_places.csv", "training/data/training_data.csv"])

def main():
    extract_training_data()
    move_data_to_training_dir()

if __name__ == "__main__":
    main()