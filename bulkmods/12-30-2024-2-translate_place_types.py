"""
This script translates to English the place types from places table with HGIS source.
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

import dbmanager.dbmanage as db

connection = db.connect_to_db()
cursor = connection.cursor()

try:
    db.execute_sql(cursor, "dbmanager/sql/updateplaces.sql")
    connection.commit()
    print("Place types successfully updated")
except Exception as e:
    print(f"Error updating place types: {e}")
    connection.rollback()
finally:
    db.close_db(cursor, connection)