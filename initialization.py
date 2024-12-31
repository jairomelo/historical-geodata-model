import dbmanager.dbmanage as db
import os

def initialize_db():
    connection = db.connect_to_db()
    cursor = connection.cursor()
    db.create_tables(cursor, "models/sql/tgn.sql")
    db.close_db(cursor, connection)
    
def create_dirs():
    os.makedirs("raw_data/TGN", exist_ok=True)
    os.makedirs("raw_data/HGIS", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
def main():
    create_dirs()
    initialize_db()

if __name__ == "__main__":
    user_permission = input("Are you sure you want to initialize the database? This will delete any existing data present in the database. (y/n) ")
    if user_permission.lower() in {"y", "yes"}:
        main()
    else:
        print("Operation cancelled by user.")
