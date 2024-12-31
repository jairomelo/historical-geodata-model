import dbmanage as db

def main():
    connection = db.connect_to_db()
    cursor = connection.cursor()
    db.create_tables(cursor, "models/sql/tgn.sql")
    db.close_db(cursor, connection)

if __name__ == "__main__":
    main()