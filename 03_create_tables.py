import psycopg2
from helper_functions.sql_queries import create_table_queries, drop_table_queries
# load secret keys

from config_loader import *

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    conn = psycopg2.connect(database=db_name,
                            user=db_user,
                            password=db_pass,
                            host=db_host,
                            port=db_port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # conn.close()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the  database. 

    - Establishes connection with the database and gets
    cursor to it.  

    - Drops all the tables.  

    - Creates all tables needed. 

    - Finally, closes the connection. 
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
