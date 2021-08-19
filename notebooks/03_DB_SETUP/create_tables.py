import psycopg2
import os
from sql_queries import create_table_queries, drop_table_queries
# load secret keys
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_port = os.environ.get('DB_PORT')

# engine = psycopg2.connect(
#     database="sparkifydb",
#     user="frank",
#     password="punkin99",
#     host="sparkifyinstance.cx3dxtsihxox.us-west-2.rds.amazonaws.com",
#     port='5432'
# )

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    #conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn = psycopg2.connect(database="crimeweatherdb",
    user="frank",
    password="punkin99",
    host="database-1.cx3dxtsihxox.us-west-2.rds.amazonaws.com",
    port='5432')
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    #cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    #cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    #conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn = psycopg2.connect(database="crimeweatherdb",
    user="frank",
    password="punkin99",
    host="database-1.cx3dxtsihxox.us-west-2.rds.amazonaws.com",
    port='5432')

    cur = conn.cursor()
    
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
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
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