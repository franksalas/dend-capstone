import psycopg2
import pandas as pd
import redshift_connector
from config_loader import *


def get_data_redshift(query):
    '''function to return data from table in tuple form'''
    with redshift_connector.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_pass
    )as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    return result


def get_cols_redshift(table):
    '''function to get column names from given table name'''
    query = f"""
            select ordinal_position as position,
                   column_name,
                   data_type,
                   case when character_maximum_length is not null
                        then character_maximum_length
                        else numeric_precision end as max_length,
                   is_nullable,
                   column_default as default_value
            from information_schema.columns
            where table_name = '{table}' -- enter table name here
                  -- and table_schema = 'Schema name'
            order by ordinal_position;
            """
    with redshift_connector.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_pass
    )as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [lis[1] for lis in result]
    return columns


def create_df(table_cols, table_data):
    '''returnd dataframe from table cols & table data'''
    df = pd.DataFrame(table_data, columns=table_cols)
    return df

def count_keys(table_name,pk):
    query = f"""
    SELECT COUNT({pk})
    FROM
        {table_name}
    """
    print(f"table name: {table_name}")
    print(f"total rows: {get_data_redshift(query)}\n")

def get_table_names():
    query = """
    SELECT 
        table_name
    FROM 
        information_schema.tables
    WHERE 
        table_type='BASE TABLE'
    AND 
        table_schema='public';
    """
    print('Print table names')
    #print(query)
    print(get_data_redshift(query))


def get_sample(table):
    query = f"""
    SELECT *
    FROM {table}
    LIMIT 5
    """    
    print('sample')
    print(get_data_redshift(query))




def main():
    get_table_names()
# (['offense_dim'], ['police_beat_dim'], ['premise_dim'], ['address_dim'], ['datetime_dim'], ['crime_fact'])
    count_keys('offense_dim','offense_id')
    get_sample('offense_dim')
    count_keys('police_beat_dim','police_beat_id')
    get_sample('police_beat_dim')
    count_keys('premise_dim','premise_id')
    get_sample('premise_dim')
    count_keys('address_dim','address_id')
    get_sample('address_dim')
    count_keys('datetime_dim','datetime_id')
    get_sample('datetime_dim')
    count_keys('crime_fact','crime_fact_id')
    get_sample('crime_fact')
if __name__ == '__main__':
    main()
