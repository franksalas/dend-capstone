import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from helper_functions import *
# AWS
import boto3
import awswrangler as wr
# load secret keys
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_port = os.environ.get('DB_PORT')




# helprer functions
def block_range_split(df):
    '''split blockrange col values
    then give median value as a string'''
    first = df.block_range.str.split(pat='-', expand=True)[0].astype('int')
    second = df.block_range.str.split(pat='-', expand=True)[1].astype('int')
    med = np.ceil((second + first)/2).astype('int')
    med = med.astype('str')
    street = df.street_name
    res = med + " " + street + " Houston, TX"
    return res


def upload_table(table_df, upload_table_sql, table_name):
    ''' upload dataframe table to redshift'''
    print(f'uploading: {table_name}')
    files = table_df.values.tolist()
    count_files = len(files)
    # connection
    conn = psycopg2.connect(database=db_name, user=db_user,
                            password=db_pass, host=db_host, port=db_port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    for i in table_df.values.tolist():
        cur.execute(upload_table_sql, i)
        conn.commit()
    conn.close()


def bucket_raw_path(bucket_name, path_dir):
    '''get raw path of bucket'''
    raw_path = f's3://{bucket_name}/{path_dir}'
    return raw_path


def load_data(raw_s3):
    # depends what file you are uploading
    file = wr.s3.list_objects(raw_s3)[1]  # depends what file you are uploading(0,1MIL,1,100 RECORDS,1, 2,1K,)
    print(f'loading file:\n{file}')
    return wr.s3.read_csv(file)


def create_table_from_df(dataframe, column_name, new_col_name, new_pk_name):
    """ creates new dataframe with selected column, find all the unique values and
    creates a new dataframe with a primary key with all the unique values
    """
    print("creating tables...")
    col_data_list = dataframe[column_name].unique().tolist()
    total_rows = len(col_data_list)
    df_table = pd.DataFrame(col_data_list, columns=[new_col_name])
    df_table = df_table.reset_index()
    df_table.rename(columns={'index': new_pk_name}, inplace=True)
    df_table[new_pk_name] = df_table[new_pk_name] + 1
    return df_table


def drop_add_pk(data, data_table, lo, ro):
    """ drops column that has been created alreadh
    and replaces it with a primary key equivalent of the new table
    """
    data = data.merge(data_table, left_on=lo, right_on=ro)
    data.drop([lo, ro], axis=1, inplace=True)
    return data


def get_data():
    """loads dataframe from s3 file
    """
    raw = bucket_raw_path('dend-data', f'capstone/load-data/')
    df = load_data(raw)
    print(f"total rows: {len(df.index)}")
    return df
