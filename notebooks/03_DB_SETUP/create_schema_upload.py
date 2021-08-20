import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
## AWS
import boto3
import awswrangler as wr

# load secret keys
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_port = os.environ.get('DB_PORT')


conn = psycopg2.connect(database=db_name,
user=db_user,
password=db_pass,
host=db_host,
port=db_port)
conn.set_session(autocommit=True)
cur = conn.cursor()

# load data
# helper function
def bucket_raw_path(bucket_name,path_dir):
    '''get raw path of bucket'''
    raw_path = f's3://{bucket_name}/{path_dir}'
    return raw_path


def create_table_from_df(dataframe,column_name,new_col_name):
	"""
	"""
	col_data_list = dataframe.column_name.unique().tolist()
	df_table = pd.DataFrame(col_data_list,columns=[new_col_name])
	df_table = df_table.reset_index()
	df_table.rename(columns={'index': f'{new_col_name}_pk'}, inplace=True)
	df_table[f'{new_col_name}_pk'}] = df_table[ f'{new_col_name}_pk'}] +1
	return df_table


raw = bucket_raw_path('dend-data',f'capstone/load-data/')
file = wr.s3.list_objects(raw)[1]
df = wr.s3.read_csv(file)