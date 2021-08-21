import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from helper_functions import *
import time
# AWS
import boto3
import awswrangler as wr

# load secret keys
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_port = os.environ.get('DB_PORT')


def main():
    # get data
    data = get_data()
    # CREATE TABLES

    # OFFENSE TABLE DIM
    offense_table = create_table_from_df(
        data, 'offense_type', 'offense_name', 'offense_pk')
    # DELETE/REPLACE WITH PRIMARY KEY
    data = drop_add_pk(data, offense_table, 'offense_type', 'offense_name')

    # POLICE BEAT TABLE DIM
    police_beat_table = create_table_from_df(
        data, 'beat', 'beat_name', 'beat_pk')
    # DELETE/REPLACE WITH PRIMARY KEY
    data = drop_add_pk(data, police_beat_table, 'beat', 'beat_name')

    # PREMISE TABLE DIM
    premise_table = create_table_from_df(
        data, 'premise_description', 'premise_name', 'premise_pk')
    # DELETE/REPLACE WITH PRIMARY KEY
    data = drop_add_pk(data, premise_table,
                       'premise_description', 'premise_name')

    # ADDRESS TABLE DIM
    data.block_range.replace('UNK', '10-100', inplace=True)
    data.block_range.replace('1.1103e+006-1.1104e+006', '10-100', inplace=True)
    data['og_address'] = block_range_split(data)
    # drop street_name & block range
    data.drop(['block_range', 'street_name'], axis=1, inplace=True)

    address_table = create_table_from_df(
        data, 'og_address', 'full_address', 'address_pk')
    # DELETE/REPLACE WITH PRIMARY KEY
    data = drop_add_pk(data, address_table, 'og_address', 'full_address')

    # DATETIME TABLE DIM
    dt_table = create_table_from_df(
        data, 'date_time', 'date_time', 'date_time_pk')

    # inster time data records
    t = pd.to_datetime(dt_table['date_time'])

    time_data = (dt_table.date_time_pk, dt_table.date_time, t.dt.hour,
                 t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('date_time_pk', 'date_time', 'hour',
                     'day', 'week', 'month', 'year', 'weekday')
    date_time_table = pd.DataFrame.from_dict(
        dict(zip(column_labels, time_data)))

    data = data.merge(date_time_table, left_on='date_time',
                      right_on='date_time', suffixes=(True, True))
    data.drop(['date_time', 'hour', 'day', 'week', 'month',
              'year', 'weekday'], axis=1, inplace=True)

    # CRIME FACT TABLE 
    crime_fact = data.reset_index()
    crime_fact.rename(columns={'index': 'pk'}, inplace=True)
    crime_fact.pk = crime_fact.pk + 1

    # UPLOAD TABLES TO REDSHIFT
    upload_table(offense_table, offense_table_insert, 'offense_table')
    upload_table(police_beat_table, police_beat_table_insert, 'police_beat_table')
    upload_table(premise_table, premise_table_insert, 'premise_table')
    upload_table(address_table, address_table_insert, 'address_table')
    upload_table(date_time_table, datetime_table_insert, 'date_time_table')
    upload_table(crime_fact, crime_fact_table_insert, 'crime_fact_table')


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
