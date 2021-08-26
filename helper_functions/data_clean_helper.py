import pandas as pd
import os
import awswrangler as wr


# Helper functions

def bucket_raw_path(bucket_name, path_dir):
    '''get raw path of s3 for download'''
    raw_path = f's3://{bucket_name}/{path_dir}'
    return raw_path


def s3_files_to_df(s3_files_path):
    ''' load s3 file path from wr result
    returns a dataframe of concat files '''
    df_list = []
    for i in wr.s3.list_objects(s3_files_path):
        temp = wr.s3.read_excel(i)
        df_list.append(temp)
    # create df from list files
    df = pd.concat(df_list, ignore_index=True)
    return df


def s3_CSV_files_to_df(s3_files_path):
    ''' load s3 file path from wr result
    returns a dataframe of concat files '''
    df_list = []
    for i in wr.s3.list_objects(s3_files_path):
        temp = wr.s3.read_csv(i,)
        df_list.append(temp)
    # create df from list files
    df = pd.concat(df_list, ignore_index=True)
    return df

# Helper functions for cleaning data


def clean_2009(df):
    '''clean crime data from 2009'''
    print('cleaning crime data: 2009')
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    # change column name
    df = df.rename(columns={'#_of_offenses': 'offenses'})
    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')
    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')
    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def clean_2010(df):
    '''clean crime data from 2010'''
    print('cleaning crime data: 2010')
    df['offenses'] = pd.concat([df['. Of Offenses'].dropna(),
                                df['. of Offenses'].dropna()]).reindex_like(df)

    # drop columns
    df = df.drop(['. Of Offenses', '. of Offenses', 'Field11',
                  'Field12',
                  'Field13'], axis=1)
    # cleanup columns
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()

    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')
    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')

    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def clean_2011(df):
    '''clean crime data from 2011'''
    print('cleaning crime data: 2011')
    # cleanup columns
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    df = df.drop(['field11', 'field12'], axis=1)
    # change column name

    df.rename(columns={
        '._of_offenses': 'offenses',
    }, inplace=True)

    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')

    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')
    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def clean_2012(df):
    '''clean crime data from 2012'''
    print('cleaning crime data: 2012')
    # cleanup columns
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    df = df.drop(['field11'], axis=1)
    # change column name

    df.rename(columns={
        '._of_offenses': 'offenses',
    }, inplace=True)

    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')
    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')

    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def clean_2013(df):
    '''clean crime data from 2013'''
    print('cleaning crime data: 2013')
    # cleanup columns
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    df = df.drop(['field11', 'field2', 'field12',
                 'field13', 'field14'], axis=1)
    # change column name

    df.rename(columns={
        '._of_offenses': 'offenses',
    }, inplace=True)
    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')
    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')
    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def clean_2014(df):
    '''clean crime data from 2014'''
    print('cleaning crime data: 2014')
    # clean hour col
    df['Hour'] = df['Hour'].str.replace('\'', '')
    # Change hour to int
    df['Hour'] = df['Hour'].astype('int64')
    df['block_range'] = pd.concat([df['Block Range'].dropna(),
                                   df['BlockRange'].dropna()]).reindex_like(df)
    df['street_name'] = pd.concat([df['Street Name'].dropna(),
                                   df['StreetName'].dropna()]).reindex_like(df)
    df['offenses'] = pd.concat([df['. Of Offenses'].dropna(),
                                df['. offenses'].dropna(),
                                df['. Offenses'].dropna(),
                                df['. Of'].dropna()]).reindex_like(df)
    # drop unused cols
    df = df.drop(['Block Range', 'BlockRange', 'Street Name', 'StreetName', '. Of Offenses', '. offenses', '. Offenses', '. Of', 'Field11',
                  ], axis=1)

    # cleanup col names
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()

    # remove values with nat date
    df = df[~df.date.isna()].reset_index(drop=True)
    df["premise"].fillna("UNK", inplace=True)
    df["block_range"].fillna("UNK", inplace=True)
    df["beat"].fillna("UNK", inplace=True)

    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')

    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')

    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def s3_files_to_df_2014(s3_files_path):
    ''' load s3 file path from wr result
    returns a dataframe of concat files
    only works with 2014 data because of some stupid shit. '''
    df_list = []
    for i in wr.s3.list_objects(s3_files_path):
        # added dtype error fix for Hour
        temp = wr.s3.read_excel(i, engine='openpyxl', dtype={'Hour': str})
        df_list.append(temp)
    # create df from list files
    df = pd.concat(df_list, ignore_index=True)
    return df


def clean_2015(df):
    '''clean crime data from 2015'''
    print('cleaning crime data: 2015')
    # cleanup columns
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    df.rename(columns={'._offenses': 'offenses', }, inplace=True)
    df.rename(columns={'blockrange': 'block_range',
              'streetname': 'street_name'}, inplace=True)
    # Change null values to UNK
    df["premise"].fillna("UNK", inplace=True)
    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')
    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')

    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def clean_2016(df):
    '''clean crime data from 2016'''
    print('cleaning crime data: 2016')
    # cleanup columns
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    # change column name
    df.rename(columns={'._offenses': 'offenses', }, inplace=True)
    df.rename(columns={'blockrange': 'block_range',
              'streetname': 'street_name'}, inplace=True)
    # Change null values to UNK

    df["premise"].fillna("UNK", inplace=True)
    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')

    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')

    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def clean_2017(df):
    '''clean crime data from 2017'''
    print('cleaning crime data: 2017')
    df['block_range'] = pd.concat([df['Block Range'].dropna(),
                                   df['BlockRange'].dropna()]).reindex_like(df)

    df['street_name'] = pd.concat([df['Street Name'].dropna(),
                                   df['StreetName'].dropna()]).reindex_like(df)

    df['offenses'] = pd.concat([df['. offenses'].dropna(),
                                df['Offenses'].dropna()]).reindex_like(df)
    # drop unused columns
    df = df.drop(['Block Range',
                  'BlockRange', 'Street Name', 'StreetName', '. offenses', 'Offenses'], axis=1)
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    # Change null values to UNK

    df["premise"].fillna("UNK", inplace=True)
    df["street_name"].fillna("UNK", inplace=True)
    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')

    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def clean_2018(df):
    '''clean crime data from 2018'''
    print('cleaning crime data: 2018')
    # cleanup columns
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    df["street_name"].fillna("UNK", inplace=True)
    # convert two columns into one datetime
    df['date_time'] = pd.to_datetime(
        df['date']) + pd.to_timedelta(df['hour'], unit='h')
    # convert offenses to int
    df['offenses'] = df['offenses'].astype('int64')

    col_ord = ['date_time', 'offenses', 'offense_type', 'block_range',
               'street_name', 'type', 'suffix', 'beat', 'premise', 'date', 'hour']
    df = df[col_ord]
    return df


def Celsius_to_Kelvin(C):
    return (C + 273.15)


def Kelvin_to_Celsius(K):
    return (K - 273.15)


def Kelvin_to_Farh(K):
    return (K - 273.15) * 9/5 + 32


def fahr_to_celsius(temp_fahr):
    """Convert Fahrenheit to Celsius

    Return Celsius conversion of input"""
    temp_celsius = (temp_fahr - 32) * 5 / 9
    return temp_celsius


# Clean weather data
# source: https://openweathermap.org/history-bulk

def clean_weather(df):
    print('cleaning weather data')
    # cleanup columns
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_').str.replace('\n', '_').str.strip()
    df = df[['dt', 'temp', 'feels_like',
             'temp_min',
             'temp_max', 'humidity',
             'wind_speed', 'rain_1h', 'snow_1h',
             'clouds_all',
             'weather_main',
             'weather_description']]

    df.rename(columns={'dt': 'date_time', 'clouds_all': 'clouds_all_per', 'humidity': 'humidity_per', 'rain_1h': 'rain_vol_1h_mm',
                       'snow_1h': 'snow_vol_1h_mm'}, inplace=True)

    # change nan to zero
    df['rain_vol_1h_mm'] = df['rain_vol_1h_mm'].fillna(0)
    df['snow_vol_1h_mm'] = df['snow_vol_1h_mm'].fillna(0)

    # convert to datetime
    df.date_time = pd.to_datetime(df['date_time'], unit='s')

    df['temp'] = Kelvin_to_Farh(df["temp"])
    df['temp_min'] = Kelvin_to_Farh(df["temp_min"])
    df['feels_like'] = Kelvin_to_Farh(df["feels_like"])
    df['temp_max'] = Kelvin_to_Farh(df["temp_max"])
    # drop a duplicate weather rows with samde datetime
    df.drop_duplicates(subset=['date_time'], keep='first', inplace=True)
    return df


def clean_premise(df):
    # load premise dataset
    raw_directory = os.path.join(
        'data', 'raw', 'crime_data', 'premise_codes.csv')
    premise_df = pd.read_csv(raw_directory)
    premise_df.columns = premise_df.columns.str.strip(
    ).str.lower().str.replace('-', '_').str.replace(' ', '_')
    #  rename premise column for easy merge
    premise_df.rename(columns={'premise_type': 'premise', }, inplace=True)
    # merge with main dataframe
    df = pd.merge(df, premise_df, on='premise', how='outer')
    # combine both columns and remove any nulls in between
    # if value is null in premise_descriptiom, copy the matching row of premise to it
    df.loc[df['premise_description'].isnull(
    ), 'premise_description'] = df['premise']
    # bit of str cleanup
    df.premise_description = df.premise_description.str.replace('/', ' ')\
        .str.replace(',', ' ').str.strip()
    # drop df.premise column
    df.drop(['premise'], axis=1, inplace=True)
    return df


def save_to_S3_c(bucket_name,df, year):
    '''save crme data to S3'''
    print(f'saving crime data: {year}')
    file_name = f'crime_{year}.csv'
    path_to_save = f"s3://{bucket_name}/capstone/inter-data/crime-data/{file_name}"
    wr.s3.to_csv(df, path_to_save, index=False)


def save_to_S3_w(bucket_name,df):
    '''save weather data to S3'''
    print('saving weather data')
    file_name = f'weather-09-18.csv'
    path_to_save = f"s3://{bucket_name}/capstone/inter-data/weather-data/{file_name}"
    wr.s3.to_csv(df, path_to_save, index=False)


def strip_col(df, col):
    '''strips column of blank spaces'''
    print(f'column:{col}')
    print('before:{}'.format(len(df[col].value_counts(dropna=False))))
    df[col] = df[col].str.strip()
    print('after:{}'.format(len(df[col].value_counts(dropna=False))))


def lower_col(df, col):
    '''lowecases string values in column'''
    print(f'column:{col}')
    print('before:{}'.format(len(df[col].value_counts(dropna=False))))
    df[col] = df[col].str.lower()
    print('after:{}'.format(len(df[col].value_counts(dropna=False))))


def capital_col(df, col):
    '''capitalies string values in column'''
    print(f'column:{col}')
    print('before:{}'.format(len(df[col].value_counts(dropna=False))))
    df[col] = df[col].str.capitalize()
    print('after:{}'.format(len(df[col].value_counts(dropna=False))))
