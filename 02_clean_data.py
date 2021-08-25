from helper_functions.data_clean_helper import *
import time

# clean data


def clean_crime_weather_data(bucket_name):
    # 2009
    raw = bucket_raw_path(bucket_name, 'capstone/raw-data/crime-data/2009')
    d09 = s3_files_to_df(raw)
    df09 = clean_2009(d09)
    save_to_S3_c(df09, '2009')

    # 2010
    raw = bucket_raw_path(bucket_name, 'capstone/raw-data/crime-data/2010')
    c10 = s3_files_to_df(raw)
    df10 = clean_2010(c10)
    save_to_S3_c(df10, '2010')
    # 2011
    raw = bucket_raw_path(bucket_name, 'capstone/raw-data/crime-data/2011')
    d11 = s3_files_to_df(raw)
    df11 = clean_2011(d11)
    save_to_S3_c(df11, '2011')

    # 2012
    raw = bucket_raw_path(bucket_name, f'capstone/raw-data/crime-data/2012')
    d12 = s3_files_to_df(raw)
    df12 = clean_2012(d12)
    save_to_S3_c(df12, '2012')

    # 2013
    raw = bucket_raw_path(bucket_name, 'capstone/raw-data/crime-data/2013')
    d13 = s3_files_to_df(raw)
    df13 = clean_2013(d13)
    save_to_S3_c(df13, '2013')

    # 2014
    raw = bucket_raw_path(bucket_name, f'capstone/raw-data/crime-data/2014')
    d14 = s3_files_to_df_2014(raw)
    df14 = clean_2014(d14)
    save_to_S3_c(df14, '2014')

    # 2015
    raw = bucket_raw_path(bucket_name, f'capstone/raw-data/crime-data/2015')
    d15 = s3_files_to_df(raw)
    df15 = clean_2015(d15)
    save_to_S3_c(df15, '2015')

    # 2016
    raw = bucket_raw_path(bucket_name, f'capstone/raw-data/crime-data/2016')
    d16 = s3_files_to_df(raw)
    df16 = clean_2016(d16)
    save_to_S3_c(df16, '2016')

    # 2017
    raw = bucket_raw_path(bucket_name, f'capstone/raw-data/crime-data/2017')
    d17 = s3_files_to_df(raw)
    df17 = clean_2017(d17)
    save_to_S3_c(df17, '2017')

    # 2018
    raw = bucket_raw_path(bucket_name, f'capstone/raw-data/crime-data/2018')
    d18 = s3_files_to_df(raw)
    df18 = clean_2018(d18)
    save_to_S3_c(df18, '2018')

    # weather data
    raw = bucket_raw_path(bucket_name, f'capstone/raw-data/weather-data/')
    dfw = wr.s3.read_csv(wr.s3.list_objects(raw)[0])
    dfweather = clean_weather(dfw)
    save_to_S3_w(dfweather)


def combine_crime_data(bucket_name):
    # combine crime data
    print("combining crime data..")
    raw = bucket_raw_path(bucket_name, 'capstone/inter-data/crime-data/')
    df = s3_CSV_files_to_df(raw)
    df.date_time = pd.to_datetime(df.date_time)
    # clean premise column
    df = clean_premise(df)
    strip_col(df, 'premise_description')
    lower_col(df, 'premise_description')
    print("DONE")
    #df.premise_description = df.premise_description.str.replace('/', ' ').str.replace(',', ' ')
    # drop nan offenses
    df = df[df['offenses'].notna()]

    df['offenses'] = df['offenses'].astype('int64')
    df['hour'] = df['hour'].astype('int64')
    df['beat'] = df.beat.str.replace("'", " ")
    df.street_name = df.street_name.replace('NAN', np.nan)
    df.street_name.fillna('UNK', inplace=True)
    strip_col(df, 'street_name')
    capital_col(df, 'street_name')
    df.offense_type = df.offense_type.replace('AutoTheft', 'Auto Theft')
    df = df[~df.offense_type.str.contains('1')]
    df = df.reset_index(drop=True)
    # select only from 2009 to 2018
    # greater than the start date and smaller than the end date
    start_date = '2009-01-01'  # Jan 01 2009
    end_date = '2018-12-31'  # may 31 2018
    mask = (df['date_time'] > start_date) & (df['date_time'] <= end_date)
    df2 = df.loc[mask].reset_index(drop=True)

    # save data
    # saving
    file_name = f'crime-09-18.csv'
    print(f"saving:{file_name}")
    path_to_save = f"s3://{bucket_name}/capstone/final-data/crime-data/{file_name}"
    wr.s3.to_csv(df2, path_to_save, index=False)


def crime_weather_merge(bucket_name):
    print("merging crime & weather data.. col:date_time")
    raw = bucket_raw_path(bucket_name, f'capstone/final-data/crime-data/')
    df_crime = s3_CSV_files_to_df(raw)
    # set to datetime
    df_crime.date_time = pd.to_datetime(df_crime.date_time)
    wraw = bucket_raw_path(bucket_name, f'capstone/inter-data/weather-data/')
    df_weather = s3_CSV_files_to_df(wraw)
    df_weather.date_time = pd.to_datetime(df_weather.date_time)
    df_crime.reset_index(drop=True, inplace=True)
    df_weather.reset_index(drop=True, inplace=True)
    df = pd.merge(df_crime, df_weather, on=['date_time'])
    df.reset_index(drop=True, inplace=True)
    # drop unused columns
    df = df[['date_time', 'offenses', 'offense_type', 'block_range',
             'street_name', 'beat', 'premise_description', 'temp', 'feels_like',
             'humidity_per', 'rain_vol_1h_mm', 'snow_vol_1h_mm']]

    # Save to bucket
    file_name = f'crime-weather-final-09-18.csv'
    path_to_save = f"s3://{bucket_name}/capstone/load-data/{file_name}"
    wr.s3.to_csv(df, path_to_save, index=False)
    print(f"saving:{file_name}")
    # same sample for testing
    # 100 rows & 1k rows
    sample_1000 = df.sample(1000).reset_index(drop=True)
    file_name = f'crime-weather-sample-1000-09-18.csv'
    path_to_save = f"s3://{bucket_name}/capstone/load-data/{file_name}"
    wr.s3.to_csv(sample_1000, path_to_save, index=False)
    print(f"saving:{file_name}")
    sample_100 = df.sample(100).reset_index(drop=True)
    file_name = f'crime-weather-sample-100-09-18.csv'
    path_to_save = f"s3://{bucket_name}/capstone/load-data/{file_name}"
    wr.s3.to_csv(sample_100, path_to_save, index=False)
    print(f"saving:{file_name}")


def main(bucket_name):
    print(f"bucket name:{bucket_name}")
    # here we clean and load crime and weather data
    clean_crime_weather_data(bucket_name)
    # here we combine all the crime data into one dataframe
    # save it to s3
    combine_crime_data(bucket_name)
    # crime & weathe data is merge and saved
    # samples are saved for testing
    crime_weather_merge(bucket_name)


if __name__ == "__main__":
    start_time = time.time()
    bucket_name = "dend-data"
    main(bucket_name)
    print("--- %s seconds ---" % (time.time() - start_time))
