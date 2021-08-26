# Data Engineering Capstone Project




# Scope the Project and Gather Data

The purpose of this project is to demostrate varius skills associateds with data engineering. In particular developing ETL pipeline, constructing a datawarehouse though Redhsift database and working with data transfers from/to S3.
In this project I will combine crime data from 10 years along with hourly weather data to create a Redshift data warehouse for future analyis or a back-end data source.

# Explore and Assess the Data

### Crime data
- this data comes from [Houston Police Department crime statistics](http://www.houstontx.gov/police/cs/crime-stats-archives.htm)
- consinst of 120 montly `.xls` files from June 2009 to May 2018
- data before June 2009 is sparce & data after May 2018 is changed drastically.
- the data neded **heavy** cleaning, custom functions for each year created in `data_clean_helper.py`
  
### Parameters

| column  | Description |
|---|---|
|date| Date of offense, include month/date/year   |
|Hour| Approximate time when an event occurs, value form 0-24  |
|Offense Type |Type I offense   |  |
|Beat| The geographic area of the city broken down for patrol and statistical purpose  |
| Premise |Identify the type of location where crime occurs (apartment complex, parking lot.|
|Block Range|The value range of street     |
|Street Name| Name of the street where the offense occurred |
| Type |Street type, rd, Blvd
|Suffix |N, S, E, W   |
|Offenses| Times offense happen within the time frame     |

### Sample
|Date      |Hour |Offense Type|Beat   |Premise  |Block Range|Street Name|Type  |Suffix|Offenses|
|----------|--------|------------|-------|---------|-----------|-----------|------|------|--------|
|02/01/2018|04 |Burglary    |10H10  |Restaurant or Cafeteria|2700-2799  |NAVIGATION |BLVD  |-     |1       |

## Weather data
- This data comes from  [OpenWeather](https://home.openweathermap.org/marketplace)
- it was purchased for $10
- it includes hourly results for the city.
- data was choosen by matching city, in this case `Houston metropolitan area` was used.
- minimal cleaning was done with this dataset, values were converted from kelvin to farenheight, and range was choosen from matching date range of the crime data.
- - `temp`, `feels_like`, `humidity`,`rain`,`snow`, and `date time` columns were used from this dataset.

### Parameters
- `city_name`: City name
- `lat` : Geographical coordinates of the location (latitude)
- `lon` : Geographical coordinates of the location (longitude)
- `main`
  - `main.temp` Temperature
  - `main.feels_like` This temperature parameter accounts for the human perception of weather
  - `main.pressure` Atmospheric pressure (on the sea level), hPa
  - `main.humidity` Humidity, %
  - `main.temp_min` Minimum temperature at the moment. This is deviation from temperature that is possible for large cities and megalopolises geographically expanded (use these parameter optionally).
  - `main.temp_max` Maximum temperature at the moment. This is deviation from temperature that is possible for large cities and megalopolises geographically expanded (use these parameter optionally).

- `wind`
  - `wind.speed` Wind speed. Unit Default: meter/sec
  - `wind.deg` Wind direction, degrees (meteorological)
- `clouds`
  - `clouds.all` Cloudiness, %
- `rain`
  - `rain.1h` Rain volume for the last hour, mm
  - `rain.3h` Rain volume for the last 3 hours, mm
- `snow`
  - `snow.1h` Snow volume for the last hour, mm (in liquid state)
  - `snow.3h` Snow volume for the last 3 hours, mm (in liquid state)
- `weather`
  - `weather.id` Weather condition id
  - `weather.main` Group of weather parameters (Rain, Snow, Extreme etc.)
  - `weather.description` Weather condition within the group
  - `weather.icon` Weather icon id
- `dt` Time of data calculation, unix, UTC
- `dt_isoDate` and time in UTC format
- `timezone` Shift in seconds from UTC


## Partial Sample
|dt        |dt_iso                       |timezone|city_name|lat      |lon       |temp  |feels_like|temp_min|temp_max|humidity|wind_speed|wind_deg|rain_1h|snow_1h|
|----------|-----------------------------|--------|---------|---------|----------|------|----------|--------|--------|--------|----------|--------|-------|-------|
|1104537600|2005-01-01 00:00:00 +0000 UTC|-21600  |Houston  |29.760427|-95.369803|293.66|294.09    |292.16  |293.78  |89      |3.1       |100     |       |       |

For furder data explination, please see [data folder](https://github.com/franksalas/dend-capstone/tree/main/data).

# Define the Data Model

![](https://i.imgur.com/Vf1fiTw.png)

- A star schema was choosen for this particular project because I wanted to use of a relational database.
- after the data had been clean it was merged by similar column `date_time` creating a very large dataframe
-  to match the schema, a dataframe would need to be created for each `dim` and `fact` table.



|date_time |offenses                     |offense_type|block_range|street_name|beat      |premise_description|temp  |feels_like|humidity_per|rain_vol_1h_mm|snow_vol_1h_mm|
|----------|-----------------------------|------------|-----------|-----------|----------|-------------------|------|----------|------------|--------------|--------------|
|9/11/2015 12:00|1                            |Theft       |6400-6499  |Richmond   |18F30     |miscellaneous business (non-specific)|74.408|76.154    |98          |1.9           |0             |

- the proceess of creating dim tables with matching primary keys and foreing keys were as follows
    - find all the unique values of a col and create  a new dataframe with a primary key with all the unique values
    - then  merge  a duplicate back with original dataframe change the index column to a foren key col 

- here are the custom functions that make it possible
```python
def create_table_from_df(dataframe, column_name, new_col_name, new_pk_name):
    """ creates new dataframe with selected column, 
    find all the unique values and
    creates a new dataframe with a primary key
    with all the unique values
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

```



# Run ETL to Model the Data
Here we go step by step in creating our pipeline from scatter flat files to Redshift.

## Step 1: `01_upload_data_S3.py`

Data was uploaded to S3 buckets in a collection of CSV json & xls files. 
- the bucket structure was as follows

```
bucket-name
    capstone/
         raw-data/
         iterm-data/
         final-data/
         load-data/
```
- The crime dataset was around 120 montly `.xls` files that were uploaded first to an S3 bucket under:  `capstone/raw-data/crime-data/year/month` and weather data that was converted form `json` and saved under : `capstone/raw-data/weather-data/filename`.



## Step 2: `02_clean_data.py`
- in this process, we **heavly** clean the crime datasets.
- by importing `data_cleaning_helper.py` it contains  25 custom functions to combine and clean crime data by given year since they vary soo much.
- crime data is combined by year and a few unused columns dropped and saved under: `capstone/iterm-data/crime-data/crime_20xx.csv`. 
- The weather data is minimised by dropping any duplicate columns
  - converting values form kelvin to fareheight
  - clean and saved under `capstone/iterm-data/weather-data/weather-09-18.csv`.
- finally weather and crime data is merged by similar column `date_time` and saved under `capstone/load-data/crime-weather-final-09-18.csv`
- multiple final files were created for testing
  - `capstone/load-data/crime-weather-sample-100-09-18.csv` 100 rows of data
  - `capstone/load-data/crime-weather-sample-1000-09-18.csv` 1K rows of data


## Step 3: `03_create_tables.py`
- `sql_queries.py` contains the schema that we will use to create tables in redshift
- we establish a connection  and check to make sure we delete/drop any tables in our redshift database before we create them
- aws variables are contained in `config.cfg` and `config_loader.py`reads the file and creates global variables that are used in to connect to aws, specifically redshift.

## Step 4: `04_create_schema_upload.py`
- some extra cleanup is done here before upload
- a `full_address` column is created by getting the middle value of `block_range` and merging it with `street_address`.
- here we split off columns from the large dataframe to create dim tables and matching foreigh keys to the main(fact) table.
- then each table is uploaded to redshift


# Complete Project Write Up





---
# Enviroment
## Create enviroment from file
```bash
conda env create -f environment.yml
```

## Activate environment

```bash
conda activate capstone
```

## Folder Structure
```
.
├── 01_upload_data_s3.py
├── 02_clean_data.py
├── 03_create_tables.py
├── 04_create_schema_upload.py
├── config.cfg
├── config_loader.py
├── data
│   ├── raw
│   └── readme.md
├── environment.yml
├── .gitignore
├── helper_functions
│   ├── data_clean_helper.py
│   ├── data_model_helper.py
│   ├── __init__.py
│   ├── readme.md
│   └── sql_queries.py
└── README.md
```