
# Data Engineering Capstone Project

![](https://i.imgur.com/TRjFhKt.png)


# Scope the Project and Gather Data

The purpose of this project is to demonstrate various skills associated with data engineering. In particular, developing ETL pipeline, constructing a data warehouse through Redshift database, and working with data transfers from/to S3.
This project will combine crime data from 10 years and hourly weather data to create a Redshift data warehouse for future analysis or a back-end data source.

The problem with crime data is that it's not always available, or if the data is available, it is not clean or accessible, and if it is, it is behind a paywall. 
This project would allow the user to extract crime data easily by doing simple SQL queries.

## Tools & technologies used
- for storage, Simple storage service `S3` was used, and `Redshift` was used as a data warehouse.
- `python` was used as the primary language
- `jupyter lab` & `vscode` was used as text editors.
- `boto3` was used to upload data to S3
- `awswrangler` was used to interact with S3 buckets & Redshift
- `psycopg2` was used as a database adapter for python
- `pandas` & `numpy` were used to shape and modify the data.
- `seaborn` was used as a data visualizer.

# Explore and Assess the Data

## Crime data
- this data comes from [Houston Police Department crime statistics](http://www.houstontx.gov/police/cs/crime-stats-archives.htm)
- consist of 120 monthly `.xlsx` files from June 2009 to May 2018 
- it contains > 1 million rows of data.
- data before June 2009 is sparse & data after May 2018 is changed drastically.
- the data needed **heavy** cleaning, custom functions for each year created in `data_clean_helper.py`
  
### Parameters

| column  | Description |
|---|---|
|date| Date of offense, include month/date/year   |
|Hour| Approximate time when an event occurs, a value from 0-24  |
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
- data was chosen by matching city; in this case, the `Houston metropolitan area` was used.
- minimal cleaning was done with this dataset, values were converted from Kelvin to Fahrenheit, and the range was chosen from the matching date range of the crime data.
-  `temp,` `feels_like,` `humidity,` `rain,` `snow,` and `date time` columns were used from this dataset.

### Parameters
- `city_name`: City name
- `lat` : Geographical coordinates of the location (latitude)
- `lon`: Geographical coordinates of the location (longitude)
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


### Partial Sample
|dt        |dt_iso                       |timezone|city_name|lat      |lon       |temp  |feels_like|temp_min|temp_max|humidity|wind_speed|wind_deg|rain_1h|snow_1h|
|----------|-----------------------------|--------|---------|---------|----------|------|----------|--------|--------|--------|----------|--------|-------|-------|
|1104537600|2005-01-01 00:00:00 +0000 UTC|-21600  |Houston  |29.760427|-95.369803|293.66|294.09    |292.16  |293.78  |89      |3.1       |100     |       |       |

For further data explination, please see [data folder](https://github.com/franksalas/dend-capstone/tree/main/data).

# Define the Data Model
## Fact table
- **crime_fact**
  - *crime_fact_id*: primary key for table
  - *numoffenses*: number of offenses given the date &time
  - *temp*: temperature in fahrenheit given the date & time
  - *feels_like* the temperature from a human perception of weather, in fahrenheit
  - *humidity*: percentage of the humidity of given date & time
  - *rain*: rain volume for the last hour in mm
  - *snow*: show volume for the last hour in mm (in liquid state)
  - *offense_dim_id*: foreign key from offense_dim table
  - *police_beat_dim_id*: foreign key from police_beat_dim table
  - *premise_dim_id*: foreign key from premise_dim table
  - *address_dim_id*: foreign key from address_dim table
  - *datetime_dim_id*: foreign key from datetime_dim table
## Dimension Tables
- **premise_dim**
  - *premise_id*:primary key for table
  - *premise_location*: Identify the type of location where crime occurs (apartment complex, parking lot.)
- **offense_dim**
  - *offense_id*: primary key for table
  - *offense_type*: type 1 offense [murder,rape, robbery, aggravated assult, burglary, theft, auto theft]
- **address_dim**
  - *address_id*: primary key for table
  - *full_address*: full addres of where the crime was commited
- **police_beat_dim**
  - *police_beat_id*: primary key for table
  - *beat_name*: name of the police beat ex '11H30'
- **date_time_dim**
  - *date_time_id*: primary key for table
  - *date_time*: date & time of crime commited
  - *hour*: hour value from date time data
  - *day*: day from the date time data
  - *week*: number of the week from the date time data( out of 52)
  - *month*: month number from the date time data
  - *year*: year value from the date time data
  - *weekday*: weekday value from the date time data

![](https://i.imgur.com/Vf1fiTw.png)

- A star schema was chosen for this particular project because I wanted to use a relational database.
- after the data had been clean, it was merged by similar column `date_time,` creating a very large data frame
-  to match the schema, a data frame would need to be created for each `dim` and `fact` table.



|date_time |offenses                     |offense_type|block_range|street_name|beat      |premise_description|temp  |feels_like|humidity_per|rain_vol_1h_mm|snow_vol_1h_mm|
|----------|-----------------------------|------------|-----------|-----------|----------|-------------------|------|----------|------------|--------------|--------------|
|9/11/2015 12:00|1                            |Theft       |6400-6499  |Richmond   |18F30     |miscellaneous business (non-specific)|74.408|76.154    |98          |1.9           |0             |

- the process of creating dim tables with matching primary keys and foreign keys were as follows
    - find all the unique values of a column and create  a new data frame with a primary key with all the unique values
    - then  merge  a duplicate back with the original data frame change the index column to a foreign key col 

- here are the custom functions that make it possible
```python
def create_table_from_df(dataframe, column_name, new_col_name, new_pk_name):
    """ creates new dataframe with the selected column, 
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
    """ drops column that has been created already
    and replaces it with a primary key equivalent of the new table
    """
    data = data.merge(data_table, left_on=lo, right_on=ro)
    data.drop([lo, ro], axis=1, inplace=True)
    return data

```


# Run ETL to Model the Data
Here are the steps in creating our pipeline from scattering flat files to Redshift.

## Step 1: `01_upload_data_S3.py`

Data was uploaded to S3 buckets in a collection of CSV JSON & xlsx files. 
- the bucket structure was as follows

```
bucket-name
    capstone/
         raw-data/
         iterm-data/
         final-data/
         load-data/
```
- The crime dataset was around 120 monthly `.xlsx` files that were uploaded first to an S3 bucket under:  `capstone/raw-data/crime-data/year/month` and weather data that was converted from `JSON` and saved under: `capstone/raw-data/weather-data/filename`.



## Step 2: `02_clean_data.py`
- in this process , the crime datasets were **heavily** clean.
- by importing `data_cleaning_helper.py`, it contains  25 custom functions to combine and clean crime data by given year since they vary soo much.
- crime data is combined by year, and a few unused columns are dropped and saved under: `capstone/iterm-data/crime-data/crime_20xx.csv`. 
- The weather data is minimized by dropping any duplicate columns
  - converting values from kelvin to Fahrenheit
  - clean and saved under `capstone/iterm-data/weather-data/weather-09-18.csv`.
- finally, weather and crime data is merged by similar column `date_time` and saved under `capstone/load-data/crime-weather-final-09-18.csv`
- multiple final files were created for testing
  - `capstone/load-data/crime-weather-sample-100-09-18.csv` 100 rows of data
  - `capstone/load-data/crime-weather-sample-1000-09-18.csv` 1K rows of data


## Step 3: `03_create_tables.py`
- `sql_queries.py` contains the schema that we will use to create tables in Redshift
- we establish a connection  and check to make sure we delete/drop any tables in our redshift database before we create them
- AWS variables are contained in `config.cfg` and `config_loader.py`reads the file and creates global variables that are used in to connect to AWS, specifically Redshift.

## Step 4: `04_create_schema_upload.py`
- some extra cleanup is done here before upload
- a `full_address` column is created by getting the median value of `block_range` and merging it with `street_address`.
- here, we split off columns from the large dataframe to create dim tables and matching foreign keys to the main(fact) table.
- then, each table is uploaded to Redshift

## Step 5: `05_quality_check.py`
- connects to redshift and check if tables were uploaded correclty
- checks for total rows of each table
- gives sample of each table


# Sample results
- *some* tables were loaded and converted to dataframe

## Get table names
```sql
SELECT 
    table_name
FROM 
    information_schema.tables
WHERE 
    table_type='BASE TABLE'
AND 
    table_schema='public';

---
(['offense_dim'],
 ['police_beat_dim'],
 ['premise_dim'],
 ['address_dim'],
 ['datetime_dim'],
 ['crime_fact'])
```

 ## cout rows in `crime_fact` table
 ```sql
SELECT 
    COUNT(crime_fact_id)
FROM
    crime_fact
---
([1125101],)
 ```
## Join Tables
- tables used
  -  `crime_fact`
  -  `datetime_dim` 
  -   `offense_dim`
- get temperature, month , year and offense type
- create a dataframe 
```python
query = """
SELECT 
     dd.date_time,dd.month,dd.year, cf.temp, od.offense_type
FROM 
    crime_fact as cf
INNER JOIN
    datetime_dim as dd
ON
    cf.datetime_id = dd.datetime_id
INNER JOIN
    offense_dim as od
ON
    cf.offense_dim_id = od.offense_id
"""

# 05_quality_check.py functions
query_data = data = get_data_redshift(query)
df = create_df(["datetime","month","year","temp","offense_type"],query_data)

```
![](https://i.imgur.com/vb5Oj3G.png)


# Complete Project Write Up
- The goal of this project is to showcase some of the tools learned in the data engineering course.
- in the future, airflow can be used to automate future monthly crime data by creating a dag that cleans and uploads the data.
- The reason this model was chosen was that I wanted to see if it was possible to merge these two completely datasets.
- I choose S3 to upload the raw data to have a singular place to work with and not be dependent on the source (HPD website) 
- And Redshift was chosen as a database because its best suited for analytics, its parallel processing across multiple nodes.
## Scenarios
### If the data was increased by 100x.
- Since this is a data warehouse, it's expected to increase many folds.
### If the pipelines were run on a daily basis by 7am.
- Future upgrades would include an airflow set up and create a node to update a given time frame.
### If the database needed to be accessed by 100+ people.
- Distributed workloads are built into Redshift.


## Folder Structure
```
.
????????? 01_upload_data_s3.py
????????? 02_clean_data.py
????????? 03_create_tables.py
????????? 04_create_schema_upload.py
????????? 05_quality_check.py
????????? config.cfg
????????? config_loader.py
????????? explore_data.ipynb
????????? data
???   ????????? raw
???   ????????? readme.md
????????? environment.yml
????????? .gitignore
????????? helper_functions
???   ????????? data_clean_helper.py
???   ????????? data_model_helper.py
???   ????????? __init__.py
???   ????????? readme.md
???   ????????? sql_queries.py
????????? README.md
```