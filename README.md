# Crime & Weather Data ETL Pipeline on AWS
# Scope of Works
The purpose of this project is to demostrate varius skills associateds with data engineering. In particular developing ETL pipeline, constructing a datawarehouse though Redhsift database and working with data transfers from/to S3.
In this project I will combine crime data from 10 years along with hourly weather data to create a Redshift data warehouse for future analyis.

# Data Model
- **Crime data**: this data comes from [Houston Police Department crime statistics](http://www.houstontx.gov/police/cs/crime-stats-archives.htm)
  - montly data from June 2009 to May 2018
  - data before June 2009 is sparce & data after May 2018 is changed drastically.

|Date      |Hour |Offense Type|Beat   |Premise  |Block Range|Street Name|Type  |Suffix|Offenses|
|----------|--------|------------|-------|---------|-----------|-----------|------|------|--------|
|02/01/2018|04 |Burglary    |10H10  |Restaurant or Cafeteria|2700-2799  |NAVIGATION |BLVD  |-     |1       |



- **Weather data**: hourly data was purchased `$10` from [OpenWeather](https://home.openweathermap.org/marketplace)
  - data was choosen by matching city, in this case `Houston metropolitan area` was used.
  - `temp`, `feels_like`, `humidity`,`rain`,`snow`, and `date time` columns were used from this dataset.

|dt        |dt_iso                       |timezone|city_name|lat      |lon       |temp  |feels_like|temp_min|temp_max|humidity|wind_speed|wind_deg|rain_1h|snow_1h|
|----------|-----------------------------|--------|---------|---------|----------|------|----------|--------|--------|--------|----------|--------|-------|-------|
|1104537600|2005-01-01 00:00:00 +0000 UTC|-21600  |Houston  |29.760427|-95.369803|293.66|294.09    |292.16  |293.78  |89      |3.1       |100     |       |       |

  
## Data Merge
- Datasets were merge by combining date & hour from crime dataset and converting dt column to date_time and merging by those columns.

## crime weather data sample
|date_time |offenses                     |offense_type|block_range|street_name|beat      |premise_description|temp  |feels_like|humidity_per|rain_vol_1h_mm|snow_vol_1h_mm|
|----------|-----------------------------|------------|-----------|-----------|----------|-------------------|------|----------|------------|--------------|--------------|
|9/11/2015 12:00|1                            |Theft       |6400-6499  |Richmond   |18F30     |miscellaneous business (non-specific)|74.408|76.154    |98          |1.9           |0             |


For furder data explination, please see [data folder](https://github.com/franksalas/dend-capstone/tree/main/data).




## Data storage
img S3 & redshift
Data was uploaded to S3 buckets in a collection of CSV json & xls files. 
- the bucket structure was as follows

```
bucket-name
    - raw-data/
    - iterm-data/
    - final-data/
    - load-data/
```
- The crime dataset was around 120 montly `.xls` files that were uploaded first to an S3 bucket under:  `raw-data/crime-data/year/month` and weather data that was converted form `json` and saved under : `raw-data/weather-data/filename`.
- After that, the data was cleaned and combined by year and saved under: `iterm-data/crime-data/crime_20xx.csv`. The weather data was clean and saved under `iterm-data/weather-data/weather-09-18.csv`.
- Finally weather and crime data is combined and saved under `load-data/crime-weather-final-09-18.csv`
- multiple final files were created for testing
  - `load-data/crime-weather-sample-100-09-18.csv` 100 rows of data
  - `load-data/crime-weather-sample-1000-09-18.csv` 1K rows of data

# Database Table Creation
- After merging both datases I was able to define a star schema by extracting crime fact table along with various dimension tables as shown below.

![](https://i.imgur.com/Vf1fiTw.png)
- schema was created by selecting particular column from the dataset and spliting & creating a dim table with a primary key and a matching foreing key.


# Conclusion

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