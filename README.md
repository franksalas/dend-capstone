# Crime & Weather Data ETL Pipeline on AWS


- Created by Francisco Salas
- version: 0.1.0
- Date: 2021-07-03
- username : franksalas

## Description
A short description of the project



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