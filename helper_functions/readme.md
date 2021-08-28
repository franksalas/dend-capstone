# Helper Functions
These files help abstract the main etl functions
## `data_cleaner_helper.py`
- main purpose is to extract the data from S3 bucket, clean it and uploaded to S3 bucket
- contains 25 custom functions to help clean crime data
- since each year varies,  yearly functions were created
### example
```python
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
```
- custom functions for uploading and downloading from S3
#### example

```python
def bucket_raw_path(bucket_name, path_dir):
    '''get raw path of s3 for download'''
    raw_path = f's3://{bucket_name}/{path_dir}'
    return raw_path

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

def save_to_S3_c(bucket_name,df, year):
    '''save crme data to S3'''
    print(f'saving crime data: {year}')
    file_name = f'crime_{year}.csv'
    path_to_save = f"s3://{bucket_name}/capstone/inter-data/crime-data/{file_name}"
    wr.s3.to_csv(df, path_to_save, index=False)
```


## `data_model_helper.py`
- its main function is to do extact/upload data from S3, minor data clean, create dim & fact tables from dataframes and upload them to Redshift

#### example
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

...
```
## `sql_queries.py`

- its used to create, drop & upload data to Redshift

#### example
```python
...

address_table_create = ("""
CREATE TABLE address_dim (
    address_id int  NOT NULL,
    full_address varchar  NOT NULL,
    CONSTRAINT address_pk PRIMARY KEY (address_id)
);
""")


address_table_insert = ("""
INSERT INTO address_dim (
	address_id,
	full_address
)
VALUES (%s,%s)

-- ON CONFLICT (address_id) DO NOTHING
""")

...
```