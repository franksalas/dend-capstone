import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
# load secret keys
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_port = os.environ.get('DB_PORT')


def process_crime_weather_data(cur,filepath):







def process_song_file(cur, filepath):
    '''
    EXTRACTS data from `song_data,
    TRANSFORMS by selecting specific cols data
    and LOADS to database
    
    PARAMETERS
    - cur: cursor object from psycopg2
    - filepath: path to file in string format
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    EXTRACTS data from `log_data,
    TRANSFORMS by selecting specific cols data
    and LOADS to database
    
    PARAMETERS
    - cur: cursor object from psycopg2
    - filepath: path to file in string format
    '''
    # open log file
    df = pd.read_json(filepath,lines=True)


    # filter by NextSong action
    df =  df[df.page.str.contains('NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t,t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year,t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day','week','month','year','weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df =  df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Process files with given filepath and input function
    
    PARAMETERS
    - cur: cursor object from psycopg2
    - conn: coneection created to database
    - filepath: path to file in string format
    - func: function to process
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    Build Pipeline for songplay data
    
    PARAMETERS
    - none
    '''
    #conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    #conn = psycopg2.connect("host=db_host dbname=db_name user=db_user password=db_pass")
    conn = psycopg2.connect(database="sparkifydb",
    user="frank",
    password="punkin99",
    host="sparkifyinstance.cx3dxtsihxox.us-west-2.rds.amazonaws.com",
    port='5432')

    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()