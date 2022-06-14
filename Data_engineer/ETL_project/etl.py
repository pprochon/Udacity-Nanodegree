import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np
from datetime import datetime
from create_tables import create_database
import pandas.io.sql as psql
from psycopg2.extensions import register_adapter, AsIs

#for int64 interpration in PostgreSQL
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs) 

def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts and store:
    - the song data in songs table,
    - the artist data in artists table.

    INPUTS: 
    * cur: the cursor variable
    * filepath: the file path to the song file
    """

    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data_col = ['song_id','title','artist_id','year','duration']
    song_data = pd.DataFrame(columns = song_data_col)
    song_data_list = list(df[song_data_col].iloc[0].values)
    song_data.loc[len(song_data)] = song_data_list
    
    for i, row in song_data.iterrows():
        cur.execute(song_table_insert, list(row))
    
    # insert artist record
    artist_data_col = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data = pd.DataFrame(columns = artist_data_col )
    artist_data_list = list(df[artist_data_col].iloc[0].values)
    artist_data.loc[len(artist_data)] = artist_data_list
    
    for i, row in artist_data.iterrows():
        cur.execute(artist_table_insert, list(row))


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts and store:
    - the time data in time table,
    - the users data in users table,
    - the song play record data in sonplays table (fact table)

    INPUTS: 
    * cur: the cursor variable
    * filepath: the file path to the song file
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].copy()

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    df['start_time'] = t
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ['time_start', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dict, orient = 'columns') 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
      
    # get songid and artistid from song and artist tables   
    for i, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
    
    # insert songplay record        
        songplay_data = [row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This proceudre:
    - load song_data and log_data files,
    - execute respective function with each file.
    
    INPUTS: 
    * cur: the cursor variable
    * conn: the connection from psycopg2
    * filepath: the file path to the song file
    * func: the function for processing of each file
    """

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
     """
    This proceudre:
    - Creates and connects to the sparkifydb,
    - Returns the connection and cursor to sparkifydb,
    - Runs processes.
    """
        
    conn = psycopg2.connect("************************")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
