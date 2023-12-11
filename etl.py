# import libraries
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - opens file, with object and song data filepath arguments
    - reads json song files from data
    - gets first values for song_data and artist_data, and returns lists
    - inserts song_data into table
    - inserts artist_data into table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
     

    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    # insert song record
    cur.execute(song_table_insert, song_data)
    
    
    # insert artist record into artist table
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    

def process_log_file(cur, filepath):
    """
    - opens file, with cursor object and song data filepath as arguments 
    - reads json log files
    - processes time data record and returns timestamp as datetime format 
    - combines time_data and column_labels using dictionary and zip methods
    - returns dataframe and processes to time table
    - processes user data and loads to table
    - processes songplay data and loads to table
    """
    df = pd.read_json(filepath, lines=True) # open log file
     

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday) 
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    #inserts rows
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

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
        # changed t to pd.to_datetime(row.ts) to get rid of error, and fixed songId and artistId
        songplay_data = (pd.to_datetime(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

        

# get all files matching extension from directory
def process_data(cur, conn, filepath, func): 
    """
    - processes data, and takes cursur object, database connection, data filepath, and transformation funtion as arguments
    - lists files in directory, then executes for each file acording to the function, to transform and save it to database.
    -returns none
    """
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
        

# connect to database
def main():
    """
    - connects to the sparkify database
    - runs all functions to load, process, and insert data
    - ends connection to database
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close() # close connection
    


if __name__ == "__main__":
    main()