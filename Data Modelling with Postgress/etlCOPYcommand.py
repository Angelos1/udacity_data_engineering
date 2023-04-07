import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime

def process_song_file(cur, filepath):
    '''
    The function to handle inserting data into songs and artists tables.
    
    Reads a song file and extracts information for songs and artists.
    Inserts this information to the artists and songs tables.
    
    Parameters:
        cur : The cursor used to execute the insert queries.
        filepath : The file path of the song file.
    '''
    
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = tuple(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = tuple(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    The function to handle inserting data into songplays, users and time tables.
    
    Reads a log file and filters only the records that are relevant to song plays.
    Extracts information for songplays, users and time.
        Processing for time table: Converts the timestamp data from milliseconds to datetime objects
        and then uses the pandas 'dt' attribute to get all necessary information.
    Inserts this information to the songplays, users and time tables.
    
    Parameters:
        cur : The cursor used to execute the insert queries.
        filepath : The file path of the log file.
    '''
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    t = df["ts"].map(lambda ms: datetime.datetime.fromtimestamp(ms/1000))

    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ["timestamp", "hour", "day", "week", "month", "year", "weekday"]
    data_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(data_dict)
    time_path = filepath.rstrip('.json') + '_time.csv'
    time_df.to_csv(time_path, index=False)
    cur.execute(time_table_copy, (time_path,))
    
    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    user_path = filepath.rstrip('.json') + '_user.csv'
    user_df.to_csv(user_path, index=False)
    
    cur.execute(user_table_copy, (user_path,))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        # converting ts to datetime
        dt = datetime.datetime.fromtimestamp(row.ts/1000)
        
        # insert songplay record
        songplay_data = (dt, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    The function to handle ETL for all the files in a directory.
    
    Recursively searches for all the filenames inside the directory defined by 'filepath'
    For each of these filenames calls the funcion 'func' to handle the insertion of the
    file's data into the database.
    
    Parameters:
        cur : The cursor used to execute the insert queries in the 'func' function's definition.
        conn : The connection to the database.
        filepath : The file path of the root directory that contains the datafiles.
        func : The function to execute the insertion of the data.
            In our case 'func' can be one of the above defined methods 'process_log_file' 
            or 'process_song_file'.
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()