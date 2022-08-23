import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import create_tables
import numpy as np
from datetime import datetime
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def cretae_df(list_of_files):
    """
    this function takes a list of all the file paths that need to be fetched, 
    this is done by creating a data frame and then appending data to it by reading the values from the different paths in the given list
    """
    result = pd.DataFrame()
    for f in list_of_files:
        df = pd.read_json(f, lines=True)
        result = result.append(df, ignore_index = True)
    return result
        
    
    
def get_time_data_from_ts(ts):
    """
    gets the all the data necessary to fill the time table from the time stamp value
    by converting the timestape value to a datetime python object that allow us to get all the data from its attriputes
    """
    result = []
    for i in ts:
        
        date = datetime.fromtimestamp(i/1000)
        result.append([date, date.hour, date.day, date.isocalendar()[1], date.month, date.year, date.weekday()])
    return result


def process_song_file(cur, conn, songs_df):
    """
    this function is responsible for processing the songs files and propagating this songs data to the songs table and the artist table
    this function uses the given data frame conatining all the songs data is used to fill the songs table by selecting 
    the appropiate fields. the method then fills the artist table by also selecting the fields required for the artist table
    """
    # insert song records
    for i,row in songs_df.iterrows():
        try:
            data = row.values[[7,8,0,9,5]]
            data[1] = str(data[1].encode('utf-8').strip())
            cur.execute(song_table_insert, data)
            conn.commit()
        except UnicodeEncodeError as e:
            print(e)
            print(i)
            print(row)
            conn.commit()
        
        
        # insert artist records
        try:
            data = row.values[[0,4,2,1,3]]
            data[1] = str(data[1].encode('utf-8').strip())
            data[2] = str(data[2].encode('utf-8').strip())
            cur.execute(artist_table_insert, data)
            conn.commit()
        except Exception as e:
            print(e)
            print(i)
            print(row.values[[0,4,2,1,3]])
            conn.commit()


def process_log_file(cur, conn, df):
    """
    this function is used to process the the log files and propagate the data in these files to the time, users, and songplays tables
    the function  uses the get_time_data_from_ts to get the data necessary for the time table to be filled with data.
    the function then uses the original data frame to populate the users table with data.
    eventually the function populates the sonplays table by selecting the song and the artist id for each specific row in the log_files dataframe
    and inserts the approate fields into the songplays table
    """
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts']
    
    # insert time data records
    time_data = get_time_data_from_ts(t)
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, row.values)
            conn.commit()
        except Exception as e:
            print(e)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row.values)
            conn.commit()
        except Exception as e:
            print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        try:
            cur.execute(song_select, (str(row.song.encode('utf-8').strip()), row.length, str(row.artist.encode('utf-8').strip())))
            results = cur.fetchone()
        except Exception as e:
            print(e)
            
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record date = datetime.fromtimestamp(row.ts/1000)
        songplay_data = (index, datetime.fromtimestamp(row.ts/1000), row.userId, row.level, songid, artistid, row.sessionId, str(row.location.encode('utf-8').strip()),  str(row.userAgent.encode('utf-8').strip()))
        try:
            cur.execute(songplay_table_insert, songplay_data)
            conn.commit()
        except Exception as e:
            print(e)

def process_data(cur, conn, filepath, func):
    """
    given the filepath of all the data this function first create a datafram containing all the data in that path. the function then 
    passes this dataframe to the approbiate function responsible for processing this dataframe 
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    # GET DATAFRAME FROM YOU OWN FUNCTION
    
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    #create dataframe containing all files
    df = cretae_df(all_files)
    # apply the actuall processing function on the dataframe
    func(cur, conn, df)


def main():
    """
    the main function first calls the main method in the create_tables script to create the database and the required tables
    the function then calls process_data along with the connection credits, the filepath and the appropiate dataframe to fill the tables
    """
    create_tables.main()
    print('tables created')
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    print('connected to DB')
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    print('processed song files')
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    print('processed log files')
    conn.close()
    print('finished')


if __name__ == "__main__":
    main()