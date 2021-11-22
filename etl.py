import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Fills the songs and artist tables of the sparkify database
    with the data from the given file.

    Args:
        cur: cursor object to a database
        filepath: path where to search for files where
                    the database data is.

    Returns:
        -

    """
    # open song file
    df = pd.read_json(path_or_buf=filepath, lines=True)

    # insert artist record
    artist_data = df.loc[:, ['artist_id', 'artist_name', 'artist_location',
                             'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df.loc[:, ['song_id', 'title', 'artist_id',
                           'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """Fills the time, user and songplay tables of the sparkify database
    with the data from the given file.

    Args:
        cur: cursor object to a database
        filepath: path where to search for files where
                    the database data is.

    Returns:
        -

    """
    # open log file
    df = pd.read_json(path_or_buf=filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    df.ts = pd.to_datetime(df.ts, unit='ms')
    t = df.ts

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear,
                 t.dt.month, t.dt.year, t.dt.weekday)
    time_data = list(zip(*time_data))  # zip(*iterables)
    column_labels = ['start_time', 'hour', 'day',
                     'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            print(results)
            songid, artistid = results

        else:
            songid, artistid = None, None

        # convert timestamp column to have same data type in time and songplay tables
        # timestamp = pd.to_datetime(row.ts, unit='ms')
        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Search for all the files packed in a given directory
    and calls a function to process them.

    Args:
        cur: cursor object to a database
        conn: connection object to a database session.
        filepath: path where to search for files where
                    the database data is.
        func: name of the function that is going to process
                the data in the founded files

    Returns:
        -

    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """Establishes connection with the sparkify database and gets
    cursor to it.

    Calls function process_data twice in order to fill the tables
    of the database with the data in the following paths:
        - data/song_data
        - data/log_data

    Finally, closes the connection.

    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
