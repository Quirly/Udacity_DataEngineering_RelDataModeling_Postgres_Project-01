import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import etl_functions as etl_f


def process_song_file(cur,conn,filepath):
    """
    Load song files stored in specific folder, read records and insert
    song specific attributes data to table SONGS and artist data to table
    ARTISTS.

    Parameters:
        cur: Cursor interacting with database
        conn: Active database connection to a certain instance
        filepath: Path where song files are stored

    Returns:
        song data read from files in dataframe format

    """
    # open and load song files
    df_song=etl_f.load_songfiles(filepath)
    print("Songs loaded ...")
    print(type(df_song))
    print("Dataframe df_song length: "+str(len(df_song)))

    #insert song records to songs table
    print(type(df_song))
    print("Dataframe df_song length: "+str(len(df_song)))
    len_new_df=etl_f.insert_songs_df(cur,conn,df_song)
    print(str(len_new_df)+" songs table entries saved to database...")

    # insert artist records to artists table
    len_new_art_df= etl_f.insert_artists_df(cur,conn,df_song)
    print(str(len_new_art_df)+" artists table entries saved to database...")

    return df_song

def process_log_file(cur, conn,filepath):
    """
    Load log files stored in specific folder, read records and insert
    song specific attributes data to table USERS and TIME SERIES data to table
    TIME. Combines song and log data by joining data via ARTIST_ID and inserts
    the combined data to table SONGPLAYS.

    Parameters:
        cur: Cursor interacting with database
        conn: Active database connection to a certain instance
        filepath: Path where log files are stored

    Returns:
        Log data read from files in dataframe format

    """
    # open log file
    df_log=etl_f.load_logfiles(filepath)
    print("Logfiles loaded ...")
    print(type(df_log))
    print("Dataframe df_log length: "+str(len(df_log)))

    # filter by NextSong action
    df_log.get('page',default='not found')
    df_cleaned=etl_f.filter_df(df_log,'page','NextSong')

    # convert timestamp column to datetime
    etl_f.create_time_df(df_log)

    # insert time data records
    new_time_df=etl_f.insert_time_df(cur,conn,df_cleaned)

    # load user table
    user_df = etl_f.set_user_df(df_cleaned)

    # insert user records
    print("Dataframe df_user length: "+str(len(user_df)))
    len_user_df=etl_f.insert_user_df(cur,conn,user_df)

    # insert songplay records
    len_songplays_df=etl_f.insert_songplays(cur, conn,df_log,False)



def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    print("Init")
    df_song=process_song_file(cur,conn,filepath='data/song_data')
    df_log=process_log_file(cur, conn, filepath='data/log_data')

    conn.close()


if __name__ == "__main__":
    main()
