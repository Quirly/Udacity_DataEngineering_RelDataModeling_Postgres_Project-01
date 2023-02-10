import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import csv

import json
from datetime import datetime as dt

def get_files(filepath):
    '''
    Returns a list of all files with their respective paths of a given folder. 
    (Here it is used to collect all file paths in a dynamic way.)

            Parameters:
                    filepath: Path to folder where files are located

            Returns:
                    list of all files in a folder of a given path
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files
    

def read_files_to_df(file_elements):
    '''
    Returns a dataframe with all entries from files. 
    Prints out the number of files collected and read.
    (Here it is used to collect all entries about songs and songplays logged from single files
     and insert all of them in a dataframe.)

            Parameters:
                    file_elements: List of files (with path)

            Returns:
                    Dataframe with all single entries collected and read from the files
                    listed in the file_elements list
    '''
    df=None
    n=0

    for file in file_elements:
        #file content in JSON format is read from file
        data_element = pd.read_json(file, lines=True)
        #In 1st iteration (empty) Dataframe is generated
        if df is None:
            df=pd.DataFrame(data_element)
        #If the dataframe exists, in further iterations, 
        #each new line is appended to dataframe generated in 1st iteration
        else:
            #convert file entry to dataframe format
            new_df=pd.DataFrame(data_element)
            print(type(new_df))
            try:
                #reads contents of existing dataframe and adds the new dataframe with new entry
                frames = [df,new_df]
                df = pd.concat(frames)
            except:
                print('Error in file: '+str(n-1))
        n=n+1
    df.reset_index(drop=True, inplace=True)
    df.to_csv('data_quality/test.csv')
    print('Number of files: '+str(n))
    return df

def load_songfiles(filepath):
    '''
    Returns a dataframe with all songs found in song files. 
    First all (song) files in a given folder are read and their paths are added to a list.
    Then each file in this list is loaded, the contents is read and added to the final dataframe.

            Parameters:
                    file_path (path to folder with song files)

            Returns:
                    Dataframe with all single entries collected and read from the song files
                    identified and listed in the file list
    '''
    song_files = get_files(filepath)
    df_song = read_files_to_df(song_files)
    return df_song

def insert_all_records_of_song_data(df):
    '''    
    Returns all records of song dataframe. 

            Parameters:
                    df: Name of dataframe with songs

            Returns:
                    array of all song dataframe records
    '''
    song_data_cols = ['song_id','title','artist_id','year','duration']
    song_data = df[song_data_cols].values
    return song_data

def songs_df(df):
    '''    
    Extracts all song related columns and generates a new song only dataframe. 

            Parameters:
                    df: Name of dataframe with songs

            Returns:
                    Dataframe with extracted song related columns only based on songs dataframe
    '''
    print(df.columns)
    songs_df = pd.DataFrame()
    song_data_cols = ['song_id','title','artist_id','year','duration']
    for column in song_data_cols:
        songs_df[column]=df[column]
    songs_df.to_csv('data_quality/songs.csv')
    return songs_df

def insert_songs_df(cur,conn, df_song):
    '''    
    Iterates song only dataframe and inserts entry by entry to table songs using sql_queries.py

            Parameters:
                    df: Name of song data only dataframe

            Returns:
                    Nothing.
                    (Please check Postgres database if songs table has entries)
    '''
    #insert all records
    all_songs_df = songs_df(df_song)
    print(len(df_song))
    for i, row in all_songs_df.iterrows():
        print(row)
        cur.execute(song_table_insert, row)
        conn.commit()
    print("Number of rows in songs dataframe: " + str(i+1))
    len_val=len(all_songs_df)
    return len_val

def insert_all_records_of_artist_data(df):
    '''    
    Extracts artist related column values of all records and returns array with the read information. 

            Parameters:
                    df: Name of dataframe with songs where artist information can be fojnd

            Returns:
                    Dataframe with extracted artist related columns only based on songs dataframe
    '''
    artist_data_cols = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data = df[artist_data_cols].values
    return artist_data

def artists_df(df):
    '''    
    Extracts all artist related columns and generates a new artists only dataframe. 

            Parameters:
                    df: Name of dataframe with songs data read from files

            Returns:
                    Dataframe with extracted artist related columns only based on songs dataframe
    '''
    artists_df = pd.DataFrame()
    artist_data_cols = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    for column in artist_data_cols:
        artists_df[column]=df[column]
        print("artists column: "+column)
    artists_df.to_csv('data_quality/artists.csv')
    return artists_df

def insert_artists_df(cur,conn,df):
    '''    
    Iterates artist only dataframe and inserts entry by entry to table artists using sql_queries.py

            Parameters:
                    df: Name of dataframe with songs data read from files

            Returns:
                    Nothing.
                    (Please check Postgres database if ARTISTS table has entries)
    '''
    #insert all records
    all_artists_df = artists_df(df)
    print("Artists Dataframe: "+str(len(all_artists_df)))
    for i, row in all_artists_df.iterrows():
        cur.execute(artist_table_insert, row)
        conn.commit()
    len_all_artists_df=i
    print("Number of rows in artist dataframe: " + str(i+1))
    return len_all_artists_df

def load_logfiles(filepath):
    '''
    Returns a dataframe with all log data found in log files. 
    First all (log) files in a given folder are read and their paths are added to a list.
    Then each file in this list is loaded, the contents is read and added to the final dataframe.

            Parameters:
                    filepath (path to folder with log files)

            Returns:
                    Dataframe with all single entries collected and read from the log files
                    identified and listed in the file list
    '''
    log_files = get_files(filepath)
    df_log = read_files_to_df(log_files)
    df_log.to_csv('data_quality/logs.csv')
    return df_log

def filter_df(df,col,parameter):
    '''
    Returns a dataframe filtered by a given parameter in a specific column col. 
            Parameters:
                    df: (dataframe to be filtered)
                    col: column in dataframe to be filtered
                    parameter: filter criterium
            Returns:
                    Dataframe with filtered entries
    '''
    df_cleaned=df.copy()
    print(df_cleaned)
    return df

def ts_converter_dataeng(df,column_labels):
    '''
    Returns dictionary of converted timestamps with elements and lists  
    with elements of converted timestamp in milliseconds in logfile (column ts)
            Parameters:
                    df: dataframe containing timestamp column
                    column_labels: list of names of columns of timestamp elements
            Returns:
                    Dictionary and lists 
    '''
    #Initialize all lists and dictionaries
    dict_time={}
    new_ts=[]
    new_ts_hour=[]
    new_ts_day=[]
    new_ts_week=[]
    new_ts_month=[]
    new_ts_year=[]
    new_ts_weekday=[]
    df_new=df['ts']
    #Append converted timestamp and elements
    for n in df_new:
        time_new_format=dt.fromtimestamp(n // 1000)
        new_ts.append(time_new_format)
        new_ts_hour.append(time_new_format.hour)
        new_ts_day.append(time_new_format.day)
        new_ts_week.append(time_new_format.strftime("%V"))
        new_ts_month.append(time_new_format.month)
        new_ts_year.append(time_new_format.year)
        new_ts_weekday.append(time_new_format.weekday())
    #Create Dictionaries and add lists
    dict_time[column_labels[0]]=new_ts
    dict_time[column_labels[1]]=new_ts_hour
    dict_time[column_labels[2]]=new_ts_day
    dict_time[column_labels[3]]=new_ts_week
    dict_time[column_labels[4]]=new_ts_month
    dict_time[column_labels[5]]=new_ts_year
    dict_time[column_labels[6]]=new_ts_weekday
    return dict_time,new_ts,new_ts_hour,new_ts_day,new_ts_week,new_ts_month,new_ts_year,new_ts_weekday

def dict_to_df(dict):
    '''
    Returns dataframe of given dictionary
            Parameters:
                    dict: Dictionary that shall be converted to dataframe
            Returns:
                    t_df: Dataframe generated from dictionary
    '''
    dict_df=pd.DataFrame([dict])
    t_df=dict_df.apply(pd.Series.explode).reset_index()
    t_df.drop(['index'], axis=1)
    return t_df

def time_data_clean(df):
    '''
    Takes timestamp column of a given dataframe 
            Parameters:
                    df: Dataframe containing timestamp
            Returns:
                    time_df: Dataframe with converted timestamp and elements
    '''
    time_data = ()
    column_labels = ['timestamp','hour','day','week','month','year','weekday']
    
    dict_time,new_ts,new_ts_hour,new_ts_day,new_ts_week,\
    new_ts_month,new_ts_year,new_ts_weekday \
    =ts_converter_dataeng(df,column_labels)
    
    time_df = dict_to_df(dict_time)
    time_df = time_df.iloc[: , 1:]
    return time_df

def add_conv_timestamp_to_df(df_orig, column_name_orig, df, column_name):
    '''
    Adds converted timestamp column to a given dataframe 
            Parameters:
                    df_orig: Dataframe containing original, non converted timestamp
                    df: New Dataframe where timestamp column shall be added
                    column_name_orig: Timestamp column name in original dataframe
                    column_name: Timestamp column name in new dataframe
            Returns:
                    Nothing
    '''
    df[column_name]=df_orig[column_name_orig]
    
def create_time_df(df):
    '''
    Generates converted timestamp from timestamp column in UNIX format and adds it to a given dataframe 
            Parameters:
                    df: Dataframe where timestamp column shall be added
            Returns:
                    Nothing
    '''
    time_df=time_data_clean(df)
    add_conv_timestamp_to_df(time_df,'timestamp',df,'timestamp')
    
def insert_time_df(cur,conn,time_df):
    '''    
    Iterates time dataframe and inserts entry by entry to table time using sql_queries.py

            Parameters:
                    df: Name of dataframe with time data read from dataframe with log data

            Returns:
                    Nothing.
                    (Please check Postgres database if ARTISTS table has entries)
    '''
    #insert all records
    all_time_df = time_data_clean(time_df)
    add_conv_timestamp_to_df(all_time_df,'timestamp',time_df,'timestamp')
    for i, row in all_time_df.iterrows():
        cur.execute(time_table_insert, row)
        conn.commit()
    print("Number of rows in time dataframe: " + str(i+1))
    all_time_df.to_csv('data_quality/time.csv')
    print("Time data table csv file saved.")
    time_df.to_csv('data_quality/logs_plus_timestamp.csv')
    print("Log file with converted timestamp csv file saved.")
    return all_time_df

def set_user_df(df):
    '''    
    Extracts all user related columns and generates a new user data only dataframe. 

            Parameters:
                    df: Name of dataframe with user data read from log files

            Returns:
                    Dataframe with extracted user data related columns only based on log information dataframe
    '''
    user_df = pd.DataFrame()
    user_data_cols = ['userId','firstName','lastName','gender','level']
    for column in user_data_cols:
        user_df[column]=df[column]
    user_df.to_csv('data_quality/users.csv')
    return user_df

def insert_user_df(cur,conn,user_df):
    '''    
    Iterates user dataframe and inserts entry by entry to table users using sql_queries.py

            Parameters:
                    user_df: Name of dataframe with user data read from dataframe with log data

            Returns:
                    Nothing.
                    (Please check Postgres database if USERS table has entries)
    '''
    for i, row in user_df.iterrows():
        try: 
            cur.execute(user_table_insert, row)
            conn.commit()
        except:
            pass
            #print("User Dataframe row: " + str(i) + " not inserted.")
    len_user_df=i
    print("Number of rows in users dataframe: " + str(i+1))
    return len_user_df
    
def insert_songplays(cur,conn,df,print_option):
    '''    
    Iterates dataframe and inserts entry by entry to table songplays
    given that artist_id in songs and artist_id in logs are equal using sql_queries.py

            Parameters:
                    df: Name of filtered dataframe with song data read from dataframe with songs data

            Returns:
                    Nothing.
                    (Please check Postgres database if SONGPLAYS table has entries)
    '''
    try:
        ct=0
        for index, row in df.iterrows():
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist,str(row.length)))
            results = cur.fetchone()
            songplays=[]
            if results:
                songid, artistid = results
                print("result/match for artist: "+str(row.artist))
                print("Song: "+str(row.song)+"\n" + "Artist: " + str(row.artist)+"\n" + "Listening time in s (length): " \
                +str(row.length) +"\n" + str("Song ID: ") + str(songid)+"\n"+"Artist ID: "+str(artistid))
                songplay_data = (row.timestamp,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
                songplays.append(songplay_data)
                cur.execute(songplay_table_insert, songplay_data)
                conn.commit()
                ct=ct+1
            else:
                songid, artistid = None, None
                if print_option==True:
                        print("No results/match for artist: " + str(row.artist))
        print("Number of results: "+str(ct))
        print(str(ct)+ " dataset successfully inserted.")
    except:
        print("No data inserted, error in execution.")
    songplays_df=pd.DataFrame(songplays)
    songplays_df.to_csv("data_quality/songplays.csv")
    return ct