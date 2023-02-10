# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = """
CREATE TABLE IF NOT EXISTS users
(id SERIAL PRIMARY KEY,
user_id VARCHAR UNIQUE,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs
(id SERIAL PRIMARY KEY,
song_id VARCHAR,
title text,
artist_id varchar,
year INTEGER,
duration text
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists
(id SERIAL PRIMARY KEY,
artist_id VARCHAR,
name text NOT NULL,
location VARCHAR(50),
latitude NUMERIC(7,2),
longitude NUMERIC(7,2)
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time
(id SERIAL PRIMARY KEY,
start_time timestamp,
hour int,
day int,
week int,
month int,
year int,
weekday int
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays
(songplay_id SERIAL PRIMARY KEY,
start_time timestamp,
user_id VARCHAR,
level varchar,
song_id VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
session_id varchar,
location varchar,
user_agent varchar)
"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays
(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
INSERT INTO users
(user_id,first_name,last_name,gender,level)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
"""
song_table_insert = """
INSERT INTO songs
(song_id, title, artist_id,year,duration)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""

artist_table_insert = """
INSERT INTO artists
(artist_id,name,location,latitude,longitude)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""


time_table_insert = """
INSERT INTO time
(start_time,hour,day,week,month,year,weekday)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
"""

# FIND SONGS

song_select = """
(
SELECT songs.song_id,artists.artist_id
FROM songs
JOIN artists
ON songs.artist_id=artists.artist_id
WHERE songs.title = %s
AND artists.name = %s
AND songs.duration=%s
ORDER BY songs.song_id
)
"""

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
