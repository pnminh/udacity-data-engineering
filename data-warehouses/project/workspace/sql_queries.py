import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession int,
    lastName text,
    length numeric,
    level text,
    location text,
    method text,
    page text,
    registration bigint,
    sessionId int,
    song text,
    status smallint,
    ts bigint,
    userAgent text,
    userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    artist_id text,
    artist_latitude numeric,
    artist_location text,
    artist_longitude numeric,
    artist_name text,
    duration numeric,
    num_songs int,
    song_id text,
    title text,
    year int
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id bigint PRIMARY KEY, 
    first_name text, 
    last_name text, 
    gender text, 
    level text
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id text PRIMARY KEY, 
    title text, 
    artist_id text, 
    year int, 
    duration numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id text PRIMARY KEY, 
    name text, 
    location text, 
    latitude numeric, 
    longtitude numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time bigint PRIMARY KEY, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id text PRIMARY KEY, 
    start_time bigint NOT NULL REFERENCES time(start_time), 
    user_id int NOT NULL REFERENCES users(user_id), 
    level text, 
    song_id text REFERENCES songs(song_id), 
    artist_id text REFERENCES artists(artist_id), 
    session_id int, 
    location text, 
    user_agent text 
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}' 
REGION {}
JSON {};
"""). \
    format(config['S3']['LOG_DATA'],
           config['IAM_ROLE']['ARN'],
           config['S3']['REGION'],
           config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto'
REGION {};
""").\
    format(config['S3']['SONG_DATA'],
           config['IAM_ROLE']['ARN'],
           config['S3']['REGION'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(
    songplay_id, start_time, user_id, level, song_id, 
    artist_id, session_id, location, user_agent
) 
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT(user_id) DO UPDATE
SET level=EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration) 
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT(song_id) DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longtitude) 
VALUES(%s,%s,%s,%s,%s) 
ON CONFLICT(artist_id) DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
VALUES(%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(start_time) DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,
                        songplay_table_insert]
