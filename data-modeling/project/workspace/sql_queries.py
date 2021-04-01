# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(songplay_id int, start_time timestamptz, user_id int, level int, song_id int, artist_id int, session_id text, location text, user_agent text )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(user_id int, first_name text, last_name text, gender text, level int)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(song_id text, title text, artist_id text, year int, duration numeric)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(artist_id int, name text, location text, latitude int, longtitude int)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(start_time timestamptz, hour int, day int, week int, month int, year int, weekday int)
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
INSERT INTO songs VALUES(%s,%s,%s,%s,%s)
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]