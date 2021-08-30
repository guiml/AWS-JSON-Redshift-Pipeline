import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS TBL_STG_EVENTS"
staging_songs_table_drop = "DROP TABLE IF EXISTS TBL_STG_SONGS"
songplay_table_drop = "DROP TABLE IF EXISTS tbl_songplay"
user_table_drop = "DROP TABLE IF EXISTS tbl_users"
song_table_drop = "DROP TABLE IF EXISTS tbl_songs"
artist_table_drop = "DROP TABLE IF EXISTS tbl_artists"
time_table_drop = "DROP TABLE IF EXISTS tbl_times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS tbl_stg_events (
    artist varchar,
    auth varchar,
    firstname varchar,
    gender varchar,
    iteminsession bigint,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionid bigint,
    song varchar,
    status bigint,
    ts bigint,
    useragent varchar,
    userid bigint
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS tbl_stg_songs (
    num_songs bigint,
    artist_id varchar,
    artist_latitude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year int
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS tbl_songplay (
    songplay_id bigint IDENTITY(0, 1),
    start_time time,
    user_id varchar,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS tbl_users (
    user_id varchar,
    firstname varchar,
    lastname varchar,
    gender varchar,
    level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS tbl_songs (
    song_id varchar,
    title varchar,
    artist_id varchar,
    year bigint,
    duration numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS tbl_artists (
    artist_id varchar,
    name varchar,
    location varchar,
    latitude varchar,
    longitude varchar
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS tbl_times (
    start_time time,
    hour bigint,
    day bigint,
    week bigint,
    month bigint,
    year bigint,
    weekday bigint
)
""")

# STAGING TABLES

staging_events_copy = """
    copy tbl_stg_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role=arn:aws:iam::376484744975:role/dwhRole'
    json 's3://udacity-dend/log_json_path.json' compupdate off region 'us-west-2';
"""

staging_songs_copy = """
    copy tbl_stg_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role=arn:aws:iam::376484744975:role/dwhRole'
    json 'auto' compupdate off region 'us-west-2';
"""

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO tbl_songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent    
)
SELECT DISTINCT
timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
se.userid,
se.level,
ss.song_id,
ss.artist_id,
se.sessionid,
se.location,
se.useragent
FROM tbl_stg_events se
JOIN tbl_stg_songs ss ON se.artist = ss.artist_name AND se.song = ss.title 
""")

user_table_insert = ("""
INSERT INTO tbl_users (
    user_id,
    firstname,
    lastname,
    gender,
    level    
)
SELECT DISTINCT  
    userid,
    firstname,
    lastname,
    gender,
    level
FROM tbl_stg_events;
""")

song_table_insert = ("""
INSERT INTO tbl_songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT  
    song_id,
    title,
    artist_id,
    year,
    duration
FROM tbl_stg_songs;
""")

artist_table_insert = ("""
INSERT INTO tbl_artists (
    artist_id,
    name,
    location,
    latitude,
    longitude    
)
SELECT DISTINCT  
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM tbl_stg_songs;
""")

time_table_insert = ("""
INSERT INTO tbl_times (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday   
)
SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
extract(hour from start_time) as hour,
extract(day from start_time) as week,
extract(week from start_time) as week,
extract(month from start_time) as month,
extract(year from start_time) as year,
extract(weekday from start_time) as weekday
from tbl_stg_events se; 
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

