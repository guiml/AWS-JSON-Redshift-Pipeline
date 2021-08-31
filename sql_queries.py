import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS tbl_stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS tbl_stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

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
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint IDENTITY(0, 1) PRIMARY KEY,
    start_time time,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id varchar,
    location varchar,
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar NOT NULL PRIMARY KEY,
    firstname varchar NOT NULL,
    lastname varchar NOT NULL,
    gender varchar,
    level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year bigint,
    duration numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude varchar,
    longitude varchar
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time time NOT NULL PRIMARY KEY,
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
    credentials  'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json' compupdate off region 'us-west-2';
""".format(config.get("IAM_ROLE","ARN"))

#SONG_DATA='s3://udacity-dend/song_data/A/A'.
staging_songs_copy = """
    copy tbl_stg_songs from 's3://udacity-dend/song_data'
    credentials  'aws_iam_role={}'
    json 'auto' compupdate off region 'us-west-2';
""".format(config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
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
INSERT INTO users (
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
FROM tbl_stg_events se
WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(
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
INSERT INTO artists (
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
INSERT INTO time (
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
FROM tbl_stg_events se
WHERE se.page = 'NextSong'; 
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

