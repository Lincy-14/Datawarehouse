import configparser
import psycopg2

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#KEY=config.get('AWS','KEY')
#SECRET=config.get('AWS','SECRET')
DB_NAME=config.get('CLUSTER','DB_NAME')
DB_USER= config.get("CLUSTER","DB_USER")
DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
DB_PORT = config.get("CLUSTER","DB_PORT")

# DROP TABLES

#drop_schema_table = "DROP SCHEMA IF EXISTS sparkify cascade"
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession integer,
lastName varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId int,
song varchar,
status int,
ts timestamp,
userAgent varchar,
userid int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int,
artist_id varchar,
artist_latitude numeric,
artist_longitude numeric,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id int IDENTITY(0,1) PRIMARY KEY not null sortkey,
start_time timestamp, 
user_id int not null,
level varchar,
song_id varchar not null distkey,
artist_id varchar not null,
session_id int not null,
location varchar,
user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY not null sortkey,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY not null sortkey,
title varchar,
artist_id varchar,
year int,
duration float
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY not null sortkey,
name varchar,
location varchar,
latittude varchar,
longitude varchar
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY not null sortkey,
hour int,
day int,
week int,
month int,
year int,
weekday int
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {s3bucket}
CREDENTIALS 'aws_iam_role={iam_role}'
REGION AS 'us-west-2'
format as json {log_json_path}
TIMEFORMAT as 'epochmillisecs';
""").format(s3bucket=config['S3']['LOG_DATA'],iam_role=config['IAM_ROLE']['ARN'],log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs from {s3bucket}
CREDENTIALS 'aws_iam_role={iam_role}'
REGION AS 'us-west-2'
format as json 'auto';
""").format(s3bucket=config['S3']['SONG_DATA'],iam_role=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
(
    start_time,user_id,level,song_id,artist_id,session_id,location,user_agent
)
SELECT DISTINCT se.ts AS start_time,
                se.userid,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionid ,
                se.location,
                se.useragent as user_agent
FROM staging_events se 
JOIN staging_songs ss 
ON (se.artist = ss.artist_name) 
AND (se.song = ss.title)
WHERE se.userid IS NOT NULL
                AND ss.artist_id IS NOT NULL
                AND se.level IS NOT NULL
                AND se.sessionId IS NOT NULL
                AND ss.song_id IS NOT NULL
                AND se.ts IS NOT NULL
                AND se.page='NextSong';        
""")

user_table_insert = ("""
INSERT INTO users 
(
    user_id,first_name,last_name,gender,level
)
SELECT DISTINCT se.userid as user_id,
                se.firstname,
                se.lastname,
                se.gender,
                se.level
FROM staging_events se
WHERE se.userid IS NOT NULL
                AND page = 'NextSong'
                AND se.userid NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
INSERT INTO songs
(
    song_id,title,artist_id,year,duration
)
SELECT DISTINCT ss.song_id,
                ss.title,
                ss.artist_id,
                ss.year,
                ss.duration
FROM staging_songs ss 
WHERE ss.song_id IS NOT NULL
        AND ss.title IS NOT NULL
        AND ss.artist_id IS NOT NULL
        AND ss.song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
INSERT INTO artists 
(
    artist_id,name,location,latittude,longitude
)
SELECT DISTINCT ss.artist_id as artist_id,
                ss.artist_name as name,
                ss.artist_location as location,
                ss.artist_latitude as latittude,
                ss.artist_longitude as longitude
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL
                AND ss.artist_name is NOT NULL
                AND ss.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
INSERT INTO time 
(
    start_time,hour,day,week,month,year,weekday
)
SELECT DISTINCT se.ts as start_time,
        EXTRACT (hour from se.ts) as hour,
        EXTRACT (day from se.ts) as day,
        EXTRACT (week from se.ts) as week,
        EXTRACT (month from se.ts) as month,
        EXTRACT (year from se.ts) as year,
        EXTRACT (weekday from se.ts) as weekday
FROM staging_events se
where se.ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# SELECT DISTINCT (TO_CHAR(se.ts :: DATETIME, 'YYYY-MM-DD HH:MI:SS')::timestamp) as start_time,