import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender char(1),
    itemInSession int,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs int,
    artist_id varchar,
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year int
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL REFERENCES time(start_time),
    user_id int NOT NULL REFERENCES users(user_id),
    level varchar ,
    song_id varchar REFERENCES songs(song_id),
    artist_id varchar REFERENCES artists(artist_id),
    session_id int,
    location varchar,
    user_agent varchar,
    UNIQUE(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender char(1),
    level varchar
);
""")


song_table_create = ("""
CREATE TABLE songs (
    song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar,
    year int,
    duration numeric NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude double precision,
    longitude double precision
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
);
""")

# STAGING TABLES
staging_events_copy = ("""
copy staging_events from {} 
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    json {};
;
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {} 
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
(
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
    timestamp 'epoch' + se.ts * interval '0.001 second',
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
LEFT JOIN staging_songs ss
ON ss.title = se.song AND ss.artist_name = se.artist
WHERE se.page = 'NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users 
(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE (userId, ts)
IN
   (SELECT userId, max(ts) as ts
    FROM staging_events
    GROUP BY userId
   )
;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
;
""")

artist_table_insert = ("""
INSERT INTO artists 
(
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
FROM staging_songs
;
""")

time_table_insert = ("""
INSERT INTO time 
(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    timestamp 'epoch' + ts * interval '0.001 second',
    EXTRACT(HOUR FROM (timestamp 'epoch' + ts * interval '0.001 second')),
    EXTRACT(DAY FROM (timestamp 'epoch' + ts * interval '0.001 second')),
    EXTRACT(WEEK FROM (timestamp 'epoch' + ts * interval '0.001 second')),
    EXTRACT(MONTH FROM (timestamp 'epoch' + ts * interval '0.001 second')),
    EXTRACT(YEAR FROM (timestamp 'epoch' + ts * interval '0.001 second')),
    EXTRACT(WEEKDAY FROM (timestamp 'epoch' + ts * interval '0.001 second'))
FROM staging_events
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
