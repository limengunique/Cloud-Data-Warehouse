import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "Drop table IF EXISTS staging_events"
staging_songs_table_drop = "Drop table IF EXISTS staging_songs"
songplay_table_drop = "Drop table IF EXISTS songplays"
user_table_drop = "Drop table IF EXISTS users"
song_table_drop = "Drop table IF EXISTS songs"
artist_table_drop = "Drop table IF EXISTS artists"
time_table_drop = "Drop table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist varchar, 
                                                                         auth varchar, 
                                                                         firstName varchar, 
                                                                         gender varchar, 
                                                                         itemInSession varchar, 
                                                                         lastName varchar, 
                                                                         length varchar, 
                                                                         level varchar, 
                                                                         location varchar, 
                                                                         method varchar, 
                                                                         page varchar, 
                                                                         registration varchar, 
                                                                         sessionId varchar, 
                                                                         song varchar, 
                                                                         status varchar, 
                                                                         ts bigint, 
                                                                         userAgent varchar, 
                                                                         userId int);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(num_songs varchar, 
                                                                        artist_id varchar, 
                                                                        artist_latitude varchar, 
                                                                        artist_longitude varchar, 
                                                                        artist_location varchar, 
                                                                        artist_name varchar, 
                                                                        song_id varchar, 
                                                                        title varchar, 
                                                                        duration varchar, 
                                                                        year int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id int identity(1,1) Primary Key NOT NULL,
                                                                 start_time timestamp NOT NULL,
                                                                 user_id int NOT NULL DISTKEY,
                                                                 level varchar,
                                                                 song_id varchar,
                                                                 artist_id varchar,
                                                                 session_id varchar,
                                                                 location varchar, 
                                                                 user_agent varchar
                                                                 )
                                                                 sortkey(start_time);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int Primary Key NOT NULL distkey, 
                                                          first_name varchar,
                                                          last_name varchar, 
                                                          gender varchar, 
                                                          level varchar);
                                                          
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar Primary Key NOT NULL, 
                                                          title varchar, 
                                                          artist_id varchar, 
                                                          year int, 
                                                          duration numeric)
                                                          diststyle auto
                                                          sortkey(title);
                            
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar Primary Key NOT NULL, 
                                                              name varchar, 
                                                              location varchar, 
                                                              latitude numeric, 
                                                              longitude numeric)
                                                              diststyle auto;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp Primary Key NOT NULL, 
                                                         hour int NOT NULL, 
                                                         day int NOT NULL, 
                                                         week int NOT NULL, 
                                                         month int NOT NULL, 
                                                         year int NOT NULL, 
                                                         weekday int NOT NULL)
                                                         diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events 
                          FROM 's3://udacity-dend/log_data' 
                          CREDENTIALS 'aws_iam_role={}' 
                          region 'us-west-2' 
                          JSON 's3://udacity-dend/log_json_path.json';
                          """).format(config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""COPY staging_songs
                         FROM 's3://udacity-dend/song_data' 
                         CREDENTIALS 'aws_iam_role={}' 
                         region 'us-west-2' 
                         JSON 'auto' TRUNCATECOLUMNS;
""").format(config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                             SELECT
                             timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
                             l.userId,
                             l.level,
                             s.song_id,
                             s.artist_id,
                             l.sessionId as session_id,
                             s.artist_location as locaation,
                             l.userAgent as user_agent
                             FROM staging_events l 
                             INNER JOIN staging_songs s
                                 ON l.song=s.title AND l.artist=s.artist_name
                             WHERE l.page='NextSong';
                         """)


user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        WITH unique_user AS (
                                             SELECT userId as user_id,
                                                    firstName as first_name,
                                                    lastName as last_name,
                                                    gender,
                                                    level,
                                                    ROW_NUMBER() OVER(PARTITION BY user_id order by user_id) rn
                                             FROM staging_events
                                             WHERE userId is not null
                                             ) 
                        SELECT user_id,
                               first_name,
                               last_name,
                               gender,
                               level
                               FROM unique_user
                        WHERE rn=1
                                             
                     """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT
                               song_id,
                               title,
                               artist_id,
                               year,    
                               duration
                        FROM staging_songs;
                     """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT
                                 artist_id,
                                 artist_name,
                                 artist_location,
                                 artist_latitude,    
                                 artist_longitude
                          FROM staging_songs;
                      """)


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH time_parse AS
(
    SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time 
    FROM staging_events
)
SELECT
    start_time AS start_time,
    EXTRACT (hour from start_time) AS hour,
    EXTRACT (day from start_time) AS day,
    EXTRACT (week from start_time) AS week,
    EXTRACT (month from start_time) AS month,
    EXTRACT (year from start_time) AS year,
    EXTRACT (dow from start_time) AS weekday
FROM time_parse;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
