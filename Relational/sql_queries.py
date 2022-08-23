# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# fact table
songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id int,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar,
    PRIMARY KEY (songplay_id)
)
""")

# dimension table 1
user_table_create = ("""
CREATE TABLE users
(
    user_id int,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar,
    PRIMARY KEY (user_id)
)
""")


# dimension table 2
song_table_create = ("""
CREATE TABLE songs
(
    song_id varchar,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int,
    duration double precision NOT NULL,
    PRIMARY KEY (song_id)
)
""")


# dimension table 3
artist_table_create = ("""
CREATE TABLE artists
(
    artist_id varchar,
    name varchar NOT NULL,
    location varchar,
    latitude double precision,
    longitude double precision,
    PRIMARY KEY (artist_id)
)
""")


# dimension table 4
time_table_create = ("""
CREATE TABLE time
(
    start_time timestamp,
    hour int,
    day int,
    weak int,
    month int,
    year int,
    weekday int,
    PRIMARY KEY (start_time)
)
""")


# INSERT RECORDS
songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO NOTHING
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO UPDATE SET 
    first_name  = EXCLUDED.first_name,
    last_name  = EXCLUDED.last_name,
    gender  = EXCLUDED.gender,
    level  = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING
""")


artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, weak, month, year, weekday)
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs s
join artists a
on s.artist_id = a.artist_id
WHERE s.title = %s AND s.duration = %s AND a.name = %s;
""")


# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]