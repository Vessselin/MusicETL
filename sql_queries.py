# ELIMINAR TABLAS

drop_songplay_table = "DROP TABLE IF EXISTS songplay"
drop_user_table = "DROP TABLE IF EXISTS users"
drop_song_table = "DROP TABLE IF EXISTS songs"
drop_artist_table = "DROP TABLE IF EXISTS artists"
drop_time_table = "DROP TABLE IF EXISTS time"

# CREAR TABLAS

create_songplay_table = ("""
CREATE TABLE IF NOT EXISTS songplays (
        songplay_id VARCHAR PRIMARY KEY, 
        start_time DATE REFERENCES time(start_time), 
        user_id VARCHAR NOT NULL REFERENCES users(user_id), 
        level VARCHAR, 
        song_id VARCHAR REFERENCES songs(song_id), 
        artist_id VARCHAR REFERENCES artists(artist_id), 
        session_id VARCHAR, 
        location TEXT, 
        user_agent VARCHAR);
""")

create_user_table = ("""
CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR);
""")

create_song_table = ("""
CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY, 
        title TEXT NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        year INT, 
        duration FLOAT);
""")

create_artist_table = ("""
CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        name TEXT NOT NULL, 
        location TEXT, 
        latitude FLOAT, 
        longitude FLOAT);
""")

create_time_table = ("""
CREATE TABLE IF NOT EXISTS time (
        start_time DATE PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday VARCHAR);
""")

# INSERTAR REGISTROS

insert_songplay = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) DO NOTHING;
""")

insert_user = ("""
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO NOTHING;
""")

insert_song = ("""
INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
""")

insert_artist = ("""
INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
""")

insert_time = ("""
INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# BUSCAR CANCIONES

select_song = ("""
SELECT song_id, artists.artist_id
    FROM songs JOIN artists ON songs.artist_id = artists.artist_id
        WHERE songs.title = %s
        AND artists.name = %s
        AND songs.duration = %s
""")

# LISTA DE CONSULTAS

create_queries = [create_user_table, create_artist_table, create_song_table, create_time_table, create_songplay_table]
drop_queries = [drop_songplay_table, drop_user_table, drop_song_table, drop_artist_table, drop_time_table]
