import os
import glob
import psycopg2
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries, song_table_insert, artist_table_insert, user_table_insert, time_table_insert, songplay_table_insert, song_select

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

import psycopg2

def create_database():
    # Conectar a la base de datos existente 
    conn = psycopg2.connect(
        host="localhost",
        dbname="practica_musica",
        user="postgres",
        password="0863",
        port="5432"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn

def process_song_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)
    for index, row in df.iterrows():
        # inserta registro de cancion
        song_data = row[["song_id", "title", "artist_id", "year", "duration"]].values
        cur.execute(song_table_insert, song_data)

        artist_data = row[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values
        cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    #Procesa el archivo de logs y lo inserta en la base de datos.
    df = pd.read_json(filepath, lines=True)
    df = df[df['page']=='NextSong']

    t = pd.to_datetime(df['ts'], unit='ms') 
    
    time_data = []
    for ts in t:
        time_data.append([ts, ts.hour, ts.day, ts.week, ts.month, ts.year, ts.day_name()])
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Cargar tabla de usuarios
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # Insertar registros de "songplay"
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (
            index, 
            pd.to_datetime(row["ts"], unit='ms'),
            row["userId"],
            row["level"],
            songid,
            artistid,
            row["sessionId"],
            row["location"],
            row["userAgent"]
        )
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()
