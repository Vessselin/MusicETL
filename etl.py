import os
import glob
import psycopg2
import pandas as pd
from sql_queries import create_queries, drop_queries, insert_song, insert_artist, insert_user, insert_time, insert_songplay, select_song

def remove_tables(cur, conn):
    """Elimina las tablas existentes."""
    for query in drop_queries:
        cur.execute(query)
        conn.commit()


def setup_tables(cur, conn):
    """Crea nuevas tablas según las consultas definidas."""
    for query in create_queries:
        cur.execute(query)
        conn.commit()


def connect_to_database():
    """Conecta a la base de datos y retorna el cursor y la conexión."""
    conn = psycopg2.connect(
        host="localhost",
        dbname="practica_musica",
        user="practica_musica_user",
        password="dL7lBx6LBhzEftJbtEPq1pNw8FhlzNTI",
        port="5432"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn


def handle_song_file(cur, filepath):
    """Procesa el archivo de canciones y lo inserta en la base de datos."""
    df = pd.read_json(filepath, lines=True)
    for index, row in df.iterrows():
        # Insertar registro de canción
        song_details = row[["song_id", "title", "artist_id", "year", "duration"]].values
        cur.execute(insert_song, song_details)

        artist_details = row[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values
        cur.execute(insert_artist, artist_details)


def handle_log_file(cur, filepath):
    """Procesa el archivo de logs y lo inserta en la base de datos."""
    df = pd.read_json(filepath, lines=True)
    df = df[df['page'] == 'NextSong']

    timestamps = pd.to_datetime(df['ts'], unit='ms') 

    time_records = []
    for ts in timestamps:
        time_records.append([ts, ts.hour, ts.day, ts.week, ts.month, ts.year, ts.day_name()])
    time_columns = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_records, columns=time_columns)

    for _, row in time_df.iterrows():
        cur.execute(insert_time, list(row))

    # Cargar tabla de usuarios
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    for _, row in user_df.iterrows():
        cur.execute(insert_user, row)

    # Insertar registros de "songplay"
    for index, row in df.iterrows():
        cur.execute(select_song, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        songplay_record = (
            index, 
            pd.to_datetime(row["ts"], unit='ms'),
            row["userId"],
            row["level"],
            song_id,
            artist_id,
            row["sessionId"],
            row["location"],
            row["userAgent"]
        )
        cur.execute(insert_songplay, songplay_record)


def process_files(cur, conn, filepath, func):
    """Procesa todos los archivos en un directorio dado usando la función proporcionada."""
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    total_files = len(all_files)
    print('{} archivos encontrados en {}'.format(total_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} archivos procesados.'.format(i, total_files))


def main():
    """Función principal para ejecutar el ETL."""
    cur, conn = connect_to_database()
    
    remove_tables(cur, conn)
    setup_tables(cur, conn)

    process_files(cur, conn, filepath='data/song_data', func=handle_song_file)
    process_files(cur, conn, filepath='data/log_data', func=handle_log_file)

    conn.close()


if __name__ == "__main__":
    main()
