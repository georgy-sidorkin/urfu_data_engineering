import json
import sqlite3

import pandas as pd


def load_pickle(file_name):
    data = pd.read_pickle(file_name)
    for item in data:
        del item["acousticness"]
        del item["energy"]

        item["duration_ms"] = int(item["duration_ms"])
        item["year"] = int(item["year"])
        item["tempo"] = float(item["tempo"])
        item["popularity"] = int(item["popularity"])

    return data


def load_json(file_name):
    with open(file_name, "r", encoding="utf-8") as input:
        data = json.load(input)
        for item in data:
            del item["explicit"]
            del item["danceability"]

            item["duration_ms"] = int(item["duration_ms"])
            item["year"] = int(item["year"])
            item["tempo"] = float(item["tempo"])
            item["popularity"] = int(item["popularity"])

    return data


def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row

    return connection


def insert_data(db, data):
    cursor = db.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS songs (artist text, song text, duration_ms int, year int, 
                                        tempo real, genre text, popularity int)""")

    cursor.executemany("""
        INSERT INTO songs (artist, song, duration_ms, year, tempo, genre, popularity)
        VALUES(
            :artist, :song, :duration_ms, :year, :tempo, :genre, :popularity
        )
    """, data)

    db.commit()


def get_top_by_popularity(db, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM songs ORDER BY popularity DESC LIMIT ?", [limit])
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items


def get_stat_by_duration(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            SUM(duration_ms) as sum,
            AVG(duration_ms) as avg,
            MIN(duration_ms) as min,
            MAX(duration_ms) as max
        FROM songs
    """)

    result = dict(res.fetchone())
    cursor.close()

    return result


def get_freq_by_artist(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            artist,
            COUNT(song) as cnt
        FROM songs
        GROUP BY artist
    """)

    result = list()

    for row in res.fetchall():
        result.append(dict(row))
    cursor.close()

    return result


def filter_by_tempo(db, min_tempo, limit=10):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM songs
        WHERE tempo > ? 
        ORDER BY popularity DESC
        LIMIT ?
    """, [min_tempo, limit])

    result = list()

    for row in res.fetchall():
        result.append(dict(row))
    cursor.close()

    return result


songs = load_pickle('tasks/task_3_var_80_part_1.pkl') + load_json('tasks/task_3_var_80_part_2.json')

conn = connect_to_db('db3')

insert_data(conn, songs)

top_popularity = get_top_by_popularity(conn, 80+10)
with open("results/task3_top_popularity.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(top_popularity, ensure_ascii=False))

duration_stats = get_stat_by_duration(conn)
with open("results/task3_duration_stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(duration_stats, ensure_ascii=False))

artist_freq = get_freq_by_artist(conn)
with open("results/task3_artist_freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(artist_freq, ensure_ascii=False))

filtered_songs = filter_by_tempo(conn, 150, 80+15)
with open("results/task3_filtered_songs.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_songs, ensure_ascii=False))
