import json
import pandas as pd
import sqlite3


def parse_file(file_name):
    df = pd.read_pickle(file_name)
    return df


def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row

    return connection


def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany(f"""
        INSERT INTO tournaments (id, name, city, begin, system, tours_count, min_rating, time_on_game)
        VALUES(
            :id, :name, :city, :begin, :system, :tours_count, :min_rating, :time_on_game
        )
    """, data)

    db.commit()


def get_top_by_rating(db, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM tournaments ORDER BY min_rating DESC LIMIT ?", [limit])
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items


def get_stat_by_time(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            SUM(time_on_game) as sum,
            AVG(time_on_game) as avg,
            MIN(time_on_game) as min,
            MAX(time_on_game) as max
        FROM tournaments
    """)

    result = dict(res.fetchone())
    cursor.close()

    return result


def get_freq_by_system(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            system,
            COUNT(id) as cnt
        FROM tournaments
        GROUP BY system
    """)

    result = list()

    for row in res.fetchall():
        result.append(dict(row))
    cursor.close()

    return result


def filter_by_tours(db, min_tours_count, limit=10):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM tournaments
        WHERE tours_count > ? 
        ORDER BY min_rating DESC
        LIMIT ?
    """, [min_tours_count, limit])

    result = list()

    for row in res.fetchall():
        result.append(dict(row))
    cursor.close()

    return result


# read data
df = parse_file("tasks/task_1_var_80_item.pkl")
# connect to db
conn = connect_to_db("db1")

# inser data into table
insert_data(conn, df)

sorted_df = get_top_by_rating(conn, 80+10)
with open("results/task1_sorted_by_rating.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_df))

stats = get_stat_by_time(conn)
with open("results/task1_stats_by_time.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(stats))

freq = get_freq_by_system(conn)
with open("results/task1_freq_system.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(freq))

filtered_df = filter_by_tours(conn, min_tours_count=10, limit=80+10)
with open("results/task1_filter_min_tours.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_df))
