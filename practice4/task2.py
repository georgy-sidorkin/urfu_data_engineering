import json
import sqlite3
import msgpack


def load_data(file_name):
    with open(file_name, "rb") as f:
        byte_data = f.read()
    data = msgpack.unpackb(byte_data)
    return data


def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row

    return connection


def insert_data(db, data):
    cursor = db.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS places (name text, place int, prise int)""")

    cursor.executemany("""
        INSERT INTO places (name, place, prise)
        VALUES(
            :name, :place, :prise
        )
    """, data)

    db.commit()


def first_query(db, title):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM places
        WHERE name = (SELECT name FROM tournaments WHERE city = ?)
        """, [title])
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)

    cursor.close()
    return items


def second_query(db, title):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            SUM(prise) AS total_prise
        FROM places
        WHERE name = (SELECT name from tournaments WHERE city = ?)
    """, [title])
    result = dict(res.fetchone())

    cursor.close()
    return result


def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT t.system,
                MAX(p.prise) AS max_prise
        FROM places p
        LEFT JOIN tournaments t ON p.name = t.name
        GROUP BY t.system
    """)
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)

    cursor.close()
    return items


df = load_data("tasks/task_2_var_80_subitem.msgpack")
conn = connect_to_db("db2")

insert_data(conn, df)

first = first_query(conn, "Барселона")
with open("results/task2_query_1.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(first))

second = second_query(conn, "Барселона")
with open("results/task2_query_2.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(second))

third = third_query(conn)
with open("results/task2_query_3.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(third))
