import json
import sqlite3
import pandas as pd


def load_data(file_name):
    data = pd.read_csv(file_name, delimiter=";") \
            .fillna({"category": "no"}) \
            .groupby("name", as_index=False) \
            .first() \
            .to_dict("records")
    return data


def load_pickle(file_name):
    data = pd.read_pickle(file_name)
    return data


def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row

    return connection


def insert_data(db, data):
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS products")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (name text UNIQUE, price real, quantity int, category text, 
                                        fromCity text, isAvailable int, views int, version int DEFAULT 0)""")

    cursor.executemany("""
        INSERT INTO products (name, price, quantity, category, fromCity, isAvailable, views)
        VALUES(
            :name, :price, :quantity, :category, :fromCity, :isAvailable, :views
        )
    """, data)
    db.commit()


def delete_by_name(db, name):
    cursor = db.cursor()
    cursor.execute("DELETE FROM products WHERE name = ?", [name])
    db.commit()


def update_price_by_percent(db, name, percent):
    cursor = db.cursor()
    cursor.execute("UPDATE products SET price = ROUND((price * (1 + ?)), 2) WHERE name = ?", [percent, name])
    cursor.execute("UPDATE products SET version = version + 1 WHERE name = ?", [name])
    db.commit()


def update_price(db, name, value):
    cursor = db.cursor()
    res = cursor.execute("UPDATE products SET price = (price + ?) WHERE (name = ?) AND ((price + ?) > 0) ", [value, name, value])
    if res.rowcount > 0:
        cursor.execute("UPDATE products SET version = version + 1 WHERE name = ?", [name])
        db.commit()


def update_available(db, name, param):
    cursor = db.cursor()
    cursor.execute("UPDATE products SET isAvailable = ? WHERE (name = ?)", [param, name])
    cursor.execute("UPDATE products SET version = version + 1 where name = ?", [name])
    db.commit()


def update_quantity(db, name, value):
    cursor = db.cursor()
    res = cursor.execute("UPDATE products SET quantity = (quantity + ?) WHERE (name = ?) AND ((quantity + ?) > 0)",
                         [value, name, value])
    if res.rowcount > 0:
        cursor.execute("UPDATE products SET version = version + 1 WHERE name = ?", [name])
        db.commit()


def handle_update(db, update_items):
    for item in update_items:
        match item["method"]:
            case "remove":
                delete_by_name(db, item["name"])
            case "price_percent":
                update_price_by_percent(db, item["name"], item["param"])
            case "price_abs":
                update_price(db, item["name"], item["param"])
            case "quantity_add" | "quantity_sub":
                update_quantity(db, item["name"], item["param"])
            case "available":
                update_available(db, item["name"], item["param"])
            case _:
                print(f"unknown method {item['method']}")


def get_top_updated(db, limit=10):
    cursor = db.cursor()
    res = cursor.execute("SELECT name, version FROM products ORDER BY version DESC LIMIT ?", [limit])
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items


def get_group_price_stat(db):
    cursor = db.cursor()
    res = cursor.execute("""
    SELECT category,
           SUM(price) AS sum_price,
           MIN(price) AS min_price,
           MAX(price) AS max_price,
           AVG(price) AS avg_price,
           COUNT(name) AS products_count
    FROM products
    GROUP BY category""")
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items


def get_group_quantity_stat(db):
    cursor = db.cursor()
    res = cursor.execute("""
    SELECT category,
           SUM(quantity) AS sum_quantity,
           MIN(quantity) AS min_quantity,
           MAX(quantity) AS max_quantity,
           AVG(quantity) AS avg_quantity
    FROM products
    GROUP BY category""")
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items


def get_top_viewed_products(db, limit=10):
    cursor = db.cursor()
    res = cursor.execute("""
    SELECT name,
           views
    FROM products
    GROUP BY name
    ORDER BY views DESC
    LIMIT ?""", [limit])
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items


products = load_data("tasks/task_4_var_80_product_data.csv")
upd = load_pickle("tasks/task_4_var_80_update_data.pkl")
conn = connect_to_db("db4")

# insert_data(conn, products)
# handle_update(conn, upd)
top_updated = get_top_updated(conn, 10)
with open("results/task4_top_updated.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(top_updated, ensure_ascii=False))

category_price_stats = get_group_price_stat(conn)
with open("results/task4_category_price_stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(category_price_stats, ensure_ascii=False))

category_quantity_stats = get_group_quantity_stat(conn)
with open("results/task4_category_quantity_stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(category_quantity_stats, ensure_ascii=False))

top_viewed = get_top_viewed_products(conn, 10)
with open("results/task4_top_viewed.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(top_viewed, ensure_ascii=False))
