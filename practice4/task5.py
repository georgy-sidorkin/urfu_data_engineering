import sqlite3
import csv
import json


def connect_to_db(db_name):
    connection = sqlite3.connect(db_name)

    return connection


# Создание таблиц
def create_table(connection):
    cursor = connection.cursor()
    cursor.executescript(
        """
        DROP TABLE IF EXISTS games;
        DROP TABLE IF EXISTS users;
        DROP TABLE IF EXISTS recommendations;
        DROP TABLE IF EXISTS games_metadata;
    
        CREATE TABLE games(
            app_id int primary key,
            title text,
            date_release text,
            win text,
            mac text,
            linux text,
            rating text,
            positive_ratio int,
            user_reviews int,
            price_final real,
            price_original real,
            discount real,
            steam_deck text
        );
    
        CREATE TABLE users(
            user_id int primary key,
            products int,
            reviews int
        );
    
        CREATE TABLE recommendations(
            app_id int,
            helpful int,
            funny int,
            date text,
            is_recommended text,
            hours real,
            user_id int,
            review_id int primary key,
            FOREIGN KEY(app_id) REFERENCES games(app_id),
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );
    
        CREATE TABLE games_metadata(
            app_id int,
            description text,
            tags text,
            FOREIGN KEY(app_id) REFERENCES games(app_id)
        );
        """)

    connection.commit()


# Наполнение таблиц
def insert_into_games_csv(connection, file_name):
    cursor = connection.cursor()

    with open(file_name, 'r', encoding='utf-8') as f:
        next(f)
        rows = csv.reader(f, delimiter=",")

        for row in rows:
            cursor.execute('INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', row)

        connection.commit()


def insert_into_users_csv(connection, file_name):
    cursor = connection.cursor()

    with open(file_name, 'r', encoding='utf-8') as f:
        next(f)
        rows = csv.reader(f, delimiter=",")

        for row in rows:
            cursor.execute('INSERT INTO users VALUES (?, ?, ?)', row)

        connection.commit()


def insert_into_recommendations_csv(connection, file_name):
    cursor = connection.cursor()

    with open(file_name, 'r', encoding='utf-8') as f:
        next(f)
        rows = csv.reader(f, delimiter=",")

        for row in rows:
            cursor.execute('INSERT INTO recommendations VALUES (?, ?, ?, ?, ?, ?, ?, ?)', row)

        connection.commit()


def insert_into_games_metadata_json(connection, file_name):
    cursor = connection.cursor()

    rows = [json.loads(line) for line in open(file_name, 'r', encoding='utf-8')]

    for row in rows:
        cursor.execute('INSERT INTO games_metadata VALUES (?, ?, ?)',
                       (row['app_id'], row['description'], ', '.join(map(str, row['tags']))))
    connection.commit()


# Запросы к бд
def get_count_per_year(connection):
    """
    Нахождение топ 10 лет в 21 веке с самым большим количеством игр за год с позитивной оценкой
    """
    cursor = connection.cursor()
    res = cursor.execute(
        """
        SELECT strftime('%Y', date_release), COUNT(app_id) as cnt
        FROM games
        WHERE LOWER(rating) LIKE '%positive%'
        GROUP BY strftime('%Y', date_release)
        ORDER BY COUNT(app_id) DESC
        LIMIT 10;
        """)

    result = dict(res.fetchall())
    cursor.close()

    return result


def get_games_on_all_os(connection):
    """
    Нахождение ирг, которые поддерживаются и на macOS, и Linux, и Windows
    """
    cursor = connection.cursor()
    res = cursor.execute(
        """
        SELECT app_id, title
        FROM games
        WHERE mac = 'true'
            AND linux = 'true'
            AND win = 'true'
        """)

    result = dict(res.fetchall())
    cursor.close()

    return result


def get_max_reviews_products(connection):
    """
    Нахождение пользователя с наибольшим количеством игр и пользователя с наибольшим количеством отзывов
    """
    cursor = connection.cursor()
    res = cursor.execute(
        """
        SELECT *
        FROM users
        WHERE reviews = (SELECT MAX(reviews) FROM users)
            OR products = (SELECT MAX(products) FROM users)
        """)

    result = {}
    for a, b, c in res:
        result[a] = (a, b, c)
    cursor.close()

    return result


def get_max_hours(connection):
    """
    Нахождение игры, в которой оставивший отзыв пользователь провел больше всего часов
    """
    cursor = connection.cursor()
    res = cursor.execute(
        """
        SELECT r.hours, g.title
        FROM users u
        JOIN recommendations r ON u.user_id = r.user_id
        JOIN games g ON g.app_id = r.app_id
        WHERE r.hours in (SELECT MAX(hours) FROM recommendations)
        """)

    result = dict(res.fetchall())
    cursor.close()

    return result


def get_most_rec_game(connection):
    """
    Нахождение игры, которую рекомендует больше всего игроков
    """
    cursor = connection.cursor()
    res = cursor.execute(
        """
        SELECT g.title, COUNT(r.review_id) as cnt
        FROM recommendations r
        JOIN games g ON g.app_id = r.app_id
        WHERE r.is_recommended = 'true'
        GROUP BY g.title
        ORDER BY COUNT(r.review_id) DESC
        LIMIT 1
        """)

    result = dict(res.fetchall())
    cursor.close()

    return result


def delete_last_10_years(connection):
    """
    Удаление рекомендаций созданных более 10 лет назад
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        DELETE FROM recommendations
        WHERE CAST(strftime('%Y', date) AS INTEGER) <= CAST(date('now', '-10 years') AS INTEGER)
        """)


conn = connect_to_db('db5')
create_table(conn)
insert_into_games_csv(conn, 'data/games.csv')
insert_into_users_csv(conn, 'data/users.csv')
insert_into_recommendations_csv(conn, 'data/recommendations.csv')
insert_into_games_metadata_json(conn, 'data/games_metadata.json')

with open('results/task5_get_count_per_year.json', "w", encoding="utf-8") as f:
    f.write(json.dumps(get_count_per_year(conn)))

with open('results/task5_get_games_on_all_os.json', "w", encoding="utf-8") as f:
    f.write(json.dumps(get_games_on_all_os(conn)))

with open('results/task5_get_max_reviews_products.json', "w", encoding="utf-8") as f:
    f.write(json.dumps(get_max_reviews_products(conn)))

with open('results/task5_get_max_hours.json', "w", encoding="utf-8") as f:
    f.write(json.dumps(get_max_hours(conn)))

with open('results/task5_get_most_rec_game.json', "w", encoding="utf-8") as f:
    f.write(json.dumps(get_most_rec_game(conn)))

delete_last_10_years(conn)
