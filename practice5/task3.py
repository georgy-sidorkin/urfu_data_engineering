from pymongo import MongoClient
import pickle
import json


def connect_to_mongodb(db_name="test-database"):
    client = MongoClient()
    db = client[db_name]
    return db.person


def load_pkl_data(path):
    with open(path, "rb") as input:
        items = pickle.load(input)
    return items


def delete_by_salary(collection):
    result = collection.delete_many({
        "$or": [
            {"salary": {"$lt": 25000}},
            {"salary": {"$gt": 175000}}
        ]
    })

    print(result)


def update_age(collection):
    result = collection.update_many({}, {
        "$inc": {
            "age": 1
        }
    })

    print(result)


def increase_salary_by_job(collection):
    filter = {
        "job": {"$in": ["Учитель", "Косметолог", "Медсестра", "IT-специалист"]}
    }

    update = {
        "$mul": {
            "salary": 1.05
        }
    }

    result = collection.update_many(filter, update)
    print(result)


def increase_salary_by_city(collection):
    filter = {
        "city": {"$in": ["Москва", "Санкт-Петербург", "Барселона", "Гранада"]}
    }

    update = {
        "$mul": {
            "salary": 1.07
        }
    }

    result = collection.update_many(filter, update)
    print(result)


def increase_salary_by_many_conditions(collection):
    filter = {
        "city": {"$in": ["Москва", "Санкт-Петербург", "Барселона", "Гранада"]},
        "job": {"$in": ["Учитель", "Косметолог", "Медсестра", "IT-специалист"]},
        "age": {"$gt": 20, "$lt": 50},
    }

    update = {
        "$mul": {
            "salary": 1.1
        }
    }

    result = collection.update_many(filter, update)
    print(result)


def delete_by_many_conditions(collection):
    result = collection.delete_many({
        "year": {"$lt": 2000},
        "$or": [
            {"age": {"$lt": 18}},
            {"age": {"$gt": 55}}
        ]
    })

    print(result)


data = load_pkl_data("tasks/task_3_item.pkl")
conn = connect_to_mongodb()
# conn.insert_many(data)

delete_by_salary(conn)
increase_salary_by_job(conn)
increase_salary_by_city(conn)
increase_salary_by_many_conditions(conn)
delete_by_many_conditions(conn)
