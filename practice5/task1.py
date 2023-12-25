from pymongo import MongoClient
import json


def connect_to_mongodb(db_name="test-database"):
    client = MongoClient()
    db = client[db_name]
    return db.person


def parse_data(file_name):
    items = []
    with open(file_name, "r", encoding="utf-8") as f:
        lines = f.readlines()
        item = dict()
        for line in lines:
            if line == "=====\n":
                items.append(item)
                item = dict()
            else:
                line = line.strip()
                splitted = line.split("::")

                if splitted[0] in ["salary", "id", "year", "age"]:
                    item[splitted[0]] = int(splitted[1])
                else:
                    item[splitted[0]] = splitted[1]

    return items


def sort_by_salary(collection):
    items = []
    for person in collection.find(limit=10).sort({"salary": -1}):
        items.append(person)
    return items


def filter_by_age(collection):
    items = []
    for person in (collection
                  .find({"age": {"$lt": 30}}, limit=15)
                  .sort({"salary": -1})):
        items.append(person)
    return items


def filter_by_city_and_job(collection):
    items = []
    for person in (collection
                  .find({"city": "Рига",
                         "job": {"$in": ["Врач", "Строитель", "Повар"]}}, limit=10)
                  .sort({"age": 1})):
        items.append(person)
    return items


def count_obj(collection):
    res = collection.count_documents({
        "age": {"$gt": 20, "$lt": 30},
        "year": {"$gte": 2019, "$lte": 2022},
        "$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]
    })
    return res


data = parse_data("tasks/task_1_item.text")

conn = connect_to_mongodb()
# conn.insert_many(data)

sorted_salary = sort_by_salary(conn)
with open("results/task1_sorted_by_salary.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_salary, ensure_ascii=False, default=str))

filtered_age = filter_by_age(conn)
with open("results/task1_filtered_age.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_age, ensure_ascii=False, default=str))

filtered_city_job = filter_by_city_and_job(conn)
with open("results/task1_filtered_city_job.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_city_job, ensure_ascii=False, default=str))

cnt_obj = count_obj(conn)
with open("results/task1_cnt_obj.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(cnt_obj, ensure_ascii=False, default=str))
