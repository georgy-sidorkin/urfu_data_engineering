from pymongo import MongoClient
import msgpack
import json


def connect_to_mongodb(db_name="test-database"):
    client = MongoClient()
    db = client[db_name]
    return db.person


def load_data(file_name):
    with open(file_name, "rb") as f:
        byte_data = f.read()
    data = msgpack.unpackb(byte_data)
    return data


def get_stat_by_salary(collection):
    q = [
        {
            "$group": {
                "_id": "result",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"}
            }
        }
    ]
    return [stat for stat in collection.aggregate(q)]


def get_freq_by_job(collection):
    q = [
        {
            "$group": {
                "_id": "$job",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ]
    return [stat for stat in collection.aggregate(q)]


def get_stat_by_column(collection, group_name, stat_name):
    q = [
        {
            "$group": {
                "_id": f"${group_name}",
                "max": {"$max": f"${stat_name}"},
                "min": {"$min": f"${stat_name}"},
                "avg": {"$avg": f"${stat_name}"}
            }
        }
    ]
    return [stat for stat in collection.aggregate(q)]


def get_max_salary_by_min_age_match(collection):
    q = [
        {
            "$group": {
                "_id": "$age",
                "max_salary": {"$max": "$salary"},
            }
        },
        {
            "$group": {
                "_id": "result",
                "min_age": {"$min": "$_id"},
                "max_salary": {"$max": "$max_salary"}
            }
        }
    ]

    return [stat for stat in collection.aggregate(q)]


def get_min_salary_by_max_age_match(collection):
    q = [
        {
            "$group": {
                "_id": "$age",
                "min_salary": {"$min": "$salary"},
            }
        },
        {
            "$group": {
                "_id": "result",
                "max_age": {"$max": "$_id"},
                "min_salary": {"$min": "$min_salary"}
            }
        }
    ]

    return [stat for stat in collection.aggregate(q)]


def get_sorted_stat_by_condition(collection):
    q = [
        {
            "$match": {
                "salary": {"$gt": 50000}
            }
        },
        {
            "$group": {
                "_id": "$city",
                "min": {"$min": "$age"},
                "max": {"$max": "$age"},
                "avg": {"$avg": "$age"}
            }
        },
        {
            "$sort": {
                "max": -1
            }
        }
    ]
    return [stat for stat in collection.aggregate(q)]


def get_salary_stat_by_condition(collection):
    q = [
        {
            "$match": {
                "city": {"$in": ["Москва", "Прага", "Малага"]},
                "job": {"$in": ["IT-специалист", "Повар", "Учитель"]},
                "$or": [
                    {"age": {"$gt": 18, "$lt": 25}},
                    {"age": {"$gt": 50, "$lt": 65}}
                ]
            }
        },
        {
            "$group": {
                "_id": "_result",
                "min": {"$min": "$salary"},
                "max": {"$max": "$salary"},
                "avg": {"$avg": "$salary"},
            }
        }
    ]
    return [stat for stat in collection.aggregate(q)]


def get_avg_salary_by_job_in_moscow(collection):
    q = [
        {
            "$match": {"city": "Москва"}
        },
        {
            "$group": {
                "_id": "$job",
                "avg": {"$avg": "$salary"},
            }
        },
        {
            "$sort": {"avg": -1}
        }
    ]
    return [stat for stat in collection.aggregate(q)]


df = load_data("tasks/task_2_item.msgpack")
conn = connect_to_mongodb()
# conn.insert_many(df)

salary_stats = get_stat_by_salary(conn)
with open("results/task2_salary_stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(salary_stats, ensure_ascii=False, default=str))

job_freq = get_freq_by_job(conn)
with open("results/task2_job_freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(job_freq, ensure_ascii=False, default=str))

salary_stats_by_city = get_stat_by_column(conn, "city", "salary")
with open("results/task2_salary_stats_by_city.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(salary_stats_by_city, ensure_ascii=False, default=str))

salary_stats_by_job = get_stat_by_column(conn, "job", "salary")
with open("results/task2_salary_stats_by_job.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(salary_stats_by_job, ensure_ascii=False, default=str))

age_stats_by_city = get_stat_by_column(conn, "city", "age")
with open("results/task2_age_stats_by_city.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(age_stats_by_city, ensure_ascii=False, default=str))

age_stats_by_job = get_stat_by_column(conn, "job", "age")
with open("results/task2_age_stats_by_job.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(age_stats_by_job, ensure_ascii=False, default=str))

max_salary_min_age = get_max_salary_by_min_age_match(conn)
with open("results/task2_max_salary_min_age.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(max_salary_min_age, ensure_ascii=False, default=str))

min_salary_max_age = get_min_salary_by_max_age_match(conn)
with open("results/task2_min_salary_max_age.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(min_salary_max_age, ensure_ascii=False, default=str))

sorted_stats_condition = get_sorted_stat_by_condition(conn)
with open("results/task2_sorted_stats_condition.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_stats_condition, ensure_ascii=False, default=str))

salary_stats_condition = get_salary_stat_by_condition(conn)
with open("results/task2_salary_stats_condition.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(salary_stats_condition, ensure_ascii=False, default=str))

avg_salary_in_msc = get_avg_salary_by_job_in_moscow(conn)
with open("results/task2_avg_salary_in_msc.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(avg_salary_in_msc, ensure_ascii=False, default=str))
