from pymongo import MongoClient
import json
import pandas as pd


def load_csv(filename):
    df = pd.read_csv(filename).to_dict(orient='records')
    return df


def load_json(filename):
    with open(filename, "r", encoding="utf-8") as input:
        data = json.load(input)
    return data


def save_json(path, file):
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(file, ensure_ascii=False, default=str))


def connect_to_mongodb(db_name="test-database"):
    client = MongoClient()
    conn = client[db_name]
    return conn


# выборка
def sort_by_quality(collection):
    items = []
    for wine in collection.find(limit=10).sort({"quality": -1}):
        items.append(wine)
    return items


def get_quality_above_7(collection):
    items = []
    for wine in collection.find({"quality": {"$gte": 7}}).sort({"quality": -1}):
        items.append(wine)
    return items


def sort_by_alco(collection):
    items = []
    for wine in collection.find(limit=30).sort({"alcohol": 1}):
        items.append(wine)
    return items


def get_bad_red_wines(collection):
    items = []
    for wine in collection.find({"quality": {"$lte": 3}, "type": "red"}).sort({"quality": 1}):
        items.append(wine)
    return items


def get_sweet_white_wines(collection):
    items = []
    for wine in collection.find({"type": "white"}, limit=15).sort({"residual sugar": -1}):
        items.append(wine)
    return items


# выборка с агрегацией
def get_freq_by_type(collection):
    q = [
        {
            "$group": {
                "_id": "$type",
                "count": {"$sum": 1}
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


def get_ph_stat_by_condition(collection):
    q = [
        {
            "$match": {
                "quality": {"$gte": 4, "$lte": 6}
            }
        },
        {
            "$group": {
                "_id": "$type",
                "min": {"$min": "$pH"},
                "max": {"$max": "$pH"},
                "avg": {"$avg": "$pH"},
            }
        }
    ]
    return [stat for stat in collection.aggregate(q)]


# обновление/удаление данных
def increase_sulphates_by_type(collection):
    filter = {
        "type": "white"
    }

    update = {
        "$mul": {
            "sulphates": 1.02
        }
    }

    result = collection.update_many(filter, update)
    print(result)


def update_fixed_acidity(collection):
    result = collection.update_many({}, {
        "$inc": {
            "fixed acidity": 0.1
        }
    })

    print(result)


def delete_by_ph(collection):
    result = collection.delete_many({
        "$or": [
            {"pH": {"$lte": 2.9}},
            {"pH": {"$gte": 3.5}}
        ]
    })

    print(result)


def increase_alco_by_density(collection):
    filter = {
        "density": {"$gte": 0.995}
    }

    update = {
        "$mul": {
            "alcohol": 1.01
        }
    }

    result = collection.update_many(filter, update)
    print(result)


def delete_by_quality(collection):
    result = collection.delete_many({
        "quality": {"$lte": 3}
    })

    print(result)


white_df = load_csv("tasks/white_wine.csv")
red_df = load_json("tasks/red_wine.json")
db = connect_to_mongodb()
# db.wines.insert_many(white_df)
# db.wines.insert_many(red_df)

top_wines = sort_by_quality(db.wines)
save_json("results/task4_top_wines.json", top_wines)
filtered_wines = get_quality_above_7(db.wines)
save_json("results/task4_filtered_wines.json", filtered_wines)
sorted_by_alco = sort_by_alco(db.wines)
save_json("results/task4_sorted_by_alco.json", sorted_by_alco)
bad_red_wines = get_bad_red_wines(db.wines)
save_json("results/task4_bad_red_wines.json", bad_red_wines)
sweet_white_wines = get_sweet_white_wines(db.wines)
save_json("results/task4_sweet_white_wines.json", sweet_white_wines)

type_freq = get_freq_by_type(db.wines)
save_json("results/task4_type_freq.json", type_freq)
stat_by_quality = get_stat_by_column(db.wines, "type", "quality")
save_json("results/task4_stat_by_quality.json", stat_by_quality)
stat_by_alcohol = get_stat_by_column(db.wines, "type", "alcohol")
save_json("results/task4_stat_by_alcohol.json", stat_by_alcohol)
stat_by_sugar = get_stat_by_column(db.wines, "type", "residual sugar")
save_json("results/task4_stat_by_sugar.json", stat_by_sugar)
ph_stat = get_ph_stat_by_condition(db.wines)
save_json("results/task4_ph_stat.json", ph_stat)

increase_sulphates_by_type(db.wines)
update_fixed_acidity(db.wines)
delete_by_ph(db.wines)
increase_alco_by_density(db.wines)
delete_by_quality(db.wines)
