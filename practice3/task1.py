from bs4 import BeautifulSoup
import re
import json
import numpy as np


def parse_file(f_name):
    with open(f_name, encoding="utf-8") as f:
        text = ""
        for line in f.readlines():
            text += line

        site = BeautifulSoup(text, "html.parser")

        item = dict()
        item["type"] = site.find_all("span", string=re.compile("Тип:"))[0].get_text().replace("Тип:", "").strip()
        item["title"] = site.find_all("h1")[0].get_text().replace("Турнир:", "").strip()

        address = site.find_all("p", attrs={"class": "address-p"})[0].get_text().split("Начало:")
        item["city"] = address[0].split()[-1].strip()
        item["date_start"] = address[1].strip()

        item["count"] = int(site.find_all("span", attrs={"class": "count"})[0]
                            .get_text().split("Количество туров:")[-1].strip())
        item["time_control"] = site.find_all("span", attrs={"class": "year"})[0] \
            .get_text().split("Контроль времени:")[-1].strip()
        item["min_rating"] = int(site.find_all("span", string=re.compile("Минимальный рейтинг для участия:"))[0]
                                 .get_text().split("Минимальный рейтинг для участия:")[-1].strip())
        item["img"] = site.find_all("img")[0]["src"]
        item["rating"] = float(site.find_all("span", string=re.compile("Рейтинг:"))[0]
                               .get_text().split("Рейтинг:")[-1].strip())
        item["views"] = int(site.find_all("span", string=re.compile("Просмотры:"))[0]
                            .get_text().split("Просмотры:")[-1].strip())
    return item


def calc_stats(col, items_lst):
    numbers = list(map(lambda x: x[col], items_lst))
    numeric_stats = {'sum': sum(numbers),
                     'avg': round(np.average(numbers), 2),
                     'min': min(numbers),
                     'max': max(numbers),
                     'std': round(np.std(numbers), 2),
                     }

    return numeric_stats


def calc_frequency(col, items_lst):
    frequency = {}

    for item in items_lst:
        frequency[item[col]] = frequency.get(item[col], 0) + 1

    return frequency


items = []
for i in range(1, 1000):
    file_name = f"tasks/task_1_var_80/{i}.html"
    result = parse_file(file_name)
    items.append(result)

items = sorted(items, key=lambda x: x["views"], reverse=True)

with open("results/task1_all.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(items, ensure_ascii=False))

filtered_items = []
for tour in items:
    if tour["min_rating"] >= 2500:
        filtered_items.append(tour)

with open("results/task1_filtered_min_rating_2500.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_items, ensure_ascii=False))

views_stats = calc_stats("views", items)
with open("results/task1_views_stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(views_stats, ensure_ascii=False))

city_freq = calc_frequency("city", items)
with open("results/task1_city_freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(city_freq, ensure_ascii=False))
