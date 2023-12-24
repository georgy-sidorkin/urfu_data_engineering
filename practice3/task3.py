from bs4 import BeautifulSoup
import numpy as np
import json
import warnings
warnings.filterwarnings("ignore")


def handle_file(file_name):
    with open(file_name, encoding="utf-8") as file:
        text = ""
        for row in file.readlines():
            text += row

    star = BeautifulSoup(text, 'lxml').star
    item = dict()
    item["name"] = star.find_all("name")[0].get_text().strip()
    item["constellation"] = star.find_all("constellation")[0].get_text().strip()
    item["spectral-class"] = star.find_all("spectral-class")[0].get_text().strip()
    item["radius"] = int(star.find_all("radius")[0].get_text().strip())
    item["rotation"] = star.find_all("rotation")[0].get_text().strip()
    item["age"] = star.find_all("age")[0].get_text().strip()
    item["distance"] = star.find_all("distance")[0].get_text().strip()
    item["absolute-magnitude"] = star.find_all("absolute-magnitude")[0].get_text().strip()

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
        if col in item:
            frequency[item[col]] = frequency.get(item[col], 0) + 1

    return frequency

items = []
for i in range(1, 501):
    file_name = f"tasks/task_3_var_80/{i}.xml"
    items.append(handle_file(file_name))

items = sorted(items, key=lambda x: int(x['radius']), reverse=True)

with open("results/task3_all.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(items))

filtered_items = []
for item in items:
    if item['radius'] >= 900000000:
        filtered_items.append(item)

with open("results/task3_filtered_min_radius_9e8.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_items, ensure_ascii=False))

radius_stats = calc_stats("radius", items)
with open("results/task3_radius_stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(radius_stats, ensure_ascii=False))

constellation_freq = calc_frequency("constellation", items)
with open("results/task3_constellation_freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(constellation_freq, ensure_ascii=False))
