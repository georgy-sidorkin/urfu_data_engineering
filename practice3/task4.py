import json
from bs4 import BeautifulSoup
import numpy as np


def handle_file(file_name):
    items = list()

    with open(file_name, encoding="utf-8") as file:
        text = ""
        for row in file.readlines():
            text += row

        root = BeautifulSoup(text, 'xml')

        for clothing in root.find_all("clothing"):
            item = dict()
            for el in clothing.contents:
                if el.name is None:
                    continue
                elif el.name == "price" or el.name == "reviews":
                    item[el.name] = int(el.get_text().strip())
                elif el.name == "rating":
                    item[el.name] = float(el.get_text().strip())
                elif el.name == "new":
                    item[el.name] = el.get_text().strip() == "+"
                elif el.name == "exclusive" or el.name == "sporty":
                    item[el.name] = el.get_text().strip() == "yes"
                else:
                    item[el.name] = el.get_text().strip()

            items.append(item)

        return items


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
for i in range(1, 101):
    file_name = f"tasks/task_4_var_80/{i}.xml"
    items += handle_file(file_name)

items = sorted(items, key=lambda x: x['price'], reverse=True)

with open("results/task4_all.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(items))

filtered_items = []
for item in items:
    if item['rating'] >= 4:
        filtered_items.append(item)

with open("results/task4_filtered_min_rating_4.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_items))

print(calc_stats("price", items))
print(calc_frequency("category", items))
