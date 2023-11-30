from bs4 import BeautifulSoup
import numpy as np
import json


def handle_file(file_name):
    items = list()

    with open(file_name, encoding="utf-8") as file:
        text = ""
        for row in file.readlines():
            text += row

        site = BeautifulSoup(text, 'html.parser')
        products = site.find_all("div", attrs={'class': 'product-item'})

        for product in products:
            item = dict()
            item['id'] = product.a['data-id']
            item['link'] = product.find_all('a')[1]['href']
            item['img_url'] = product.find_all('img')[0]['src']
            item['title'] = product.find_all('span')[0].get_text().strip()
            item['price'] = int(product.price.get_text().replace("₽", "").replace(" ", "").strip())
            item['bonus'] = int(product.strong.get_text().replace("+ начислим ", "").replace(" бонусов", "").strip())
            props = product.ul.find_all("li")
            for prop in props:
                item[prop['type']] = prop.get_text().strip()

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
for i in range(1, 29):
    file_name = f"tasks/task_2_var_80/{i}.html"
    items += handle_file(file_name)

items = sorted(items, key=lambda x: x['price'], reverse=True)

with open("results/task2_all.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(items))

filtered_items = []
for item in items:
    if item['bonus'] >= 2000:
        filtered_items.append(item)

with open("results/task2_filtered_min_bonus_2000.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_items))

print(calc_stats("price", items))
print(calc_frequency("ram", items))
