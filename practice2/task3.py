import json
import msgpack
import os

with open("tasks/products_80.json") as f:
    data = json.load(f)

products = dict()

for item in data:
    if item['name'] in products:
        products[item['name']].append(item['price'])
    else:
        products[item['name']] = []
        products[item['name']].append(item['price'])

res = []

for name, prices in products.items():
    sum_price = 0
    max_price = prices[0]
    min_price = prices[0]
    size = len(prices)
    for price in prices:
        sum_price += price
        max_price = max(max_price, price)
        min_price = min(min_price, price)

    res.append({
        "name": name,
        "max": max_price,
        "min": min_price,
        "avg": sum_price / size,
    })

with open("results/products_res.json", "w") as f_json:
    f_json.write(json.dumps(res))

with open("results/products_res.msgpack", "wb") as f_msg:
    f_msg.write(msgpack.dumps(res))

print(f"json    = {os.path.getsize('results/products_res.json')}")
print(f"msgpack = {os.path.getsize('results/products_res.msgpack')}")
