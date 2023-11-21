import pandas as pd
import json
import os
import msgpack
import pickle

df = pd.read_csv("tasks/lego.csv", sep=";")

df = df[["year", "Theme name", "Part category", "Part color", "Set Price", "Number of reviews", "Star rating"]]

df["Set Price"] = df["Set Price"].transform(lambda x: float(x.replace(",", ".")))
df["Star rating"] = df["Star rating"].transform(lambda x: float(x.replace(",", ".")))
df["Number of reviews"] = df["Number of reviews"].transform(lambda x: int(float(x.replace(",", "."))))

numeric_columns = df.select_dtypes(include='number').columns
numeric_stats = df[numeric_columns].describe().to_dict()

obj_dict = dict()

obj_columns = df.select_dtypes(include='object').columns

for col in obj_columns:
    obj_dict[col] = df[col].value_counts().to_dict()

res = numeric_stats | obj_dict

with open("results/lego.json", "w") as json_output_file:
    json_output_file.write(json.dumps(res))

df.to_json('results/lego.json', orient='records')

with open('results/lego.msgpack', 'wb') as f:
    f.write(msgpack.packb(df.to_dict(orient='records')))

with open("results/lego.pkl", "wb") as f:
    f.write(pickle.dumps(df))


print('Size of data.csv:', os.path.getsize('tasks/lego.csv'), 'bytes')
print('Size of data.json:', os.path.getsize('results/lego.json'), 'bytes')
print('Size of data.msgpack:', os.path.getsize('results/lego.msgpack'), 'bytes')
print('Size of data.pkl:', os.path.getsize('results/lego.pkl'), 'bytes')
