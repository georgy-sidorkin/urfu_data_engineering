from bs4 import BeautifulSoup
import pandas as pd
import csv

items = []

with open("tasks/text_5_var_80", encoding="utf-8") as f:
    lines = f.readlines()
    html = ''
    for line in lines:
        html += line

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    for row in rows[1:]:
        cells = row.find_all("td")
        item = {
            "company": cells[0].text,
            "contact": cells[1].text,
            "country": cells[2].text,
            "price": cells[3].text,
            "item": cells[4].text,
        }
        items.append(item)

df = pd.DataFrame(items)
df.to_csv("results/text_5.csv", sep=",", index=False)
