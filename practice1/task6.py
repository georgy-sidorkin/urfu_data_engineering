import json
import requests
from bs4 import BeautifulSoup


url = "http://api.coincap.io/v2/assets"
params = {"limit": 10}

res = requests.get(url=url, params=params)

data = json.loads(res.text)

soup = BeautifulSoup("""<table>
    <tr>
        <th>id</th>
        <th>rank</th>
        <th>symbol</th>
        <th>name</th>
        <th>supply</th>
        <th>maxSupply</th>
        <th>marketCapUsd</th>
        <th>volumeUsd24Hr</th>
        <th>priceUsd</th>
        <th>changePercent24Hr</th>
        <th>vwap24Hr</th>
        <th>explorer</th>
    </tr>
</table>""", "html.parser")

table = soup.contents[0]

for tick in data["data"]:
    tr = soup.new_tag("tr")
    for key, val in tick.items():
        td = soup.new_tag("td")
        if val is None:
            td.string = ""
        else:
            td.string = val
        tr.append(td)
    table.append(tr)

with open("results/text_6.html", "w") as result:
    result.write(soup.prettify())
    result.write("\n")
