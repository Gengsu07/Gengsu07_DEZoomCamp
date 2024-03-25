import datetime
import math

import pandas as pd
import requests

url = "https://api.rawg.io/api/games"
api_key = "4ce3012ed2024e39861c8d0811fe28d3"
params = {"key": f"{api_key}", "page_size": 100}

current_date = datetime.datetime.now().date()
current_year = datetime.datetime.now().year
first_date_this_year = datetime.datetime(current_year, 1, 1).date()
params["dates"] = f"{first_date_this_year},{current_date}"


def parse_json(response):
    data = response.json()
    result = data["results"]
    df = pd.json_normalize(result)
    return df


response = requests.get(url=url, params=params)
response.raise_for_status()
data = response.json()
count = data["count"]
print(count)
page_amount = math.ceil(count / 40)
for page in range(1, page_amount)[:1]:
    params["page"] = page
    response = requests.get(url=url, params=params)
    df = parse_json(response)
    print(df.head())
    df.to_excel("rawg_games.xlsx", index=False)
