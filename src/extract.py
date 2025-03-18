import pandas as pd
import requests
import os
import pathlib
url = "https://opensky-network.org/api/states/all"
response = requests.get(url)
data = response.json()
data_short = list()
for row in data["states"]:
    data_short.append(row[:7])

df = pd.DataFrame(data_short,
                  columns=["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude",
                           "latitude"])

data_dir = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), "data")
if not os.path.exists(data_dir):
    os.mkdir(data_dir)
df.to_csv(os.path.join(data_dir, "flights.csv"), index=False)
