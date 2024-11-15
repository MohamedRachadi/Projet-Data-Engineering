import requests
import os
from datetime import datetime
import json

def get_nantes_realtime_bicycle_data():
    # Replace this with the actual Nantes API URL
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/records?limit=20"
    response = requests.get(url)
    if response.status_code == 200:
        serialize_data(response.text, "nantes_realtime_bicycle_data.json")
    else:
        print(f"Failed to fetch data for Nantes. Status code: {response.status_code}")

def serialize_data(raw_json: str, file_name: str):
    today_date = datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)

if __name__ == "__main__":
    get_nantes_realtime_bicycle_data()
