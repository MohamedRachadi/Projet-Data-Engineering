import os
from datetime import datetime
import json
import requests


# Fonction pour récupérer les données des vélos en libre-service en temps réel à Paris
def get_paris_realtime_bicycle_data():
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    response = requests.request("GET", url)
    if response.status_code == 200:
        serialize_data(response.text, "paris_realtime_bicycle_data.json")
    else:
        print(f"Failed to fetch data for Paris. Status code: {response.status_code}")


# Fonction pour récupérer les données des vélos en libre-service en temps réel à Nantes
def get_nantes_realtime_bicycle_data():
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    response = requests.request("GET", url)
    if response.status_code == 200:
        serialize_data(response.text, "nantes_realtime_bicycle_data.json")
    else:
        print(f"Failed to fetch data for Nantes. Status code: {response.status_code}")


# Fonction pour récupérer les données des vélos en libre-service en temps réel à Toulouse
def get_toulouse_realtime_bicycle_data():
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json"
    response = requests.request("GET", url)
    if response.status_code == 200:
        serialize_data(response.text, "toulouse_realtime_bicycle_data.json")
    else:
        print(f"Failed to fetch data for Toulouse. Status code: {response.status_code}")

# Fonction pour récupérer les données en temps réel des communes
def get_communes_realtime_data():
    url = "https://geo.api.gouv.fr/communes"
    response = requests.get(url)
    if response.status_code == 200:
        json_data = json.dumps(response.json(), ensure_ascii=False, indent=4)
        serialize_data(json_data, "communes_realtime_data.json")
    else:
        print(f"Failed to fetch data for Communes. Status code: {response.status_code}")


# Fonction pour sauvegarder les données dans un fichier JSON
def serialize_data(raw_json: str, file_name: str):
    today_date = datetime.now().strftime("%Y-%m-%d")
    folder_path = f"data/raw_data/{today_date}"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    try:
        parsed_json = json.loads(raw_json)  
        with open(f"{folder_path}/{file_name}", "w") as fd:
            json.dump(parsed_json, fd, indent=4)  
    except json.JSONDecodeError as e:
        print(f"Error in JSON structure: {e}")



