import os
from datetime import datetime
import json
import requests


def get_paris_realtime_bicycle_data():
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"

    response = requests.request("GET", url)

    serialize_data(response.text, "paris_realtime_bicycle_data.json")



def get_nantes_realtime_bicycle_data():
    # Replace this with the actual Nantes API URL
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    response = requests.request("GET", url)
    if response.status_code == 200:
        serialize_data(response.text, "nantes_realtime_bicycle_data.json")
    else:
        print(f"Failed to fetch data for Nantes. Status code: {response.status_code}")


def get_toulouse_realtime_bicycle_data():
    # Replace this with the actual Nantes API URL
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json"
    response = requests.request("GET", url)
    if response.status_code == 200:
        serialize_data(response.text, "toulouse_realtime_bicycle_data.json")
    else:
        print(f"Failed to fetch data for Toulouse. Status code: {response.status_code}")

def get_communes_realtime_data():
    url = "https://geo.api.gouv.fr/communes"
    response = requests.get(url)
    if response.status_code == 200:
        # Convertir la réponse JSON en chaîne de caractères
        json_data = json.dumps(response.json(), ensure_ascii=False, indent=4)
        serialize_data(json_data, "communes_realtime_data.json")
        print("Données des communes récupérées et sauvegardées.")
    else:
        print(f"Failed to fetch data for Communes. Status code: {response.status_code}")


def serialize_data(raw_json: str, file_name: str):
    today_date = datetime.now().strftime("%Y-%m-%d")
    folder_path = f"data/raw_data/{today_date}"
    
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    try:
        # Validate JSON before saving
        parsed_json = json.loads(raw_json)  # Parse to ensure valid JSON
        with open(f"{folder_path}/{file_name}", "w") as fd:
            json.dump(parsed_json, fd, indent=4)  # Write formatted JSON
    except json.JSONDecodeError as e:
        print(f"Error in JSON structure: {e}")


# def serialize_data(raw_json: str, file_name: str):
#     today_date = datetime.now().strftime("%Y-%m-%d")

#     if not os.path.exists(f"data/raw_data/{today_date}"):
#         os.makedirs(f"data/raw_data/{today_date}")

#     with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
#         fd.write(raw_json)



