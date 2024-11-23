import json
from datetime import datetime, date

import duckdb
import pandas as pd
import requests
import time




today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # Consolidation logic for Paris Bicycle data
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]]

    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")


def consolidate_city_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)
    city_data_df.drop_duplicates(inplace = True)

    city_data_df["created_date"] = date.today()
    print(city_data_df)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

## Paris
def consolidate_station_statement_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # Consolidate station statement data for Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = datetime.fromisoformat('2024-10-21')
    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]]

    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")

#################################################################################

##NANTES


def consolidate_nantes_station_data():
    # Connect to the DuckDB database
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Load the Nantes data from the JSON file
    today_date = date.today().strftime("%Y-%m-%d")
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)


    # Normalize JSON data into a Pandas DataFrame
    nantes_raw_data_df = pd.json_normalize(data)

    # Add or modify columns to match the CONSOLIDATE_STATION table structure
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()
    nantes_raw_data_df["city_code"] = 44000  # Set this to actual city code if needed


    # Select and rename columns to match the schema in the CONSOLIDATE_STATION table
    nantes_station_data_df = nantes_raw_data_df[[
        "id", "number", "name", "contract_name", "city_code", "address",
        "position.lon", "position.lat", "status", "created_date", "bike_stands"
    ]].copy()

    # Rename columns to match the schema
    nantes_station_data_df.rename(columns={
        "number": "code",
        "contract_name": "city_name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity"
    }, inplace=True)

    # Ensure columns are in the correct order
    expected_columns = [
        "id", "code", "name", "city_name", "city_code", "address", "longitude",
        "latitude", "status", "created_date", "capacity"
    ]
    nantes_station_data_df = nantes_station_data_df[expected_columns].copy()

    # Standardize the 'status' column: change 'OPEN' to 'OUI' and others to 'NON'
    nantes_station_data_df["status"] = nantes_station_data_df["status"].apply(lambda x: "OUI" if x == "OPEN" else "NON")

    # Insert data into the DuckDB table
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_data_df;")
    print("Nantes station data has been consolidated successfully.")


#################################################################################

##COMMUNES

def consolidate_communes_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # Charger les données JSON des communes
    with open(f"data/raw_data/{today_date}/communes_realtime_data.json") as fd:
        data = json.load(fd)
    
    # Convertir en DataFrame
    communes_df = pd.json_normalize(data)
    
    # Ajouter des colonnes nécessaires et ajuster les noms
    communes_df["created_date"] = datetime.now().date()
    communes_df.rename(columns={
        "nom": "name",
        "code": "id",
        "codeDepartement": "department_code",
        "codeRegion": "region_code",
        "codesPostaux": "postal_codes",
        "population": "population"
    }, inplace=True)

    # Colonnes finales à insérer dans CONSOLIDATE_CITY
    communes_df = communes_df[[
        "id", "name", "department_code", "region_code", "postal_codes", "population", "created_date"
    ]]

    # Insérer les données dans la table DuckDB
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM communes_df;")
    print("Communes data has been consolidated successfully.")
