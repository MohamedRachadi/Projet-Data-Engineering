import json
from datetime import datetime, date
import numpy as np
import duckdb
import pandas as pd
import requests
import time
import os



# Création des tables consolidées à partir des instructions SQL
today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3

# Consolidation des données des stations pour Paris
def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


## Paris
def consolidate_station_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    # Ajout de colonnes et renommage pour correspondre au schéma cible
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
    # Insertion des données consolidées dans la table cible
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")

# Consolidation des données des villes pour Paris
def paris_consolidate_city_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    file_path = f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json"
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    try:
        with open(file_path, "r") as fd:
            data = json.load(fd) 
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {file_path}: {e}")
        return

    raw_data_df = pd.json_normalize(data)
    # Ajout de colonnes supplémentaires et déduplication
    raw_data_df["nb_inhabitants"] = None
    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    city_data_df = city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    })
    city_data_df = city_data_df.drop_duplicates()
    city_data_df.loc[:, "created_date"] = date.today()
    # Insertion des données consolidées dans la table cible
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

# Consolidation des données de déclaration des stations pour Paris
def paris_consolidate_station_statement_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    # Consolidate station statement data for Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    # Ajout de colonnes supplémentaires et déduplication
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] =date.today()
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
    # Insertion des données consolidées dans la table cible
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")

#################################################################################

##NANTES

# Consolidation des données des villes pour Nantes
def nantes_consolidate_city_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    # Ajout de colonnes supplémentaires et déduplication
    city_data_df = pd.DataFrame({
        "city_code": 1,
        "name": ["Nantes"],
        "nb_inhabitants": [np.nan]
    })
    
    city_data_df = city_data_df.rename(columns={
        "city_code": "id",
        "name": "name"
    })
    city_data_df.loc[:, "created_date"] = date.today()
    # Insertion des données consolidées dans la table cible
    con.execute("""INSERT OR REPLACE INTO CONSOLIDATE_CITY 
                SELECT * FROM city_data_df;""")



def consolidate_nantes_station_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    today_date = date.today().strftime("%Y-%m-%d")
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    nantes_raw_data_df = pd.json_normalize(data)
    # Ajout de colonnes supplémentaires et déduplication
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()
    nantes_raw_data_df["city_code"] = 1
    nantes_station_data_df = nantes_raw_data_df[[
        "id", "number", "name", "contract_name", "city_code", "address",
        "position.lon", "position.lat", "status", "created_date", "bike_stands"
    ]].copy()
    nantes_station_data_df.rename(columns={
        "number": "code",
        "contract_name": "city_name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity"
    }, inplace=True)
    expected_columns = [
        "id", "code", "name", "city_name", "city_code", "address", "longitude",
        "latitude", "status", "created_date", "capacity"
    ]
    nantes_station_data_df = nantes_station_data_df[expected_columns].copy()
    nantes_station_data_df["status"] = nantes_station_data_df["status"].apply(lambda x: "OUI" if x == "OPEN" else "NON")
    # Insertion des données consolidées dans la table cible
    con.execute("""INSERT OR REPLACE INTO CONSOLIDATE_STATION 
                SELECT * FROM nantes_station_data_df;""")


# Consolidation des données de déclaration des stations pour Nantes
def nantes_consolidate_station_statement_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    nantes_raw_data_df = pd.json_normalize(data)
    # Ajout de colonnes supplémentaires et déduplication
    nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()
    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)
    # Insertion des données consolidées dans la table cible
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM nantes_station_statement_data_df;")

#################################################################################

##TOULOUSE

# Consolidation des données des villes pour Nantes
def toulouse_consolidate_city_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    city_data_df = pd.DataFrame({
        "city_code": 3,
        "name": ["toulouse"],
        "nb_inhabitants": [np.nan]
    })
    city_data_df = city_data_df.rename(columns={
        "city_code": "id",
        "name": "name"
    })
    city_data_df.loc[:, "created_date"] = date.today()
    # Insertion des données consolidées dans la table cible
    con.execute("""INSERT OR REPLACE INTO CONSOLIDATE_CITY 
                SELECT * FROM city_data_df;""")


def consolidate_toulouse_station_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    today_date = date.today().strftime("%Y-%m-%d")
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    toulouse_raw_data_df = pd.json_normalize(data)
    # Ajout de colonnes supplémentaires et déduplication
    toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = date.today()
    toulouse_raw_data_df["city_code"] = 3  # np.nan Set this to actual city code if needed
    toulouse_station_data_df = toulouse_raw_data_df[[
        "id", "number", "name", "contract_name", "city_code", "address",
        "position.lon", "position.lat", "status", "created_date", "bike_stands"
    ]].copy()
    toulouse_station_data_df.rename(columns={
        "number": "code",
        "contract_name": "city_name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity"
    }, inplace=True)
    expected_columns = [
        "id", "code", "name", "city_name", "city_code", "address", "longitude",
        "latitude", "status", "created_date", "capacity"
    ]
    toulouse_station_data_df = toulouse_station_data_df[expected_columns].copy()
    toulouse_station_data_df["status"] = toulouse_station_data_df["status"].apply(lambda x: "OUI" if x == "OPEN" else "NON")
    # Insertion des données consolidées dans la table cible
    con.execute("""INSERT OR REPLACE INTO CONSOLIDATE_STATION 
                SELECT * FROM toulouse_station_data_df;""")

# Consolidation des données de déclaration des stations pour Nantes
def toulouse_consolidate_station_statement_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    toulouse_raw_data_df = pd.json_normalize(data)
    # Ajout de colonnes supplémentaires et déduplication
    toulouse_raw_data_df["station_id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = date.today()
    toulouse_station_statement_data_df = toulouse_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    toulouse_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)
    # Insertion des données consolidées dans la table cible
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM toulouse_station_statement_data_df;")


#################################################################################


##COMMUNES

def consolidate_communes_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    today_date = datetime.now().strftime("%Y-%m-%d")
    with open(f"data/raw_data/{today_date}/communes_realtime_data.json") as fd:
        data = json.load(fd)
    if isinstance(data, list):
        communes_df = pd.json_normalize(data)
        # Ajouter une colonne de date de création
        communes_df["created_date"] = datetime.now().date()
        # Renommer les colonnes pour correspondre au schéma de CONSOLIDATE_COMMUNES
        communes_df.rename(columns={
            "nom": "name",
            "code": "id",
            "codeDepartement": "department_code",
            "codeRegion": "region_code",
            "codesPostaux": "postal_codes",
            "population": "population"
        }, inplace=True)
        # Si `postal_codes` est une liste, vous pouvez la convertir en chaîne de caractères
        communes_df["postal_codes"] = communes_df["postal_codes"].apply(lambda x: ",".join(x) if isinstance(x, list) else x)
        # Filtrer uniquement les colonnes nécessaires
        communes_df = communes_df[[
            "id", "name", "department_code", "region_code", "postal_codes", "population", "created_date"
        ]]
        # Charger les données dans DuckDB
        con.execute("CREATE OR REPLACE TEMPORARY TABLE tmp_communes AS SELECT * FROM communes_df;")
        # Insérer ou remplacer dans la table finale CONSOLIDATE_COMMUNES
        con.execute("""
            INSERT OR REPLACE INTO CONSOLIDATE_COMMUNES
            SELECT * FROM tmp_communes
        """)
    else:
        print("Erreur : les données chargées ne sont pas sous forme de liste.")


def update_consolidate_station():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    # Requête SQL pour mettre à jour le champ CITY_CODE dans la table CONSOLIDATE_STATION
    con.execute("""
        UPDATE CONSOLIDATE_STATION
        SET CITY_CODE = (
            SELECT id
            FROM CONSOLIDATE_COMMUNES AS COMMUNES
            WHERE lower(COMMUNES.name) = lower(CONSOLIDATE_STATION.CITY_NAME)
            AND COMMUNES.CREATED_DATE = (
                SELECT MAX(CREATED_DATE)
                FROM CONSOLIDATE_COMMUNES
                WHERE lower(name) = lower(CONSOLIDATE_STATION.CITY_NAME)
            )
        )
        WHERE lower(CITY_NAME) IN ('nantes', 'toulouse');
    """)

def update_consolidate_city():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    # Requête SQL pour mettre à jour le champ ID dans la table CONSOLIDATE_CITY
    con.execute("""
        UPDATE CONSOLIDATE_CITY
            SET ID = (
                SELECT COMMUNES.id
                FROM CONSOLIDATE_COMMUNES AS COMMUNES
                WHERE lower(COMMUNES.name) = lower(CONSOLIDATE_CITY.NAME)
                AND COMMUNES.CREATED_DATE = (
                    SELECT MAX(CREATED_DATE)
                    FROM CONSOLIDATE_COMMUNES
                    WHERE lower(name) = lower(CONSOLIDATE_CITY.NAME)
                )
                LIMIT 1
            )
            WHERE lower(NAME) IN ('nantes', 'toulouse')
            AND NOT EXISTS (
                SELECT 1
                FROM CONSOLIDATE_CITY AS existing
                WHERE existing.ID = (
                    SELECT COMMUNES.id
                    FROM CONSOLIDATE_COMMUNES AS COMMUNES
                    WHERE lower(COMMUNES.name) = lower(CONSOLIDATE_CITY.NAME)
                    AND COMMUNES.CREATED_DATE = (
                        SELECT MAX(CREATED_DATE)
                        FROM CONSOLIDATE_COMMUNES
                        WHERE lower(name) = lower(CONSOLIDATE_CITY.NAME)
                    )
                    LIMIT 1
                )
                AND existing.CREATED_DATE = CONSOLIDATE_CITY.CREATED_DATE
            );
    """)