import duckdb

con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

# Check consolidated communes
result = con.execute("SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE FROM DIM_CITY dm INNER JOIN (SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE FROM FACT_STATION_STATEMENT WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION) GROUP BY CITY_ID) tmp ON dm.ID = tmp.CITY_ID WHERE lower(dm.NAME) IN ('paris', 'nantes', 'vincennes', 'toulouse');").fetchdf()
print(result.head())


result = con.execute("SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available FROM DIM_STATION ds JOIN ( SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available FROM FACT_STATION_STATEMENT GROUP BY station_id) AS tmp ON ds.id = tmp.station_id WHERE ds.ID like '2-%';").fetchdf()
print(result.head())

"""
result = con.execute("SELECT * FROM CONSOLIDATE_STATION_STATEMENT where STATION_ID like '3-%';").fetchdf()
print(result.head())

result = con.execute("SELECT * FROM CONSOLIDATE_STATION where ID like '3-%';").fetchdf()
print(result.head())

result = con.execute("SELECT * FROM CONSOLIDATE_STATION where lower(CITY_NAME) like 'toulouse';").fetchdf()
print(result.head())

result = con.execute("SELECT * FROM CONSOLIDATE_COMMUNES where lower(name) = 'toulouse';").fetchdf()
print(result.head())

result = con.execute(\"""SELECT ID, CREATED_DATE, COUNT(*)
FROM CONSOLIDATE_CITY
GROUP BY ID, CREATED_DATE
HAVING COUNT(*) > 1;
\""").fetchdf()
print(result.head())

result = con.execute("SELECT * FROM FACT_STATION_STATEMENT --where ID = '94069';").fetchdf()
print(result.head())



try:
    print("Connexion réussie à la base de données.")

    # Récupérer toutes les tables existantes
    tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';"
    tables = con.execute(tables_query).fetchall()

    if not tables:
        print("La base de données est déjà vide.")
    else:
        # Supprimer chaque table
        for (table_name,) in tables:
            drop_query = f"DROP TABLE IF EXISTS {table_name};"
            con.execute(drop_query)
            print(f"Table '{table_name}' supprimée.")

        print("Toutes les tables ont été supprimées avec succès.")

except Exception as e:
    print(f"Erreur lors de la suppression des tables : {e}")

finally:
    # Fermer la connexion
    con.close()
    print("Connexion à la base de données fermée.")
"""
