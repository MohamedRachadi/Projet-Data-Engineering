import duckdb

con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=True)
""""
# Check consolidated station data
result = con.execute("SELECT * FROM CONSOLIDATE_STATION where lower(CITY_NAME) like '%paris%';").fetchdf()
print(result.head())

# Check consolidated city data
result = con.execute("SELECT * FROM CONSOLIDATE_CITY where lower(NAME) like '%nantes%';").fetchdf()
print(result.head())
"""
# Check consolidated communes
result = con.execute("SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE FROM DIM_CITY dm INNER JOIN (SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE FROM FACT_STATION_STATEMENT WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION) GROUP BY CITY_ID) tmp ON dm.ID = tmp.CITY_ID WHERE lower(dm.NAME) IN ('paris', 'nantes', 'vincennes', 'toulouse');").fetchdf()
print(result.head())


result = con.execute("SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available FROM DIM_STATION ds JOIN ( SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available FROM FACT_STATION_STATEMENT GROUP BY station_id) AS tmp ON ds.id = tmp.station_id WHERE ds.ID like '2-%';").fetchdf()
print(result.head())


result = con.execute("SELECT * FROM CONSOLIDATE_STATION_STATEMENT where STATION_ID like '2-%';").fetchdf()
print(result.head())

result = con.execute("SELECT * FROM CONSOLIDATE_STATION where ID like '2-%';").fetchdf()
print(result.head())

result = con.execute("SELECT * FROM DIM_CITY where ID=44109;").fetchdf()
print(result.head())

result = con.execute("SELECT * FROM FACT_STATION_STATEMENT where STATION_ID like '2-%';").fetchdf()
print(result.head())

result = con.execute("SELECT * FROM CONSOLIDATE_STATION where lower(CITY_NAME) like 'nantes';").fetchdf()
print(result.head())

