import duckdb

con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=True)
"""
# Check consolidated station data
result = con.execute("SELECT * FROM CONSOLIDATE_STATION;").fetchdf()
print(result.head())
"""

# Check consolidated city data
result = con.execute("SELECT * FROM CONSOLIDATE_STATION WHERE ID like '2-%';").fetchdf()
print(result.head())

# Check consolidated communes
result = con.execute("SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE FROM DIM_CITY dm INNER JOIN (SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE FROM FACT_STATION_STATEMENT WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION) GROUP BY CITY_ID) tmp ON dm.ID = tmp.CITY_ID WHERE lower(dm.NAME) IN ('paris', 'nantes', 'vincennes', 'toulouse');").fetchdf()
print(result.head())




