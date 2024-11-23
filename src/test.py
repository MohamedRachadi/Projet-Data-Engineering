import pandas as pd
import duckdb

# Connect to the DuckDB database
con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=True)

# Fetch all data from CONSOLIDATE_STATION
result = con.execute("SELECT * FROM CONSOLIDATE_STATION_STATEMENT").fetchdf()

#SELECT * FROM CONSOLIDATE_STATION

result.to_excel("consolidate_station_full.xlsx", index=False)
print("Table exported to 'consolidate_station_full.xlsx'")

# # Print the result
# print(result)
