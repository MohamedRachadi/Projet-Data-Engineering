import duckdb

# Connect to the DuckDB database
con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=True)

# Run the DESCRIBE query
result = con.execute("select * from CONSOLIDATE_STATION").fetchdf()
print(result)
