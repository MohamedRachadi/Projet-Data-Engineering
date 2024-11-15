import duckdb
import os

## testtt
def create_agregate_tables():
    db_path = "data/duckdb/mobility_analysis.duckdb"
    db_dir = os.path.dirname(db_path)

    # Ensure the directory exists
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    con = duckdb.connect(database=db_path, read_only=False)

    sql_file_path = "C:/Users/HP/Downloads/polytech-de-101-2024-tp-subject-main/polytech-de-101-2024-tp-subject-main/data/sql_statements/create_agregate_tables.sql"

    # Check if the file exists and is readable
    if not os.path.isfile(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path, "r") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            if statement.strip():
                print(statement)
                con.execute(statement)


def agregate_dim_station():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT 
        ID,
        CODE,
        NAME,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITTY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql_statement)


def agregate_dim_city():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)


def agregate_fact_station_statements():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # First we agregate the Paris station statement data
    sql_statement = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
    SELECT STATION_ID, cc.ID as CITY_ID, BICYCLE_DOCKS_AVAILABLE, BICYCLE_AVAILABLE, LAST_STATEMENT_DATE, current_date as CREATED_DATE
    FROM CONSOLIDATE_STATION_STATEMENT
    JOIN CONSOLIDATE_STATION ON CONSOLIDATE_STATION.ID = CONSOLIDATE_STATION_STATEMENT.STATION_ID
    LEFT JOIN CONSOLIDATE_CITY as cc ON cc.ID = CONSOLIDATE_STATION.CITY_CODE
    WHERE CITY_CODE != 0 
        AND CONSOLIDATE_STATION_STATEMENT.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION_STATEMENT)
        AND CONSOLIDATE_STATION.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
        AND cc.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)
