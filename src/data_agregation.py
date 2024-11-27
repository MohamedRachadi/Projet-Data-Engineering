import duckdb


def create_agregate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


def agregate_dim_station():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

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
        CC.ID,
        CC.NAME,
        MAX(DC.population) AS NB_INHABITANTS
    FROM CONSOLIDATE_CITY AS CC
    LEFT JOIN CONSOLIDATE_COMMUNES AS DC ON CC.ID = DC.id
    WHERE CC.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY)
    GROUP BY CC.ID, CC.NAME
    ;
    """

    con.execute(sql_statement)

def aggregate_dim_commune():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    sql_statement = """
    INSERT OR REPLACE INTO DIM_COMMUNE
    SELECT 
        id AS ID,
        name AS NAME,
        department_code AS DEPARTMENT_CODE,
        region_code AS REGION_CODE,
        postal_codes AS POSTAL_CODES,
        population AS POPULATION,
        created_date AS CREATED_DATE
    FROM CONSOLIDATE_COMMUNES
    WHERE created_date = (SELECT MAX(created_date) FROM CONSOLIDATE_COMMUNES);
    """

    con.execute(sql_statement)
    print("DIM_COMMUNE table aggregated successfully.")



def agregate_fact_station_statements():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

#      First we agregate the Paris station statement data
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


