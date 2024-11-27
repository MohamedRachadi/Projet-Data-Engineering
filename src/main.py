import warnings
warnings.filterwarnings("ignore")

from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements,
    aggregate_dim_commune 
)

from data_consolidation import (
    create_consolidate_tables,
    paris_consolidate_city_data,
    nantes_consolidate_city_data,
    consolidate_station_data,
    paris_consolidate_station_statement_data,
    nantes_consolidate_station_statement_data,
    consolidate_nantes_station_data,
    toulouse_consolidate_station_statement_data,
    consolidate_toulouse_station_data,
    consolidate_communes_data,
    update_consolidate_station,
    toulouse_consolidate_city_data,
    update_consolidate_city
)

from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
    get_toulouse_realtime_bicycle_data,
    get_communes_realtime_data

)

def main():
    print("Process start.")
    # data ingestion
    print("\n----------------Data ingestion started----------------\n")
    get_paris_realtime_bicycle_data()
    get_nantes_realtime_bicycle_data()
    get_toulouse_realtime_bicycle_data()
    get_communes_realtime_data()
    print("\n----------------Data ingestion ended----------------\n")

    # data consolidation
    print("\n----------------Consolidation data started----------------\n")
    create_consolidate_tables()
    paris_consolidate_city_data()
    nantes_consolidate_city_data()
    toulouse_consolidate_city_data()
    consolidate_station_data()
    paris_consolidate_station_statement_data()
    nantes_consolidate_station_statement_data()
    consolidate_nantes_station_data()
    toulouse_consolidate_station_statement_data()
    consolidate_toulouse_station_data()
    consolidate_communes_data()
    update_consolidate_station()
    update_consolidate_city()
    print("\n----------------Consolidation data ended----------------\n")

    # data agregation
    print("\n----------------Agregate data started----------------\n")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statements()
    aggregate_dim_commune()
    print("\n----------------Agregate data ended----------------\n")


if __name__ == "__main__":
    main()




