from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
import pyarrow.compute as pc
from pathlib import Path
import configparser
from utils import *
import logging
import duckdb
import os


def date_dim_etl(con, sql_base_path, filter_date):
    logging.info(f"""date_dim_etl""")

    sql_query = Path(sql_base_path + '/date_dim_etl.sql').read_text()
    con.execute(sql_query, [filter_date])

    logging.info(f"""date_dim_etl -> Done""")



def aggregate_by_hour(table):
    logging.info(f"""aggregate_by_hour""")

    # Truncate timestamp to hour
    time_hour = pc.floor_temporal(table["time_stamp"], unit="hour")
    table = table.append_column("time_hour", time_hour)


    # Group by time_hour, source_ip, destination_ip and sum bandwidth_kb
    result = table.group_by(keys=["time_hour", "user","source_ip", "destination_ip"]).\
                            aggregate([("bandwidth_kb", "sum")])


    logging.info(f"""aggregate_by_hour -> Done""")
    return result



def connect_to_postgres_wh():
    logging.info(f"""connect_to_postgres_wh""")

    con = duckdb.connect()  
    con.sql(f"""
        INSTALL postgres;
        LOAD postgres;  
        ATTACH '  
            dbname={os.getenv("POSTGRES_NAME")}
            hostaddr={os.getenv("POSTGRES_HOST")}  
            port={os.getenv("POSTGRES_PORT")}  
            user={os.getenv("POSTGRES_USER")}  
            password={os.getenv("POSTGRES_PASSWORD")}  
        ' AS db (TYPE postgres);  
    """)


    logging.info(f"""connect_to_postgres_wh -> Done""")
    return con



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    logging.info(f"""Main""")

    # ----- Load .env and conf -----
    logging.info(f"""Main -> Load Conf and Envs""")
    load_dotenv()
    config = configparser.ConfigParser()
    config.read_file(open(r'conf'))
    local_or_aws       = config.get('Warehouse ETL', 'local_or_aws')
    filter_date        = config.get('Warehouse ETL', 'filter_date')
    sql_base_path      = config.get('Warehouse ETL', 'sql_base_path')
    logging.info(f"""Main -> Load Conf and Envs -> Done""")
    # ----- Load .env and conf -----


    if local_or_aws == 'aws':
        # aws
        pass
    else:
        # ----- SQL Lite Local Path -----
        catalog = load_local_sqlite_catalog()
        iceberg_table = catalog.load_table("PacketX_Raw.Packets")
        con = connect_to_postgres_wh()

        start_time = f"{filter_date}T00:00:00"
        end_time   = f"{filter_date}T23:59:59"
        filtered_day_table = iceberg_table.scan(row_filter=f"time_stamp >= '{start_time}' AND time_stamp <= '{end_time}'")
        filtered_day_table = aggregate_by_hour(filtered_day_table.to_arrow())

        con.register("lakehouse_packets", filtered_day_table)

        date_dim_etl(con, sql_base_path, filter_date)
        con.close()
        # ----- SQL Lite Local Path -----