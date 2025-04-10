from dotenv import load_dotenv
from pathlib import Path
import configparser
import logging
import duckdb
import os



def test_wh_etl(con, sql_base_path):
    logging.info(f"""test_wh_etl""")

    sql_query = Path(sql_base_path + '/test_wh_etl.sql').read_text()
    con.sql(sql_query).show()

    logging.info(f"""test_wh_etl -> Done""")



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

    # Set default schema path to db.warehouse
    con.sql("SET search_path = db.warehouse;")


    logging.info(f"""connect_to_postgres_wh -> Done""")
    return con



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    logging.info(f"""Main""")

    # ----- Load .env and conf -----
    logging.info(f"""Main -> Load Conf & Envs""")
    load_dotenv()
    config = configparser.ConfigParser()
    config.read_file(open(r'conf'))

    sql_base_path = config.get('Warehouse ETL', 'sql_base_path')
    logging.info(f"""Main -> Load Conf & Envs""")
    # ----- Load .env and conf -----


    con = connect_to_postgres_wh()
    test_wh_etl(con, sql_base_path)