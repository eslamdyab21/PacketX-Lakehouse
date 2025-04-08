from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
import configparser
from utils import *
import logging
import os




if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    logging.info(f"""Main""")

    # ----- Load .env and conf -----
    logging.info(f"""Main -> Load Conf and Envs""")
    load_dotenv()
    config = configparser.ConfigParser()
    config.read_file(open(r'conf'))
    s3_lakehouse_path  = config.get('Raw S3 Iceberg Lakehouse ETL', 's3_lakehouse_path')
    region_name        = config.get('Raw S3 Iceberg Lakehouse ETL', 'region_name')
    local_or_aws       = config.get('Warehouse ETL', 'local_or_aws')
    logging.info(f"""Main -> Load Conf and Envs -> Done""")
    # ----- Load .env and conf -----


    if local_or_aws == 'aws':
        # aws
        pass
    else:
        # ----- SQL Lite Local Path -----
        catalog = load_local_sqlite_catalog()
        iceberg_table = catalog.load_table("PacketX_Raw.Packets")
        
        # ----- SQL Lite Local Path -----