from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, FloatType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.compute as pc
import pyarrow.dataset as ds
import logging
import configparser
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from datetime import date
import boto3
import os



def aggregate_bandwidth_by_user(df, date):
    logging.info("aggregate_bandwidth_by_user")

    grouped = df.group_by('user').aggregate([
        ('bandwidth_kb', 'sum')
    ])
    
    result_dict = grouped.rename_columns(['user', 'total_bandwidth_kb']).to_pydict()

    result_list = []
    for user, bandwidth in zip(result_dict['user'], result_dict['total_bandwidth_kb']):
        result_list.append(
            {
                'user': user,
                'total_bandwidth_kb': bandwidth,
                'date': date
            }
        )
   
    
    print(result_list)
    logging.info("aggregate_bandwidth_by_user -> Done")
    return result_list



def load_local_sqlite_catalog():
    logging.info(f"""load_local_sqlite_catalog""")


    catalog = load_catalog('lakehouse', **{
        'uri': 'sqlite:///iceberg_catalog/catalog.db',
        'warehouse': 'file://iceberg_catalog'
    })
    os.environ['PYICEBERG_HOME'] = os.getcwd()


    logging.info(f"""load_local_sqlite_catalog -> Done""")
    return catalog



def load_s3_glue_catalog(s3_lakehouse_path, region_name):
    logging.info("load_s3_glue_catalog")

    s3_lakehouse_bucket = f"s3://{os.getenv('s3_bucket_name')}/{s3_lakehouse_path}"
    
    catalog = load_catalog(
        'glue_lakehouse',
        **{
            "type": 'glue',
            "s3.region": region_name,
            "s3.access-key-id": os.getenv('aws_access_key_id'),
            "s3.secret-access-key": os.getenv('aws_secret_access_key'),
            "region_name": region_name,
            "glue.region": region_name,
            "glue.access-key-id": os.getenv('aws_access_key_id'),
            "glue.secret-access-key": os.getenv('aws_secret_access_key'),
            "aws_access_key_id": os.getenv('aws_access_key_id'),
            "aws_secret_access_key": os.getenv('aws_secret_access_key'),
            "warehouse": s3_lakehouse_bucket
        }
    )

    logging.info("load_s3_glue_catalog -> Done")
    return catalog



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    logging.info(f"""Main""")


    # ----- Load .env and conf -----
    load_dotenv()
    config = configparser.ConfigParser()
    config.read_file(open(r'conf'))
    s3_lakehouse_path  = config.get('Raw S3 Iceberg Lakehouse ETL', 's3_lakehouse_path')
    region_name        = config.get('Raw S3 Iceberg Lakehouse ETL', 'region_name')
    filter_date        = config.get('DynamoDB ETL', 'filter_date')
    # ----- Load .env and conf -----

    
    # ----- SQL Lite Local Path -----
    # catalog = load_local_sqlite_catalog()
    # iceberg_table = catalog.load_table("PacketX_Raw.Packets")

    # start_time = f"{filter_date}T00:00:00"
    # end_time   = f"{filter_date}T23:59:59"
    # filtered_day_table = iceberg_table.scan(row_filter=f"time_stamp >= '{start_time}' AND time_stamp <= '{end_time}'")
    # aggregate_bandwidth_by_user(filtered_day_table.to_arrow(), filter_date)
    # ----- SQL Lite Local Path -----


    # ----- Glue Catalog S3 Path -----
    catalog = load_s3_glue_catalog(s3_lakehouse_path, region_name)
    iceberg_table = catalog.load_table("PacketX_Raw.Packets")

    start_time = f"{filter_date}T00:00:00"
    end_time   = f"{filter_date}T23:59:59"
    filtered_day_table = iceberg_table.scan(row_filter=f"time_stamp >= '{start_time}' AND time_stamp <= '{end_time}'")
    aggregate_bandwidth_by_user(filtered_day_table.to_arrow(), filter_date)
    # ----- Glue Catalog S3 Path -----

    
    logging.info(f"""Main -> Done""")