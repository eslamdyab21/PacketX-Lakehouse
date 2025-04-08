from pyiceberg.catalog import load_catalog
import logging
import os



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