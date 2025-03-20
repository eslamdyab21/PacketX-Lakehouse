import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import logging
import gzip
import shutil
from datetime import date
import configparser
import os



def upload_file_to_s3(file_path, object_key):
    logging.info(f"""upload_file_to_s3""")

    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id = os.getenv('aws_access_key_id'),
        aws_secret_access_key = os.getenv('aws_secret_access_key')
    )
    bucket_name = os.getenv('s3_bucket_name')

    logging.info(f"""upload_file_to_s3 -> connected to S3 Client""")

    try:
        s3_client.upload_file(file_path, bucket_name, object_key)
        logging.info(f"""upload_file_to_s3 -> File <{file_path}> uploaded to S3 bucket <{bucket_name}> as <{object_key}>""")

    except ClientError as e:
        logging.error(f"Error uploading file to S3: {e}")
        raise
    
    except FileNotFoundError:
        logging.error(f"Local file {local_file_path} not found")
        raise

    

    logging.info(f"""upload_file_to_s3 -> Done""")



def compress_file(local_file_path):
    logging.info(f"""compress_file""")

    gz_local_file_path = local_file_path.replace('.csv', '.gz')

    with open(local_file_path, "rb") as f_in:
        with gzip.open(gz_local_file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    

    logging.info(f"""compress_file -> {gz_local_file_path} -> Done""")
    return gz_local_file_path


if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    logging.info(f"""Main""")

    # Load .env and conf
    load_dotenv()
    config = configparser.ConfigParser()
    config.read_file(open(r'conf'))
    local_csv_dir_path = config.get('Upload To S3', 'local_csv_dir_path')
    s3_object_key_path = config.get('Upload To S3', 's3_object_key_path')


    current_date = str(date.today())

    local_file_path = local_csv_dir_path + current_date + '.csv'

    gz_local_file_path = compress_file(local_file_path)

    
    s3_object_key = s3_object_key_path + gz_local_file_path.split('/')[-1]

    upload_file_to_s3(gz_local_file_path, s3_object_key)

    os.remove(gz_local_file_path)
    logging.info(f"""Main -> {gz_local_file_path} -> Removed""")
    logging.info(f"""Main -> Done""")