import boto3
from dotenv import load_dotenv
import logging
from zipfile import ZipFile, ZIP_DEFLATED
from datetime import date
import os



def upload_file_to_s3(file_path, bucket_name, object_key):
    logging.info(f"""upload_file_to_s3""")

    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id = os.getenv('aws_access_key_id'),
        aws_secret_access_key = os.getenv('aws_secret_access_key')
    )

    logging.info(f"""upload_file_to_s3 -> connected to S3 Client""")

    try:
        s3_client.upload_file(file_path, bucket_name, object_key)
        logging.info(f"""upload_file_to_s3 -> File {file_path} uploaded to S3 bucket {bucket_name} as {object_key}""")

    except:
        logging.info(f"""upload_file_to_s3 -> Error Uploading""")
    

    logging.info(f"""upload_file_to_s3 -> Done""")



def compress_file(local_file_path):
    logging.info(f"""compress_file""")

    zip_local_file_path = local_file_path.replace('.csv', '.zip')

    with ZipFile(zip_local_file_path, 'w', ZIP_DEFLATED) as zip:
        zip.write(local_file_path)
    

    logging.info(f"""compress_file -> {zip_local_file_path} -> Done""")
    return zip_local_file_path


if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    logging.info(f"""Main""")

    load_dotenv()

    current_date = str(date.today())

    local_file_path = f"/home/dyab/projects/PacketX/traffic_log/{current_date}.csv"
    zip_local_file_path = compress_file(local_file_path)

    s3_bucket_name = "log-storage-bucket-v1"
    s3_object_key = "lakehouse/raw_data_upload/" + zip_local_file_path.split('/')[-1]

    upload_file_to_s3(zip_local_file_path, s3_bucket_name, s3_object_key)

    os.remove(zip_local_file_path)
    logging.info(f"""Main -> {zip_local_file_path} -> Removed""")
    logging.info(f"""Main -> Done""")