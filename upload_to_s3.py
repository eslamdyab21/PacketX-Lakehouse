import boto3
from dotenv import load_dotenv
import os

def upload_file_to_s3(file_path, bucket_name, object_key):

    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id = os.getenv('aws_access_key_id'),
        aws_secret_access_key = os.getenv('aws_secret_access_key')
    )

    try:
        s3_client.upload_file(file_path, bucket_name, object_key)
        print(f"File '{file_path}' uploaded to S3 bucket '{bucket_name}' as '{object_key}'")

    except:
        print(f"Error Uploading")


if __name__ == "__main__":
    load_dotenv()

    local_file_path = "/home/dyab/projects/PacketX/traffic_log/2025-03-18.csv"
    s3_bucket_name = "log-storage-bucket-v1"
    s3_object_key = "lakehouse/raw_data_upload/" + local_file_path.split('/')[-1]

    
    upload_file_to_s3(local_file_path, s3_bucket_name, s3_object_key)