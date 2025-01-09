import boto3
from botocore.exceptions import ClientError
import os

import json
import logging

import pyarrow.parquet as pq
import pyarrow.fs as fs

from modules.config import AWS_Config

# connection to s3
def connect_s3():
    s3_client = boto3.client(
        's3', 
        aws_access_key_id=AWS_Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_Config.AWS_SECRET_ACCESS_KEY,
        region_name=AWS_Config.REGION
    )
    return s3_client

def connect_s3fs():
    s3 = fs.S3FileSystem(
        access_key=AWS_Config.AWS_ACCESS_KEY_ID,
        secret_key=AWS_Config.AWS_SECRET_ACCESS_KEY,
        region=AWS_Config.REGION,
        endpoint_override='https://s3.ap-northeast-2.amazonaws.com'  
    )
    return s3

# check if file exists in s3
def check_file_exists(bucket_name, s3_key):
    s3_client = connect_s3()
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logging.error(e)
            return False

# upload json to s3
def upload_json_to_s3(bucket_name, s3_key, json_data):
    s3_client = connect_s3()
    try:
        json_string = json.dumps(json_data)
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json_string)
        logging.info(f"File {s3_key} uploaded to {bucket_name}.")
    except ClientError as e:
        logging.error(f"Error uploading file: {e}")

# validate s3 file and delete if exists
def validate_and_upload_s3_file(bucket_name, s3_key, json_data):
    s3_client = connect_s3()
    if check_file_exists(bucket_name, s3_key):
        logging.info(f'{s3_key} already exists in {bucket_name}')
        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        logging.info(f"File {s3_key} deleted successfully.")
        upload_json_to_s3(bucket_name, s3_key, json_data)
        logging.info(f"File {s3_key} uploaded successfully.")
    else:
        logging.info(f'{s3_key} does not exist in {bucket_name}')
        upload_json_to_s3(bucket_name, s3_key, json_data)
        logging.info(f"File {s3_key} uploaded successfully.")

# product_id 추출
def get_product_ids(s3_path):
    dataset = pq.ParquetDataset(s3_path, filesystem=connect_s3fs())
    table = dataset.read(columns=['product_id'])
    
    product_ids = table['product_id'].to_pylist()
    
    return product_ids
