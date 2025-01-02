import boto3
from botocore.exceptions import ClientError

import json

# connection to s3
def connect_s3(aws_access_key_id, aws_secret_access_key, region_name):
    s3_client = boto3.client(
        's3', 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='ap-northeast-2'
    )
    return s3_client

# check if file exists in s3
def check_file_exists(s3_client, bucket_name, s3_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            print(e)
            return False

# upload json to s3
def upload_json_to_s3(s3_client, bucket_name, s3_key, json_data):
    try:
        json_string = json.dumps(json_data)
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json_string)
        print(f"File {s3_key} uploaded to {bucket_name}.")
    except ClientError as e:
        print(f"Error uploading file: {e}")

# validate s3 file and delete if exists
def validate_and_upload_s3_file(s3_client, bucket_name, s3_key, json_data):
    if check_file_exists(s3_client, bucket_name, s3_key):
        print(f'{s3_key} already exists in {bucket_name}')
        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        print(f"File {s3_key} deleted successfully.")
        upload_json_to_s3(s3_client, bucket_name, s3_key, json_data)
        print(f"File {s3_key} uploaded successfully.")
    else:
        print(f'{s3_key} does not exist in {bucket_name}')
        upload_json_to_s3(s3_client, bucket_name, s3_key, json_data)
        print(f"File {s3_key} uploaded successfully.")