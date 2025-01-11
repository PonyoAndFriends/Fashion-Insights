import boto3
import json
from ably_modules.aws_info import AWS_S3_CONFIG

def list_folders(bucket_name, base_path):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_S3_CONFIG.get("aws_access_key_id"),
        aws_secret_access_key=AWS_S3_CONFIG.get("aws_secret_access_key")
    )
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_path, Delimiter='/')
    return [content['Prefix'] for content in response.get('CommonPrefixes', [])]

bucket_name = "{bucket_name}"
base_path = "{base_path}"
folders = list_folders(bucket_name, base_path)

with open('/airflow/xcom/return.json', 'w') as f:
    json.dump({"folders": folders}, f)