import boto3, json, sys
from datetime import datetime, timedelta
from ably_modules.aws_info import *
from ably_modules.ably_dependencies import *

args = sys.argv
AWS_ACCESS_KEY = args[0]
AWS_SECRET_KEY = args[1]
BUCKET_NAME = args[2]
REGION = args[3]

AWS_S3_CONFIG = {
    "aws_access_key_id": AWS_ACCESS_KEY,
    "aws_secret_access_key": AWS_SECRET_KEY,
    "region_name": REGION,
    "bucket_name": BUCKET_NAME,
}

bucket_name = DEFAULT_S3_DICT["bucket_name"]
base_path = f"{(datetime.now() + timedelta(hours=9)).strftime('%Y-%m-%d')}/review_data/"


def list_folders(bucket_name, base_path):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_S3_CONFIG.get("aws_access_key_id"),
        aws_secret_access_key=AWS_S3_CONFIG.get("aws_secret_access_key"),
    )
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=base_path, Delimiter="/"
    )
    return [content["Prefix"] for content in response.get("CommonPrefixes", [])]


folders = list_folders(bucket_name, base_path)

with open("/opt/app/folders.json", "w") as f:
    json.dump({"folders": folders}, f)


def get_product_ids(bucket_name, folder):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_S3_CONFIG.get("aws_access_key_id"),
        aws_secret_access_key=AWS_S3_CONFIG.get("aws_secret_access_key"),
    )
    key = f"{folder}goods_sno_list.json"
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    return next(iter(data.items()))


product_ids_map = {}
for folder in folders:
    category_key, product_ids = get_product_ids(bucket_name, folder)
    product_ids_map[folder] = {"category_key": category_key, "product_ids": product_ids}

with open("/airflow/xcom/return.json", "w") as f:
    json.dump(product_ids_map, f)


def get_product_ids(bucket_name, folder):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_S3_CONFIG.get("aws_access_key_id"),
        aws_secret_access_key=AWS_S3_CONFIG.get("aws_secret_access_key"),
    )
    key = f"{folder}goods_sno_list.json"
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    return next(iter(data.items()))


bucket_name = DEFAULT_S3_DICT["bucket_name"]
base_path = f"{datetime.now().strftime('%Y-%m-%d')}/review_data/"

product_ids_map = {}
for folder in folders:
    category_key, product_ids = get_product_ids(bucket_name, folder)
    product_ids_map[folder] = {"category_key": category_key, "product_ids": product_ids}

with open("/airflow/xcom/return.json", "w") as f:
    json.dump(product_ids_map, f)
