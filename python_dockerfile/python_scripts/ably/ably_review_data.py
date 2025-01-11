import boto3, json, requests, logging, os
from datetime import datetime, timedelta
from ably_modules.aws_info import *
from ably_modules.ably_dependencies import *


AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "team3-2-s3"
REGION = "ap-northeast-2"

AWS_S3_CONFIG = {
    "aws_access_key_id": AWS_ACCESS_KEY,
    "aws_secret_access_key": AWS_SECRET_KEY,
    "region_name": REGION,
    "bucket_name": BUCKET_NAME,
}

logging.basicConfig(level=logging.INFO)


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


def fetch_reviews(api_url, headers, product_id, max_reviews=20, retries=3):
    reviews = []
    url = f"{api_url}/{product_id}/reviews/"
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json().get("reviews", [])
                reviews.extend(data)
                if len(reviews) >= max_reviews:
                    return reviews[:max_reviews]
                break
        except Exception as e:
            logging.error(f"Error fetching reviews for {product_id}: {e}")
    return reviews


def save_reviews(s3_client, bucket_name, folder, category_key, product_id, reviews):
    key = f"{folder}reviews_{category_key}_{product_id}.json"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(reviews, ensure_ascii=False, indent=4),
        ContentType="application/json",
    )
    logging.info(f"Saved reviews for {product_id} to {key}")


if __name__ == "__main__":
    bucket_name = DEFAULT_S3_DICT["bucket_name"]
    base_path = (
        f"{(datetime.now() + timedelta(hours=9)).strftime('%Y-%m-%d')}/review_data/"
    )
    api_url = "https://api.a-bly.com/webview/goods"
    headers = ABLY_HEADER

    aws_access_key_id = AWS_ACCESS_KEY
    aws_secret_access_key = AWS_SECRET_KEY
    aws_region = REGION

    folders = list_folders(bucket_name, base_path)

    with open("/app/python_scripts/folders.json", "w") as f:
        json.dump({"folders": folders}, f)

    product_ids_map = {}
    for folder in folders:
        category_key, product_ids = get_product_ids(bucket_name, folder)
        product_ids_map[folder] = {
            "category_key": category_key,
            "product_ids": product_ids,
        }

    with open("/app/python_scripts/product_ids.json", "w") as f:
        json.dump(product_ids_map, f)

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region,
    )

    for folder, data in product_ids_map.items():
        category_key = data["category_key"]
        product_ids = data["product_ids"]
        for product_id in product_ids:
            reviews = fetch_reviews(api_url, headers, product_id)
            if reviews:
                save_reviews(
                    s3_client, bucket_name, folder, category_key, product_id, reviews
                )
