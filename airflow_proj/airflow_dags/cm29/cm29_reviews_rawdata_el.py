import os
import json
import requests
import logging
import boto3
from airflow.models import Variable
from datetime import datetime, timedelta

# 로그 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# S3 설정
S3_BUCKET_NAME = Variable.get("s3_bucket")
AWS_ACCESS_KEY = Variable.get("aws_access_key_id")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="ap-northeast-2",
)

# 리뷰 API 설정
REVIEW_API_URL = "https://review-api.29cm.co.kr/api/v4/reviews"
HEADERS = {
    "Authorization": f"Bearer {Variable.get('29cm_token')}",  # Bearer 토큰 임시로 비워둠..
    "Content-Type": "application/json",
}


def list_files_in_s3(folder_path):
    """S3에서 소분류 카테고리 폴더 경로의 모든 JSON 파일 목록을 반환"""
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=folder_path)
        if "Contents" in response:
            return [
                obj["Key"]
                for obj in response["Contents"]
                if obj["Key"].endswith("_ids.json")
            ]
        return []
    except Exception as e:
        logger.error(f"Error listing files in S3: {e}")
        return []


def read_ids_json_from_s3(file_key):
    """S3에서 product_id가 있는 JSON 파일 읽기"""
    logger.info(f"Reading S3 file: {file_key}")

    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        data = response["Body"].read().decode("utf-8")
        logger.info(f"Successfully read file: {file_key}")
        return json.loads(data)
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"File not found in S3: {file_key}")
        return []


def save_reviews_to_s3(folder_path, file_name, reviews):
    """리뷰 데이터를 S3에 저장"""
    s3_key = os.path.join(folder_path, f"{file_name}.json")
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(reviews, ensure_ascii=False, indent=4),
            ContentType="application/json",
        )
        logger.info(f"Reviews saved to S3: {s3_key}")
    except Exception as e:
        logger.error(f"Error while saving reviews to S3: {e}")


def fetch_reviews_for_product(product_id, created_at):
    """특정 상품 ID의 최대 50개의 리뷰 데이터 가져오기"""
    all_reviews = []
    page_size = 20
    page = 0
    max_reviews = 20  # 최대 가져올 리뷰 수

    while len(all_reviews) < max_reviews:
        url = f"{REVIEW_API_URL}?itemId={product_id}&page={page}&size={page_size}&sort=RECENT"

        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            logger.error(
                f"Failed to fetch reviews for product ID {product_id}, page {page}. Status code: {response.status_code}"
            )
            break

        try:
            review_data = response.json()
            reviews = review_data.get("data", {}).get("results", [])
            logger.info(
                f"{len(reviews)} reviews fetched for product ID {product_id}, page {page}."
            )

            if not reviews:
                logger.info(f"No more reviews available for product ID {product_id}.")
                break

            # 리뷰 데이터에 product_id와 created_at 추가
            for review in reviews:
                review["product_id"] = product_id
                review["created_at"] = created_at

            all_reviews.extend(reviews)
            if len(reviews) < page_size:
                break

            page += 1

        except ValueError:
            logger.error(
                f"Invalid JSON response for product ID {product_id}, page {page}."
            )
            break

    return all_reviews[:max_reviews]  # 최대 리뷰 수 제한


def fetch_and_save_reviews_from_all_files(file_key):
    """S3의 특정 소분류 IDs 파일을 읽고 리뷰 데이터를 가져와 저장"""
    product_ids = read_ids_json_from_s3(file_key)
    if not product_ids:
        logger.error(f"No product IDs found in {file_key}. Skipping...")
        return

    category_reviews = []
    now = datetime.now()
    created_at = (now + timedelta(hours=9)).strftime("%Y-%m-%d")  # created_at 값 추가

    for product_id in product_ids:
        reviews = fetch_reviews_for_product(product_id, created_at)  # created_at 전달
        category_reviews.extend(reviews)

    if category_reviews:
        category_path = os.path.dirname(file_key)
        file_name = os.path.basename(file_key).replace("_ids.json", "_reviews")
        save_reviews_to_s3(category_path, file_name, category_reviews)
        logger.info(f"Reviews saved for file: {file_key}")
