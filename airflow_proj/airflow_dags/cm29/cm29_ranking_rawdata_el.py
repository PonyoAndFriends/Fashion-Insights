import os
from airflow.models import Variable
import json
import requests
import logging
from datetime import datetime, timedelta
import boto3
from cm29.cm29_mapping_table import SHOES_CATEGORIES, CATEGORY_TREE

# 로그 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# API URL 및 헤더 설정
API_URL = "https://search-api.29cm.co.kr/api/v4/plp/category"
HEADERS = {
    "Authorization": f"Bearer {Variable.get('29cm_token')}",  # Bearer 토큰 임시로 비워둠..
    "Content-Type": "application/json",
}

# AWS 자격 증명 설정
AWS_ACCESS_KEY = Variable.get("aws_access_key_id")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key")

# S3 설정
S3_BUCKET_NAME = Variable.get("s3_bucket")
PLATFORM_FOLDER_PATH = "29cm"
PRODUCT_FOLDER_PATH = "29cm_product"
REVIEW_FOLDER_PATH = "29cm_reviews"

# boto3 클라이언트 설정
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="ap-northeast-2",  # 서울 리전
)


def extract_product_data(product, rank):
    """랭킹 상품 데이터 추출"""
    categories = product.get("frontCategoryInfo", [])

    category_large_set = set(
        [
            category.get("categoryLargeName", "")
            for category in categories
            if category.get("categoryLargeName")
        ]
    )
    gender = (
        "M/W"
        if "남성의류" in category_large_set and "여성의류" in category_large_set
        else (
            "M"
            if "남성의류" in category_large_set
            else "W" if "여성의류" in category_large_set else None
        )
    )
    category_medium_names = list(
        set(
            [
                category.get("categoryMediumName", "")
                for category in categories
                if category.get("categoryMediumName")
                and category.get("categoryMediumName") != "EXCLUSIVE"
            ]
        )
    )
    category_small_names = list(
        set(
            [
                category.get("categorySmallName", "")
                for category in categories
                if category.get("categorySmallName")
                and category.get("categorySmallName") not in ["상의", "하의", "아우터"]
            ]
        )
    )
    created_at = (datetime.now() - timedelta(hours=9)).strftime("%Y-%m-%d")
    collection_platform = "29CM"

    img_url = "https://img.29cm.co.kr" + product["imageUrl"]

    return {
        "ranking": rank,
        "product_id": product["itemNo"],
        "product_name": product["itemName"],
        "frontBrandNo": product["frontBrandNo"],
        "brand_name_kr": product["frontBrandNameKor"],
        "brand_name_en": product["frontBrandNameEng"],
        "original_price": product["consumerPrice"],
        "final_price": product["lastSalePrice"],
        "discount_ratio": product["lastSalePercent"],
        "review_counting": product["reviewCount"],
        "review_avg_rating": product["reviewAveragePoint"],
        "like_counting": product["heartCount"],
        "gender": gender,
        "categoryMediumNames": category_medium_names,
        "categorySmallNames": category_small_names,
        "platform": collection_platform,
        "created_at": created_at,
        "img_url": img_url,
        "soldout_status": product["isSoldOut"],
        "isNew": product["isNew"],
    }


def fetch_and_save_data_to_s3(large_id, medium_id, small_id, s3_path, gender_folder):
    """API를 통해 데이터를 가져와 S3에 저장"""
    payload = {
        "facetGroupInput": {
            "categoryFacetInputs": [
                {
                    "largeId": large_id,
                    "middleId": medium_id,
                    "smallId": small_id,
                }
            ],
            "sortFacetInput": {"type": "RECOMMEND", "order": "DESC"},
        },
        "pagination": {"page": 0, "size": 10},
    }

    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload)
        if response.status_code != 200:
            logger.error(
                f"Failed to fetch data for {s3_path}. Status code: {response.status_code}"
            )
            return []

        data = response.json()
        if not data.get("data") or not data["data"].get("products"):
            logger.info(f"No products found for {s3_path}. Skipping...")
            return []

        # 상품 데이터 처리 및 rank 추가
        processed_data = [
            extract_product_data(product, rank)
            for rank, product in enumerate(data["data"]["products"], start=1)
        ]

        # 오늘 날짜 폴더 경로 추가
        today = (datetime.now() - timedelta(hours=9)).strftime("%Y-%m-%d")

        # 상품 데이터 저장 경로
        if any(category in s3_path for category in SHOES_CATEGORIES):
            product_data_key = f"bronze/{today}/{PLATFORM_FOLDER_PATH}/{PRODUCT_FOLDER_PATH}/{gender_folder}/Shoes/{s3_path.split('/')[-1]}.json"
        else:
            product_data_key = f"bronze/{today}/{PLATFORM_FOLDER_PATH}/{PRODUCT_FOLDER_PATH}/{gender_folder}/{s3_path}.json"

        # 상품 ID 저장 경로
        if any(category in s3_path for category in SHOES_CATEGORIES):
            product_id_key = f"bronze/{today}/{PLATFORM_FOLDER_PATH}/{REVIEW_FOLDER_PATH}/{gender_folder}/Shoes/{s3_path.split('/')[-1]}_ids.json"
        else:
            product_id_key = f"bronze/{today}/{PLATFORM_FOLDER_PATH}/{REVIEW_FOLDER_PATH}/{gender_folder}/{s3_path}_ids.json"

        # S3에 상품 데이터 저장
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=product_data_key,
            Body=json.dumps(processed_data, ensure_ascii=False, indent=4),
            ContentType="application/json",
        )
        logger.info(f"Product data saved to S3: {product_data_key}")

        # S3에 상품 ID 저장
        product_ids = [product["product_id"] for product in processed_data]
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=product_id_key,
            Body=json.dumps(product_ids, ensure_ascii=False, indent=4),
            ContentType="application/json",
        )
        logger.info(f"Product IDs saved to S3: {product_id_key}")

        return product_ids

    except Exception as e:
        logger.error(f"Exception occurred while processing {s3_path}: {e}")
        return []


def process_category_data():
    """카테고리 데이터 처리"""
    for large_category, category_info in CATEGORY_TREE.items():
        large_id = category_info["large_id"]
        gender_folder = "Woman" if "Woman" in large_category else "Man"

        for medium_category, medium_info in category_info["subcategories"].items():
            # Shoes 카테고리인지 확인하여 처리
            if medium_category in SHOES_CATEGORIES:
                s3_path = f"Shoes/{medium_category}"
                logger.info(f"Processing: {gender_folder} > {s3_path}")
                fetch_and_save_data_to_s3(
                    large_id, medium_info, None, s3_path, gender_folder
                )

            # 일반 의류 데이터 처리
            elif isinstance(medium_info, dict) and "subcategories" in medium_info:
                medium_id = medium_info["large_id"]
                for small_category, small_id in medium_info["subcategories"].items():
                    s3_path = f"{medium_category}/{small_category}"
                    logger.info(f"Processing: {gender_folder} > {s3_path}")
                    fetch_and_save_data_to_s3(
                        large_id, medium_id, small_id, s3_path, gender_folder
                    )

            elif isinstance(medium_info, str):  # 중분류가 문자열인 경우
                s3_path = medium_category
                logger.info(f"Processing: {gender_folder} > {s3_path}")
                fetch_and_save_data_to_s3(
                    large_id, medium_info, None, s3_path, gender_folder
                )
