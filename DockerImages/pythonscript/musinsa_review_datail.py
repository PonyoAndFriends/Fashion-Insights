import requests
import json
import os
import logging  # 추후 로깅을 추가하십시오
import argparse

from s3_validate import connect_s3, validate_and_upload_s3_file
from datetime import datetime

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = "ap-northeast-2"

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

base_url = f"https://goods.musinsa.com/api2/review/v1/view/list"

today_date = datetime.now().strftime("%Y-%m-%d")


def main():
    # argparse를 활용하여 argument를 가져옴.
    parser = argparse.ArgumentParser()
    parser.add_argument("--gender", type=str, required=True)
    parser.add_argument("--category", type=str, required=True)
    parser.add_argument("--product_ids", type=str, required=True)

    args = parser.parse_args()

    # product_ids 문자열을 리스트로 변환
    product_ids = json.loads(args.product_ids)
    s3_client = connect_s3(aws_access_key_id, aws_secret_access_key, region_name)

    for product_id in product_ids:
        params = {
            "page": 0,
            "pageSize": 10,
            "goodsNo": f"{product_id}",
            "sort": "new",
            "selectedSimilarNo": "1551840",
            "myFilter": "false",
            "hasPhoto": "false",
            "isExperience": "false",
        }
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()

        bucket_name = "source-bucket-hs"
        file_name = f"{today_date}/Musinsa/review_detail/{args.gender}/{args.category}/{agrs.gender}_{args.category}_review_detail.json"

        validate_and_upload_s3_file(s3_client, bucket_name, file_name, response.json())


if __name__ == "__main__":
    main()
