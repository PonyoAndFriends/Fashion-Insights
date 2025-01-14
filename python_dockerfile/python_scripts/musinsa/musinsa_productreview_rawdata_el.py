import argparse
import json
import re
import time
import threading
import logging
import requests
import pyarrow.fs as fs

from modules.config import Musinsa_Config

import modules.s3_module as s3_module

LIST_SIZE = 40

URL = "https://goods.musinsa.com/api2/review/v1/view/list"

PARAMS = {
    "page": 0,
    "pageSize": 30,
    "myFilter": "false",
    "hasPhoto": "false",
    "isExperience": "false",
    "sort": "new",
}

TODAY_DATE = Musinsa_Config.TODAY_DATE


def porductid_list_iterable(iterable):
    for i in range(0, len(iterable), LIST_SIZE):
        yield iterable[i : i + LIST_SIZE]


def el_productreview(s3_client, product_id_list, key):
    bronze_bucket = "team3-2-s3"
    max_retries = 3  # 최대 재시도 횟수
    retries = 0
    for product_id in product_id_list:
        s3_key = key + f"{product_id}.json"
        PARAMS["goodsNo"] = product_id

        while retries < max_retries:
            try:
                time.sleep(1.5)
                response = requests.get(
                    URL, headers=Musinsa_Config.HEADERS, params=PARAMS
                )

                if response.status_code == 403:
                    raise ValueError("HTTP error: 403 Forbidden")
                elif response.status_code != 200:
                    raise ValueError(f"HTTP error: {response.status_code}")

                data = response.json()["data"]
                print(product_id, data)

                if data["total"] != 0:
                    s3_module.upload_json_to_s3(s3_client, bronze_bucket, s3_key, data)
                    break
                elif data["total"] == 0:
                    break

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logging.error(f"Error with product_id {product_id}: {e}")
                retries += 1
                if retries < max_retries:
                    logging.info(
                        f"Retrying product_id {product_id} (Attempt {retries})"
                    )
                    time.sleep(120)  # 재시도 전 대기
                else:
                    logging.error(
                        f"Skipping product_id {product_id} after {max_retries} attempts."
                    )
                    break  # 최대 재시도 횟수 초과 시 루프 종료

            except Exception as e:
                logging.error(f"Unexpected error with product_id {product_id}: {e}")
                break  # 예기치 못한 에러 시 종료


def main():
    # argument
    parser = argparse.ArgumentParser(description="category2depth/category3depth")
    parser.add_argument("category_3_depth", type=str, help="sexual")
    parser.add_argument("category_4_depth_list", type=str, help="category")

    args = parser.parse_args()

    category3depth = args.category_3_depth
    category4depth_list = json.loads(args.category_4_depth_list)

    s3_client = s3_module.connect_s3()

    # product_id list 불러오기
    silver_bucket = "team3-2-s3"
    file_key = f"/silver/{TODAY_DATE}/musinsa/ranking_tb/{category3depth}/"

    try:
        s3 = s3_module.connect_s3fs()
        base_path = silver_bucket + file_key
        files = s3.get_file_info(fs.FileSelector(base_dir=base_path, recursive=True))

    except:
        raise Exception("S3 connection failed, causing container to crash.")

    # request
    for category4depth in category4depth_list:
        print(f"Category : {category3depth}_{category4depth} Crawler Start")
        directory_pattern = re.compile(rf".*{category4depth}\.parquet$")
        directories = [
            file
            for file in files
            if file.type == fs.FileType.Directory and directory_pattern.match(file.path)
        ]

        product_ids = []

        for directory in directories:
            temp_ids = s3_module.get_product_ids(directory.path)
            product_ids += temp_ids

        for product_list in porductid_list_iterable(product_ids):
            key = f"bronze/{TODAY_DATE}/musinsa/product_review_data/{category3depth}/{category4depth}/"
            t = threading.Thread(
                target=el_productreview, args=(s3_client, product_list, key)
            )
            t.start()


if __name__ == "__main__":
    main()
