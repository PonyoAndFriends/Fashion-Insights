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
}

SORT = ["goods_est_desc", "goods_est_asc"]

TODAY_DATE = Musinsa_Config.TODAY_DATE


def porductid_list_iterable(iterable):
    for i in range(0, len(iterable), LIST_SIZE):
        yield iterable[i : i + LIST_SIZE]


def el_productreview(s3_client, product_id_list, key):
    bronze_bucket = "team3-2-s3"
    for sort_method in SORT:
        PARAMS["sort"] = sort_method
        time.sleep(120)
        for product_id in product_id_list:
            s3_key = key + f"{product_id}{sort_method}.json"
            PARAMS["goodsNo"] = product_id
            try:
                response = requests.get(
                    URL, headers=Musinsa_Config.HEADERS, params=PARAMS
                )
                time.sleep(1.5)
                data = response.json()["data"]
                print(f"res.json(): {response.json()}")
                if data["total"] != 0:
                    s3_module.upload_json_to_s3(s3_client, bronze_bucket, s3_key, data)
            except Exception as e:
                logging.error(f"Error: {e}")
                continue


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
