import argparse
import json
import re
import random
import time
import threading

import requests
import pyarrow.fs as fs

from modules.config import Musinsa_Config

import modules.s3_module as s3_module

LIST_SIZE = 10

URL = "https://goods.musinsa.com/api2/review/v1/view/list"

PARAMS = {
    "page" : 0,
    "pageSize" : 30,
    "myFilter" : "false",
    "hasPhoto" : "false",
    "isExperience" : "false"
}

SORT = ['goods_est_desc', 'goods_est_asc']

TODAY_DATE = Musinsa_Config.TODAY_DATE

def porductid_list_iterable(iterable):
    for i in range(0, len(iterable), LIST_SIZE):
        yield iterable[i:i + LIST_SIZE]

def el_productreview(product_id_list,key):
    bronze_bucket = "project4-raw-data"
    for sort_method in SORT:
        PARAMS["sort"] = sort_method
        for product_id in product_id_list:
            s3_key = key + f"{product_id}_{sort_method}.json"
            PARAMS["goodsNo"] = product_id
            time.sleep(0.8)
            response = requests.get(URL, headers=Musinsa_Config.HEADERS, params=PARAMS)
            data = response.json()['data']
            s3_module.upload_json_to_s3(bronze_bucket, s3_key, data)
        
def main():
    # argument
    parser = argparse.ArgumentParser(description="category2depth/category3depth")
    parser.add_argument('category3depth', type=str, help='sexual')
    parser.add_argument('category4depth_list', type=str, help='category')
    
    args = parser.parse_args()
    
    category3depth = args.category3depth
    category4depth_list = json.loads(args.category4depth_list)
    
    # product_id list 불러오기
    silver_bucket = "project4-silver-data/"
    file_key = f"{TODAY_DATE}/Musinsa/RankingData/{category3depth}/"
    
    s3 = s3_module.connect_s3fs()
    
    base_path = silver_bucket + file_key
    
    files = s3.get_file_info(fs.FileSelector(base_dir=base_path, recursive=True))
    
    # request
    for category4depth in category4depth_list:
        print(f"Category : {category3depth}_{category4depth} Crawler Start")
        directory_pattern = re.compile(rf'.*{category4depth}\.parquet$')
        directories = [file for file in files if file.type == fs.FileType.Directory and directory_pattern.match(file.path)]
        
        product_ids = []

        for directory in directories:
            temp_ids = s3_module.get_product_ids(directory.path)
            product_ids += temp_ids
        
        for product_list in porductid_list_iterable(product_ids):
            key = f"{TODAY_DATE}/Musinsa/ProductReviewData/{category3depth}/{category4depth}/"        
            t = threading.Thread(target=el_productreview, args=(product_list, key))
            t.start()
            
if __name__ == "__main__":
    main()
    