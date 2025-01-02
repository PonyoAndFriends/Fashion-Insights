import boto3
import logging
import requests
import os
import json
from s3_validate import connect_s3, validate_and_upload_s3_file

from datetime import datetime, timedelta

import time, random

import sys
import shutil

# s3 connect
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name='ap-northeast-2'

s3_client = connect_s3(aws_access_key_id, aws_secret_access_key, region_name)

# url
url = "https://api.musinsa.com/api2/hm/v2/pans/ranking/sections/199?"

# headers
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

params = {
    "storeCode" : "musinsa",
    "period" : "DAILY",
    "AGE_BAND" : "AGE_BAND_ALL"
}

# categoryCode
categoryCode = ["103000", "002000", "001000"] # 카테고리가 늘어남에 따라 수정 필요
today_date = datetime.now().strftime("%Y-%m-%d")

if __name__ == "__main__":
    arguments1 = sys.argv[1]
    
    params['gf'] =  arguments1
    
    for code in categoryCode:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            response_json = response.json()
            print("response 200")
        
        bucket_name = 'project4-raw-data'
        file_name = f"{today_date}/Musinsa/RankingData/{code}/musinsa_{params['gf']}_{code}.json"
        
        validate_and_upload_s3_file(s3_client, bucket_name, file_name, response_json)
        print(file_name)
