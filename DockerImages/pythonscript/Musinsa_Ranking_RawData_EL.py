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

import argparse

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

# params
params = {
    "storeCode" : "musinsa",
    "period" : "DAILY",
}

# categoryCode
categoryCode = ["103000", "002000", "001000"] # 카테고리가 늘어남에 따라 수정 필요

# today_date
today_date = datetime.now().strftime("%Y-%m-%d")

def main():
    parser = argparse.ArgumentParser(description="SEXUAL / AGEBAND")
    
    parser.add_argument('gf', type=str, help='parameter : SEXUAL')
    parser.add_argument('ageBand', type=str, help='parameter : age_band')
    
    args = parser.parse_args()
    
    params['gf'] =  args.gf
    params['ageBand'] = args.ageBand
    
    for code in categoryCode:
        params['categoryCode'] = code
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            response_json = response.json()
            logging.info("response 200")
        
        bucket_name = 'source-bucket-hs'
        file_name = f"{today_date}/Musinsa/RankingData/{code}/musinsa_{params['gf']}_{params['ageBand']}_{code}.json"

        validate_and_upload_s3_file(s3_client, bucket_name, file_name, response_json)
    
if __name__ == "__main__":
    main()
