import logging
import requests
import json

import argparse

from pythonscript.modules.s3_module import connect_s3, validate_and_upload_s3_file
from modules.config import Musinsa_Config

# url
url = "https://api.musinsa.com/api2/hm/v2/pans/ranking/sections/199?"

# params
params = {
    "storeCode" : "musinsa",
    "period" : "DAILY",
    "ageBand" : "AGE_BAND_ALL"
}

TODAY_DATE = Musinsa_Config.TODAY_DATE

# mapping table - 2depth 카테고리를 이용해 airflow에서 task를 분기 중
# task 명은 영어로 만들어야 함. 하지만 파일 적재는 한국어로 통일해서 적재 계획 (4depth가 한국어)
# => 2depth 카테고리를 한국어로 바꿔야 함
def mapping_2depth_kor(depth2category):
    if depth2category == 'top':
        return '상의'
    elif depth2category == 'bottom':
        return '하의'
    elif depth2category == 'shoes':
        return '신발'
    elif depth2category == 'outer':
        return '아우터'
    
def main():
    parser = argparse.ArgumentParser(description="sexual/category param")
    parser.add_argument('sexual', type=str, help='sexual')
    parser.add_argument('category_data', type=str, help='category')

    args = parser.parse_args()

    sexual_data = json.loads(args.sexual)
    category_data = json.loads(args.category_data)

    params['gf'] = sexual_data[0]

    category2depth = mapping_2depth_kor(category_data[0])
    
    logging.info(f"Category : {sexual_data[0]}_{category_data[0]} Crawler Start")
   
    for category_info in category_data[1]:
        category3depth = list(category_info.items())[0]
        
        for category4code, category4name in category3depth[1].items():
            params['categoryCode'] = category4code
            response = requests.get(url, headers=Musinsa_Config.HEADERS, params=params)
            
            if response.status_code == 200:
                response_json = response.json()
                logging.info("response 200")
            
            bucket_name = 'project4-raw-data'
            file_name = f"{today_date}/Musinsa/RankingData/{category3depth[0]}/{sexual_data[1]}_{category2depth}_{category3depth[0]}_{category4name}.json"
    
            validate_and_upload_s3_file(connect_s3(), bucket_name, file_name, response_json)
  
if __name__ == "__main__":
    main()
    