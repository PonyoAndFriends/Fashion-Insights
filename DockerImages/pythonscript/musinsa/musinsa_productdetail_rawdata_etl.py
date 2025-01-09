import time
import argparse
import requests
import re
from datetime import datetime, timedelta
import json
import threading

from bs4 import BeautifulSoup

from modules.config import Musinsa_Config
import modules.s3_module as s3_module

TODAY_DATE = Musinsa_Config.TODAY_DATE

LIST_SIZE = 10

def porductid_list_iterable(iterable):
    for i in range(0, len(iterable), LIST_SIZE):
        yield iterable[i:i + LIST_SIZE]

def mapping_2depth_kor(depth2category):
    if depth2category == 'top':
        return '상의'
    elif depth2category == 'bottom':
        return '하의'
    elif depth2category == 'shoes':
        return '신발'
    elif depth2category == 'outer':
        return '아우터'

# parsing 안되는 경우
def get_text_or_none(element):
    return element.text if element else None

def get_content_or_none(element):
    return element['content'] if element else None

def et_product1_detail(product_id):
    url = f"https://www.musinsa.com/products/{product_id}"
    
    response = requests.get(url, headers=Musinsa_Config.HEADERS)
    
    soup = BeautifulSoup(response.text, features="html.parser")

    title_text = soup.find('title').text
    product_name = re.sub(r' - 사이즈 & 후기.*', '', title_text)

    # brand naeme_kr, brand name_en
    brand_name_kr = soup.find('meta', {'property': 'product:brand'}).get('content')
    brand_name_en = soup.find('div', class_='sc-11x022e-0 hzZrPp')
    brand_name_en = brand_name_en.find('a', href=True)['href'].split('brand/')[1] if brand_name_en else None

    original_price = get_content_or_none(soup.find('meta', {'property': 'product:price:normal_price'}))
    final_price = get_content_or_none(soup.find('meta', {'property': 'product:price:amount'}))
    discount_rate = get_content_or_none(soup.find('meta', {'property': 'product:price:sale_rate'}))

    try:
        # review_count, review_avg_ratin
        json_data = json.loads(soup.find('script', {'type': 'application/ld+json'}).string)
        # ratingValue와 reviewCount 값 추출
        review_count = json_data['aggregateRating']['reviewCount']
        review_avg_rating = json_data['aggregateRating']['ratingValue']
    except:
        review_count = None
        review_avg_rating = None
    
    image_tag = soup.find('meta', attrs={'property': 'og:image'})['content']
    
    return product_name, brand_name_kr, brand_name_en, original_price, final_price, discount_rate, review_count, review_avg_rating, image_tag

def et_product2_detail(product_id):
    url = f"https://like.musinsa.com/like/api/v2/liketypes/goods/counts"

    payload = {
        "relationIds": [product_id]
    }
    
    try:
        response = requests.post(url, headers=Musinsa_Config.HEADERS, json=payload).json()

        like_counting = response['data']['contents']['items'][0]['count']
    except:
        like_counting = None

    return like_counting

# product detail parsing => DataFrame Record
def et_product_detail(master_category, depth4category, product_id_list, key):
    for product_id in product_id_list:
        bronze_bucket = "project4-raw-data"
        s3_key = key + f"{product_id}.json"
        time.sleep(0.8)
        
        print(f"product_id : {product_id}")
        
        # request api
        product_name, brand_name_kr, brand_name_en, original_price, final_price, discount_rate, review_count, review_avg_rating, image_tag = et_product1_detail(product_id)
        like_counting = et_product2_detail(product_id)
        
        created_at = datetime.now().strftime('%Y-%m-%d')
        
        # data => dict
        data = {
            "platform": 'Musinsa',
            "master_category_name": master_category,
            "small_category_name": depth4category,
            "product_id": product_id,
            "product_name": product_name,
            "brand_name_kr": brand_name_kr,
            "brand_name_en": brand_name_en,
            "original_price": original_price,
            "final_price": final_price,
            "discount_rate": discount_rate,
            "review_count": review_count,
            "review_avg_rating": review_avg_rating,
            "like_counting": like_counting,
            "image_src" : image_tag,
            "created_at": created_at,
        }
        
        s3_module.upload_json_to_s3(bronze_bucket, s3_key, data)
    
def main():
    # argment parsing
    parser = argparse.ArgumentParser(description="sexual/category param")
    parser.add_argument('sexual', type=str, help='sexual')
    parser.add_argument('category_data', type=str, help='category')
    
    args = parser.parse_args()

    sexual_data = json.loads(args.sexual)
    category_data = json.loads(args.category_data)
    
    category2depth = mapping_2depth_kor(category_data[0])
    
    for category_info in category_data[1]:
        category3depth = list(category_info.items())[0]
        
        for category4depth in category3depth[1].values():
            silver_bucket = "project4-silver-data"
            read_file_path = f"{TODAY_DATE}/Musinsa/RankingData/{category3depth[0]}/{sexual_data[1]}_{category2depth}_{category3depth[0]}_{category4depth}.parquet"
            file_path = f"{silver_bucket}/{read_file_path}"
            product_lists = s3_module.get_product_ids(file_path)
            
            for product_list in porductid_list_iterable(product_lists):
                master_category = f"{sexual_data[1]}-{category2depth}-{category3depth[0]}"
                key = f"{TODAY_DATE}/Musinsa/ProductDetailData/{category3depth[0]}/{category4depth}/"
                t = threading.Thread(target=et_product_detail, args = (master_category, category4depth, product_list, key))
                t.start()
            
if __name__ == "__main__":
    main()
    