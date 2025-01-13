# from concurrent.futures import ThreadPoolExecutor, as_completed
# import requests
# import json
# import time
# import boto3
# import logging
# from datetime import datetime, timedelta
# from ably_modules.ably_dependencies import ABLY_HEADER, DEFAULT_S3_DICT
# from ably_modules.aws_info import AWS_S3_CONFIG

# # 로깅 설정
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# class S3Handler:
#     def __init__(self, bucket_name):
#         self.s3_client = boto3.client(
#             "s3",
#             aws_access_key_id=AWS_S3_CONFIG.get("aws_access_key_id"),
#             aws_secret_access_key=AWS_S3_CONFIG.get("aws_secret_access_key")
#         )
#         self.bucket_name = bucket_name
#         self.date_prefix = 'bronze/' + (datetime.now() + timedelta(hours=9)).strftime('%Y-%m-%d')
#         self.base_path = f"{self.date_prefix}/ably/review_data/"
#         logger.info(f"base_path: {self.base_path}")

#     def list_folders(self):
#         try:
#             response = self.s3_client.list_objects_v2(
#                 Bucket=self.bucket_name,
#                 Prefix=self.base_path,
#                 Delimiter='/'
#             )
#             return [content['Prefix'] for content in response.get('CommonPrefixes', [])]
#         except Exception as e:
#             logger.error(f"Error listing folders: {e}")
#             return []

#     def get_product_ids(self, folder):
#         try:
#             key = f"{folder}goods_sno_list.json"
#             response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
#             data = json.loads(response['Body'].read().decode('utf-8'))
#             return next(iter(data.items()))
#         except Exception as e:
#             logger.error(f"Error reading goods_sno_list.json: {e}")
#             return None, []

# class ReviewProcessor:
#     def __init__(self, api_url, headers, s3_client, bucket_name, max_reviews=20, retries=3):
#         self.api_url = api_url
#         self.headers = headers
#         self.s3_client = s3_client
#         self.bucket_name = bucket_name
#         self.max_reviews = max_reviews
#         self.retries = retries

#     def fetch_reviews(self, product_id):
#         url = f"{self.api_url}/{product_id}/reviews/"
#         logger.info(f"fetchgin from url: {url}")
#         logger.info(f"fetchgin using headers: {headers}")
#         reviews = []

#         for attempt in range(self.retries):
#             try:
#                 time.sleep(3)
#                 response = requests.get(url, headers=self.headers)
#                 if response.status_code == 200:
#                     data = response.json().get("reviews", [])
#                     reviews.extend(data)
#                     if len(reviews) >= self.max_reviews:
#                         return reviews[:self.max_reviews]
#                     break
#                 elif response.status_code == 403:
#                     logger.warning(f"403 Forbidden for {product_id}, retrying...")
#                     time.sleep(2 ** attempt + 0.1)
#                 else:
#                     logger.warning(f"Unexpected status code {response.status_code} for {product_id}")
#             except Exception as e:
#                 logger.error(f"Error fetching reviews for {product_id}: {e}")
#         return reviews[:self.max_reviews]

#     def save_reviews(self, product_id, reviews, category_key, folder):
#         file_name = f"reviews_{category_key}_{product_id}.json"
#         key = f"{folder}{file_name}"
#         try:
#             self.s3_client.put_object(
#                 Bucket=self.bucket_name,
#                 Key=key,
#                 Body=json.dumps(reviews, ensure_ascii=False, indent=4),
#                 ContentType="application/json"
#             )
#             logger.info(f"Saved reviews: {key}")
#         except Exception as e:
#             logger.error(f"Error saving reviews to S3: {e}")

#     def process(self, product_id, category_key, folder):
#         reviews = self.fetch_reviews(product_id)
#         if reviews:
#             self.save_reviews(product_id, reviews, category_key, folder)
#         else:
#             logger.warning(f"No reviews for {product_id}")

# if __name__ == "__main__":
#     bucket_name = DEFAULT_S3_DICT["bucket_name"]
#     api_url = "https://api.a-bly.com/webview/goods"
#     headers = ABLY_HEADER

#     s3_handler = S3Handler(bucket_name)
#     folders = s3_handler.list_folders()
#     logger.info("I'm working man!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

#     processor = ReviewProcessor(api_url, headers, s3_handler.s3_client, bucket_name)

#     for folder in folders:
#         category_key, product_ids = s3_handler.get_product_ids(folder)
#         if product_ids:
#             with ThreadPoolExecutor(max_workers=5) as executor:
#                 futures = [
#                     executor.submit(processor.process, product_id, category_key, folder)
#                     for product_id in product_ids
#                 ]
#                 for future in as_completed(futures):
#                     try:
#                         future.result()  # 예외가 있으면 여기서 발생
#                     except Exception as e:
#                         logger.error(f"Error in processing: {e}")

import json, requests
import logging
from ably_modules.ably_dependencies import ABLY_HEADER

logger = logging.getLogger(__name__)
url = "https://api.a-bly.com/webview/goods/33882115/reviews"

res = requests.get(url, headers=ABLY_HEADER)
logger.info(f"response: {print(json.dumps(res.json(), indent=4, ensure_ascii=False))}")

