import requests
import json
import time
import boto3
import logging
import threading
from queue import Queue
from datetime import datetime
from ably_modules.ably_dependencies import ABLY_HEADER, DEFAULT_S3_DICT
from ably_modules.aws_info import AWS_S3_CONFIG

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3Handler:
    """
    S3 작업을 처리하는 클래스
    - S3에서 폴더 목록을 가져옴
    - 특정 폴더에서 goods_sno_list.json 파일을 읽어 상품 ID와 카테고리 키를 추출함
    """
    def __init__(self, bucket_name):
        """
        초기화 메서드.
        - S3 클라이언트를 생성하고 사용할 S3 버킷 이름과 데이터 경로를 설정

        Args:
            bucket_name (str): S3 버킷 이름
        """
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_S3_CONFIG.get("aws_access_key_id"),
            aws_secret_access_key=AWS_S3_CONFIG.get("aws_secret_access_key")
        )
        self.bucket_name = bucket_name
        self.date_prefix = datetime.now().strftime('%Y-%m-%d')
        self.base_path = f"{self.date_prefix}/ReviewData/"

    def list_folders(self):
        """
        S3에서 ReviewData 폴더 내 모든 서브 폴더 목록을 가져옴
        - S3의 Prefix 및 Delimiter를 사용하여 특정 경로의 하위 디렉토리만 가져옴

        Returns:
            list: 폴더 경로 리스트. 폴더 경로가 없으면 빈 리스트 반환
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.base_path,
                Delimiter='/'
            )
            return [content['Prefix'] for content in response.get('CommonPrefixes', [])]
        except Exception as e:
            logger.error(f"Error listing folders: {e}")
            return []

    def get_product_ids(self, folder):
        """
        특정 폴더에서 goods_sno_list.json 파일을 읽고 상품 ID와 카테고리 키를 반환
        - 파일이 없거나 읽는 중 에러가 발생하면 None과 빈 리스트를 반환

        Args:
            folder (str): S3의 폴더 경로

        Returns:
            tuple: (카테고리 키, 상품 ID 리스트). 에러 발생 시 (None, [])
        """
        try:
            key = f"{folder}goods_sno_list.json"
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            return next(iter(data.items()))  # (category_key, product_ids)
        except Exception as e:
            logger.error(f"Error reading goods_sno_list.json: {e}")
            return None, []

class ReviewProcessor:
    """
    리뷰 데이터를 처리하는 클래스
    - 특정 상품 ID에 대해 리뷰 데이터를 API에서 가져옴
    - 가져온 리뷰 데이터를 S3에 저장
    """
    def __init__(self, api_url, headers, s3_client, bucket_name, max_reviews=20, retries=3):
        """
        초기화 메서드.
        - 리뷰 데이터를 가져오기 위한 API 설정과 S3 클라이언트를 초기화

        Args:
            api_url (str): 리뷰 데이터를 요청할 API URL
            headers (dict): API 요청 헤더
            s3_client (boto3.client): S3 클라이언트
            bucket_name (str): S3 버킷 이름
            max_reviews (int): 가져올 최대 리뷰 개수
            retries (int): API 요청 재시도 횟수
        """
        self.api_url = api_url
        self.headers = headers
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.max_reviews = max_reviews
        self.retries = retries

    def fetch_reviews(self, product_id):
        """
        특정 상품 ID에 대해 API를 호출하여 리뷰 데이터를 가져옴
        - 최대 max_reviews 개의 리뷰 데이터를 가져옴

        Args:
            product_id (str): 상품 ID

        Returns:
            list: 가져온 리뷰 데이터 리스트. 실패 시 빈 리스트 반환
        """
        url = f"{self.api_url}/{product_id}/reviews/"
        reviews = []

        for attempt in range(self.retries):
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code != 200:
                    logger.warning(f"Error {response.status_code} for {product_id}, retry {attempt + 1}")
                    time.sleep(2)
                    continue
                data = response.json().get("reviews", [])
                reviews.extend(data)
                if len(reviews) >= self.max_reviews:
                    return reviews[:self.max_reviews]
                break
            except Exception as e:
                logger.error(f"Error fetching reviews for {product_id}: {e}")

        return reviews[:self.max_reviews]

    def save_reviews(self, product_id, reviews, category_key, folder):
        """
        가져온 리뷰 데이터를 S3에 저장.
        - 저장 경로는 {folder}/reviews_{category_key}_{product_id}.json 형태.

        Args:
            product_id (str): 상품 ID
            reviews (list): 리뷰 데이터 리스트
            category_key (str): 카테고리 키
            folder (str): S3 폴더 경로
        """
        file_name = f"reviews_{category_key}_{product_id}.json"
        key = f"{folder}{file_name}"
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(reviews, ensure_ascii=False, indent=4),
                ContentType="application/json"
            )
            logger.info(f"Saved reviews: {key}")
        except Exception as e:
            logger.error(f"Error saving reviews to S3: {e}")

    def process(self, product_id, category_key, folder):
        """
        상품 ID에 대해 리뷰 데이터를 가져오고 S3에 저장

        Args:
            product_id (str): 상품 ID
            category_key (str): 카테고리 키
            folder (str): S3 폴더 경로
        """
        logger.info(f"Processing {product_id}")
        reviews = self.fetch_reviews(product_id)
        if reviews:
            self.save_reviews(product_id, reviews, category_key, folder)
        else:
            logger.warning(f"No reviews for {product_id}")

def worker(task_queue, processor, category_key, folder):
    """
    작업 스레드 함수
    - Queue에서 상품 ID를 가져와 리뷰 데이터를 처리

    Args:
        task_queue (Queue): 상품 ID 작업 큐
        processor (ReviewProcessor): 리뷰 데이터 처리기
        category_key (str): 카테고리 키
        folder (str): S3 폴더 경로
    """
    while not task_queue.empty():
        product_id = task_queue.get()
        processor.process(product_id, category_key, folder)
        task_queue.task_done()

if __name__ == "__main__":
    bucket_name = DEFAULT_S3_DICT["bucket_name"]
    api_url = "https://api.a-bly.com/webview/goods"
    headers = ABLY_HEADER

    s3_handler = S3Handler(bucket_name)
    folders = s3_handler.list_folders()

    processor = ReviewProcessor(api_url, headers, s3_handler.s3_client, bucket_name)

    for folder in folders:
        category_key, product_ids = s3_handler.get_product_ids(folder)
        if product_ids:
            task_queue = Queue()
            for product_id in product_ids:
                task_queue.put(product_id)

            threads = []
            for _ in range(5):  # 5개 스레드 실행
                thread = threading.Thread(target=worker, args=(task_queue, processor, category_key, folder))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()
