from datetime import datetime
from itertools import islice
from ably_modules.ably_dependencies import (
    DEFAULT_S3_DICT,
    ABLY_HEADER,
    ABLY_NEXT_TOKEN,
    ABLY_CATEGORY,
)
import threading
import logging
import json
import boto3
import requests

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# 데이터 수집 및 업로드 클래스 정의
class DataPipeline:
    """
    데이터 파이프라인 클래스

    데이터 수집, 가공한 데이터를 S3 버킷에 업로드
    각 카테고리와 서브카테고리에 대해 멀티스레딩을 활용하여 병렬 처리를 수행
    """

    def __init__(self):
        """
        DataPipeline 클래스의 생성자, API 호출을 위한 기본 설정 및 S3 버킷 이름 초기화
        """
        # API 호출을 위한 기본 URL과 헤더 설정
        self.api_url = "https://api.a-bly.com/api/v2/screens/COMPONENT_LIST/"
        self.api_headers = ABLY_HEADER
        # API 호출에 필요한 초기 토큰 설정
        self.next_token = ABLY_NEXT_TOKEN
        self.s3_bucket = DEFAULT_S3_DICT["bucket_name"]  # 데이터를 저장할 S3 버킷 이름
        # 카테고리와 서브카테고리 매핑
        self.category_subcategories = ABLY_CATEGORY

        # 스레드 동시 실행 제한을 위한 세마포어 초기화
        self.semaphore = threading.Semaphore(10)

    def collect_and_upload_data(self, category_id, sub_category_id, data_type):
        """
        지정된 카테고리 및 서브카테고리에 대해 데이터를 수집하고 S3에 업로드

        Args:
            category_id (int): 카테고리 ID
            sub_category_id (int): 서브카테고리 ID
            data_type (str): 처리할 데이터 유형 ('ranking' 또는 'goods_sno')

        Raises:
            ValueError: 유효하지 않은 데이터 유형이 전달된 경우
        """
        with self.semaphore:  # 세마포어로 동시 실행 제한
            try:
                # API 요청 페이로드 생성
                payload = {
                    "next_token": self.next_token,
                    "category_sno": category_id,
                    "sub_category_sno": sub_category_id,
                }
                # API 호출
                response = requests.get(
                    self.api_url, headers=self.api_headers, params=payload, timeout=10
                )
                response.raise_for_status()
                data = response.json()

                # 데이터 유형에 따라 처리
                if data_type == "ranking":
                    file_name = "RankingData"
                    items = data.get("item", [])[:50]  # 최대 50개의 item만 가져오기
                    content = json.dumps(items, ensure_ascii=False, indent=4)
                elif data_type == "goods_sno":
                    file_name = "ReviewData"
                    goods_sno_list = [
                        item["logging"]["analytics"]["GOODS_SNO"]
                        for item in data.get("item", [])[:50]  # item에서 데이터 추출
                        if "logging" in item
                        and "analytics" in item["logging"]
                        and "GOODS_SNO" in item["logging"]["analytics"]
                    ]
                    content = json.dumps(goods_sno_list, ensure_ascii=False, indent=4)
                else:
                    raise ValueError(f"Invalid data type: {data_type}")

                # S3 경로 생성
                today_date = datetime.now().strftime("%Y-%m-%d")
                s3_path = f"{today_date}/Ably/{file_name}/{category_id}_{sub_category_id}/data.json"

                # 데이터 S3에 업로드
                s3_client = boto3.client("s3")
                s3_client.put_object(Body=content, Bucket=self.s3_bucket, Key=s3_path)

                # 업로드 성공 로그 출력
                logger.info(f"Uploaded {data_type} data to S3: {s3_path}")
            except Exception as e:
                # 오류 발생 시 로그 출력
                logger.error(
                    f"Error uploading {data_type} data for category {category_id}, sub_category {sub_category_id}: {e}"
                )

    def run_all_categories(self, data_type):
        """
        모든 카테고리에 대해 데이터를 수집하고 S3에 업로드

        Args:
            data_type (str): 처리할 데이터 유형 ('ranking'  or 'goods_sno')
        """

        def batched(iterable, batch_size):
            """
            이터러블을 주어진 배치 크기로 분할하는 제너레이터 함수

            Args:
                iterable (iterable): 분할할 데이터
                batch_size (int): 배치 크기

            Yields:
                list: 분할된 배치 데이터
            """
            iterator = iter(iterable)
            for first in iterator:
                yield [first] + list(islice(iterator, batch_size - 1))

        threads = []  # 스레드 목록 초기화
        for category_id, sub_categories in self.category_subcategories.items():
            for batch in batched(sub_categories, 50):  # 서브카테고리를 배치 단위로 처리
                for sub_category_id in batch:
                    # 각 서브카테고리에 대해 스레드 생성 및 실행
                    thread = threading.Thread(
                        target=self.collect_and_upload_data,
                        args=(category_id, sub_category_id, data_type),
                    )
                    threads.append(thread)
                    thread.start()

        # 모든 스레드 작업 완료 대기
        for thread in threads:
            thread.join()


<<<<<<< HEAD:python_dockerfile/python_scripts/ably/ably_ranking_data.py

if __name__=="__main__":
    pipeline_1 = DataPipeline()
    pipeline_1.run_all_categories('ranking')

    pipeline_2 = DataPipeline()
    pipeline_2.run_all_categories('goods_sno')
=======
if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_all_categories("ranking")
    pipeline.run_all_categories("goods_sno")
>>>>>>> 7c2a8840d8497ba441288bc326da431e62e7c7ca:python_dockerfile/python_scripts/ably/dag_ably_ranking_data.py
