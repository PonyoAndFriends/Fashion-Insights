from datetime import datetime
from itertools import islice
import os
from ably_modules.ably_dependencies import (
    DEFAULT_S3_DICT,
    ABLY_HEADER,
    ABLY_NEXT_TOKEN,
    ABLY_CATEGORY,
)
from ably_modules.aws_info import (
    AWS_S3_CONFIG,
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

    에이블리 API로 상품 데이터 수집, AWS S3 업로드 파이프라인 구현
    멀티스레딩 활용해 여러 카테고리 데이터 병렬 처리

    Attributes:
        api_url: 에이블리 API 엔드포인트 URL
        api_headers: API 요청 헤더
        next_token: 페이지네이션용 토큰
        s3_bucket: AWS S3 버킷명
        category_subcategories: 카테고리/서브카테고리 정보
        semaphore: 동시 실행 스레드 제한용 세마포어
        aws_access_key_id: AWS 액세스 키 ID
        aws_secret_access_key: AWS 시크릿 액세스 키
        region_name: AWS 리전명
    """

    def __init__(self):
        """
        DataPipeline 클래스 생성자

        API 호출과 S3 업로드용 설정값 초기화
        """
        self.api_url = "https://api.a-bly.com/api/v2/screens/COMPONENT_LIST/"
        self.api_headers = ABLY_HEADER
        self.next_token = ABLY_NEXT_TOKEN
        self.s3_bucket = DEFAULT_S3_DICT.get("bucket_name")
        self.category_subcategories = ABLY_CATEGORY
        self.semaphore = threading.Semaphore(10)

        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = "ap-northeast-2"

    def collect_and_upload_data(self, category_id, sub_category_id, data_type):
        """
        지정 카테고리 데이터 수집/S3 업로드

        API로 최대 50개 상품 데이터 수집
        데이터 타입별 랭킹 데이터/상품 고유번호 추출
        S3에 JSON 형태로 저장

        Args:
            category_id: 카테고리 ID
            sub_category_id: 서브카테고리 ID
            data_type: 데이터 타입 ('ranking'/'goods_sno')

        Raises:
            ValueError: 유효하지 않은 data_type 전달 시
            Exception: API 요청/S3 업로드 중 오류 발생 시
        """
        with self.semaphore:
            try:
                # [수정] 여러 번 요청하여 50개까지 누적하는 로직
                collected_items = []
                next_token_val = self.next_token  # 초기값 설정

                # 최대 50개 모을 때까지, 또는 next_token이 없을 때까지 반복
                while len(collected_items) < 50 and next_token_val:
                    payload = {
                        "next_token": next_token_val,
                        "category_sno": category_id,
                        "sub_category_sno": sub_category_id,
                    }
                    response = requests.get(
                        self.api_url,
                        headers=self.api_headers,
                        params=payload,
                        timeout=10,
                    )
                    response.raise_for_status()
                    data = response.json()
                    components = data.get("components", [])

                    for component in components:
                        if component.get("component_id") == 9:
                            item_list = component.get("entity", {}).get("item_list", [])
                            collected_items.extend(item_list)

                    # 다음 next_token 갱신
                    next_token_val = data.get("next_token")

                    # 더 이상 next_token이 없거나 50개 이상이면 반복 종료
                    if not next_token_val or len(collected_items) >= 50:
                        break

                items = collected_items[:50]
                if data_type == "ranking":
                    folder_name = "RankingData"
                    file_name = f"{category_id}_{sub_category_id}"
                    content = json.dumps(items, ensure_ascii=False, indent=4)

                elif data_type == "goods_sno":
                    folder_name = "ReviewData"
                    goods_sno_list = []

                    for element in items:
                        # "item" 키 검사
                        item_dict = element.get("item", {})
                        if not item_dict:
                            print("item_dict가 비어있습니다:", element)
                            continue

                        # "like" 키 검사
                        like_dict = item_dict.get("like", {})
                        if not like_dict:
                            print("like_dict가 비어있습니다:", item_dict)
                            continue

                        # "goods_sno" 값 추출 및 검사
                        goods_sno = like_dict.get("goods_sno")
                        if goods_sno:
                            goods_sno_list.append(goods_sno)

                    # 카테고리ID_서브카테고리ID를 key, goods_sno 목록을 value로 저장
                    goods_sno_dict = {
                        f"{category_id}_{sub_category_id}": goods_sno_list
                    }

                    file_name = "goods_sno_list"
                    content = json.dumps(goods_sno_dict, ensure_ascii=False, indent=4)
                else:
                    raise ValueError(f"Invalid data type: {data_type}")

                today_date = datetime.now().strftime("%Y-%m-%d")
                s3_path = f"{today_date}/{folder_name}/{category_id}_{sub_category_id}/{file_name}.json"
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                )
                s3_client.put_object(Body=content, Bucket=self.s3_bucket, Key=s3_path)

                logger.info(f"Uploaded {data_type} data to S3: {s3_path}")
            except Exception as e:
                logger.error(
                    f"Error uploading {data_type} data for category {category_id}, sub_category {sub_category_id}: {e}"
                )

    def run_all_categories(self, data_type):
        """
        전체 카테고리/서브카테고리 데이터 수집 및 S3 업로드

        멀티스레딩으로 병렬 처리
        50개씩 배치로 나누어 실행

        Args:
            data_type: 수집 데이터 유형
                - 'ranking': 상품 랭킹 데이터
                - 'goods_sno': 상품 고유번호 데이터
        """

        def batched(iterable, batch_size):
            # 주어진 이터러블을 batch_size 크기의 청크로 나누는 헬퍼 함수
            iterator = iter(iterable)
            for first in iterator:
                yield [first] + list(islice(iterator, batch_size - 1))

        threads = []
        for category_id, category_info in self.category_subcategories.items():
            sub_categories = category_info["sub_category"].keys()
            for batch in batched(sub_categories, 50):
                for sub_category_id in batch:
                    thread = threading.Thread(
                        target=self.collect_and_upload_data,
                        args=(category_id, sub_category_id, data_type),
                    )
                    threads.append(thread)
                    thread.start()

        for thread in threads:
            thread.join()


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_all_categories("ranking")
    pipeline.run_all_categories("goods_sno")
