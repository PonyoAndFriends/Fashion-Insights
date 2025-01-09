from airflow.models import BaseOperator
from custom_operators.custom_modules import s3_upload
from custom_operators.custom_modules.otherapis_dependencies import FILE_EXT
from zoneinfo import ZoneInfo

import json
from datetime import datetime
import requests


class FetchNonPagedDataOperator(BaseOperator):
    """
    페이지네이션이 없는 단순 API에서 데이터를 받아오는 커스텀 오퍼레이터

    :param url: api 호출 URL
    :param params: API 요청 시 사용할 파라미터를 담은 딕셔너리
    :param headers: API request header
    :param s3_dict: s3에 데이터를 적재하기 위한 설정을 담은 딕셔너리
    """

    def __init__(
        self, url, file_topic, content_type, params=None, headers=None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.url = url
        self.params = params
        self.headers = headers
        self.content_type = content_type
        self.file_topic = file_topic

    def execute(self, context):
        """페이지가 없는 API에서 데이터를 가져오는 함수"""
        url = self.url
        self.log.info(f"Fetching non-paged data from {url}...")

        try:
            # API 호출
            response = requests.get(url, params=self.params, headers=self.headers)
            response.raise_for_status()
            self.log.info("Data fetch success")

            # 현재 날짜 생성
            now = datetime.now().astimezone(ZoneInfo("Asia/Seoul"))
            now_string = now.strftime("%Y-%m-%d")

            # 데이터 처리 및 파일 경로 생성
            file_ext = FILE_EXT
            if self.content_type == "application/json":
                data_file = json.dumps(response.json(), ensure_ascii=False)
            else:
                data_file = response.text
            file_path = f"{now_string}/otherapis/{self.file_topic}_raw_data/{self.file_topic}.{file_ext[self.content_type]}"
            # S3 업로드를 위한 딕셔너리 준비
            s3_dict = {
                "data_file": data_file,
                "file_path": file_path,
                "content_type": self.content_type,
            }

            # 데이터를 S3로 업로드
            s3_upload.load_data_to_s3(s3_dict)

        except requests.exceptions.RequestException as e:
            self.log.error(f"Failed to fetch data: {e}")
            raise
