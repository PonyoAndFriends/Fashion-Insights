from airflow.operators.python import PythonOperator
from custom_modules import s3_upload

import json
import time
import requests


class FetchNonPagedDataOperator(PythonOperator):
    """
    페이지네이션이 없는 단순 api에서 데이터를 받아오는 커스텀 오퍼레이터

    :param url: Base URL
    :param params: API 요청 시 사용할 파라미터를 담은 딕셔너리
    :param headers: API request header
    :param s3_dict: s3에 데이터를 적재하기 위한 설정을 담은 딕셔너리
    """

    def __init__(
        self, url, file_topic, content_type, params=None, headers=None, *args, **kwargs
    ):
        super().__init__(python_callable=self._fetch_data, *args, **kwargs)
        self.url = url
        self.params = params
        self.headers = headers
        self.content_type = content_type
        self.file_topic = file_topic

    def _fetch_data(self):
        """페이지가 없는 api에서 데이터를 가져오는 함수"""
        url = self.url
        self.log.info(f"Fetching non-paged data from {url}...")

        try:
            response = requests.get(url, params=self.params, headers=self.headers)
            response.raise_for_status()
            self.log.info("Data fetch success")

            now = time.strftime("%Y-%m-%d")

            if self.content_type == "application/json":
                data_file = json.dumps(response.json(), ensure_ascii=False)
                file_path = f"/{self.file_topic}_raw_data/{now}/{self.file_topic}.json"
            elif self.content_type == "text/html":
                data_file = response.text
                file_path = f"/{self.file_topic}_raw_data/{now}/{self.file_topic}.html"
            else:
                # plain/text 라고 가정.
                data_file = response.text
                file_path = f"/{self.file_topic}_raw_data/{now}/{self.file_topic}.txt"

            s3_dict = {
                "data_file": data_file,
                "file_path": file_path,
                "content_type": self.content_type,
            }

            s3_upload.load_data_to_s3(s3_dict)
        except requests.exceptions.RequestException as e:
            self.log.error(f"Failed to fetch data: {e}")
            raise
