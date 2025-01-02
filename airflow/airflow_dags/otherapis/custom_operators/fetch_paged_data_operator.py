from airflow.operators.python import PythonOperator
from custom_modules import s3_upload

import requests
import json
import time


class FetchPagedDataOperator(PythonOperator):
    """
    페이지네이션 된 데이터를 받아오는 커스텀 오퍼레이터

    :param url: api 요청 url
    :param params: api 요청 쿼리 파라미터
    :param s3_dict: S3 설정 정보 딕셔너리
    :param file_topic: 적재될 파일의 주제를 담은 문자열
    :param content_type: json, html 등의 리턴 형식
    :param headers: api 요청 header
    :param page_range: (시작 페이지, 끝 페이지)를 담은 튜플
    """

    def __init__(self, url, page_range, file_topic, content_type, params=None, headers=None, *args, **kwargs):
        super().__init__(python_callable=self._fetch_paged_data, *args, **kwargs)
        self.url = url
        self.params = params
        self.file_topic = file_topic
        self.content_type = content_type
        self.headers = headers
        self.page_range = page_range

    def _fetch_paged_data(self, **kwargs):
        start_page, end_page = self.page_range
        self.log.info(f"Fetching data from pages {start_page} to {end_page}...")

        for page in range(start_page, end_page + 1):
            self.params['page'] = page
            response = requests.get(self.url, params=self.params, headers=self.headers)
            response.raise_for_status()

            now = time.strftime("%Y-%m-%d")
            
            if self.content_type == 'application/json':
                data_file = json.dumps(response.json(), ensure_ascii=False)
                file_path = f"/{self.file_topic}_raw_data/{now}/{self.file_topic}_{page}.json"
            elif self.content_type == 'text/html':
                data_file = response.text
                file_path = f"/{self.file_topic}_raw_data/{now}/{self.file_topic}_{page}.html"
            else:
                data_file = response.text
                file_path = f"/{self.file_topic}_raw_data/{now}/{self.file_topic}_{page}.txt"

            s3_dict = {
                "data_file": data_file,
                "file_path": file_path,
                "content_type": self.content_type
            }
            s3_upload.load_data_to_s3(s3_dict)

        self.log.info(f"Successfully fetched data for pages {start_page} to {end_page}.")
