from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago

# 기본 설정값 정의
default_args = {
    "owner": "jeongseoha1203@gmail.com",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
}

# DAG 정의
dag = DAG(
    "fetch_and_upload_pipeline",
    default_args=default_args,
    description="Fetch data from API and upload to S3 using KubernetesPodOperator",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

# KubernetesPodOperator 태스크 정의
fetch_and_upload_task = KubernetesPodOperator(
    namespace="default",
    image="python:3.9",  # API 호출 및 파일 업로드를 위한 Python 기반 Docker 이미지
    cmds=["python", "-c"],
    arguments=[
        """
import os
import boto3
import requests
import time
import json
import logging
from datetime import datetime
# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = 'https://api.a-bly.com/api/v2/screens/COMPONENT_LIST/'
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'origin': 'https://m.a-bly.com',
    'referer': 'https://m.a-bly.com/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
    'x-anonymous-token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhbm9ueW1vdXNfaWQiOiIzMzc3ODI3NDAiLCJpYXQiOjE3MzQ0MDE2MTR9.LZgtaLkf3KFnAcG4b5bcOa_taHF9FL6odYx5vd_o7w4',
    'x-app-version': '0.1.0',
    'x-device-id': '42e0f21c-3460-40f5-bfa5-d1595286ed3e',
    'x-device-type': 'MobileWeb',
    'x-web-type': 'Web'
}

# Category 정보를 딕셔너리로 정의
categories = {
    7: {"SUB_CATEGORY_SNO": [16, 293, 294, 296, 297, 496, 497, 577]},
    8: {"SUB_CATEGORY_SNO": [18, 21, 298, 299, 300, 357, 498, 499, 500]},
    174: {"SUB_CATEGORY_SNO": [176, 177, 178, 501]},
    517: {"SUB_CATEGORY_SNO": [518, 520, 521, 519]},
    10: {"SUB_CATEGORY_SNO": [206, 207]},
    203: {"SUB_CATEGORY_SNO": [204, 205]},
    481: {"SUB_CATEGORY_SNO": [488, 489, 490, 491, 492, 493, 494]}
}

for category_sno, data in categories.items():
    sub_category_list = data["SUB_CATEGORY_SNO"]

    for sub_category_sno in sub_category_list:
        response = requests.get(BASE_URL, headers=HEADERS, params={"category_sno": category_sno, "sub_category_sno": sub_category_sno})
        response.raise_for_status()

        items = response.json()
        file_name = f"category_{category_sno}_sub_category_{sub_category_sno}.json"
        with open(file_name, "w") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)

        # S3 업로드
        s3 = boto3.client("s3")
        s3.upload_file(file_name, "ablyrawdata", f"Ably/{file_name}")
        os.remove(file_name)
        logger.info(f"Uploaded and deleted local file: {file_name}")
        """
    ],
    name="fetch-and-upload",
    task_id="fetch_and_upload_task",
    get_logs=True,
    dag=dag,
)
