from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from utils.otherapis_dependencies import *

import time
import requests
import boto3
import json

# AWS 관련 variables
AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')
AWS_REGION = Variable.get('aws_region')
S3_BUCKET_NAME = Variable.get('s3_bucket_name')
S3_KEY = Variable.get('s3_key')

# API 정보
KEY_URL = r"/profile-rankings/BRAND/DAILY"
BASE_URL = r"https://content.musinsa.com/api2/content/snap/v1"

# 병렬 실행할 TASK 개수
PARALLEL_TASK_NUM = 2
PAGE_SIZE = 25

# Airflow 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='musinsa_snap_api_brand_ranking_to_s3_dag',
    default_args=default_args,
    description='Fetch data from an Musinsa SNAP API and save to S3 using Airflow Variables',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    # Get data for each page ranges
    def fetch_paginated_data_task(page_range):
        """특정 page의 데이터를 s3로 적재하는 함수"""
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "origin": "https://www.musinsa.com",
            "referer": "https://www.musinsa.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }
        start_page, end_page = page_range
        url = BASE_URL + KEY_URL
        print(f"Fetching pages {start_page} to {end_page}...")
        
        now = datetime.now()
        # 문자열 형식으로 변환 (예: "YYYY-MM-DD")
        date_string = now.strftime("%Y-%m-%d")
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        for page in range(start_page, end_page + 1):
            params = {
                'page': 1,
                'size': 36
            }
            time.sleep(0.2)
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()

            # JSON 데이터를 S3에 저장
            s3_file_path = f"/musinsa_snap_raw_data/{date_string}/snap_ranking_brand_{page}.json"
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_KEY + s3_file_path,
                Body=json.dumps(response, ensure_ascii=False),  # 한글 깨짐 방지
                ContentType='application/json'
            )
        
        print(f"Fetched {PAGE_SIZE} datas per each {end_page - start_page + 1} pages from Musinsa SNAP Ranking brand api.")

    # 병렬 작업 생성
    fetch_data_tasks = []
    page_ranges = [(1, 2), (3, 4)]
    for i in range(PARALLEL_TASK_NUM):
        fetch_data_tasks.append(
            PythonOperator(
                task_id=f'fetch_data_task_{i + 1}',
                python_callable=fetch_paginated_data_task,
                op_kwargs={
                    "page_range": page_ranges[i],
                },
            )
        )

    # 작업 간의 의존성
    fetch_data_tasks
