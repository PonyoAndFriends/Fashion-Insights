from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import time
import requests
import boto3
import json
import math

# AWS 관련 variables
AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')
AWS_REGION = Variable.get('aws_region')
S3_BUCKET_NAME = Variable.get('s3_bucket_name')
S3_KEY = Variable.get('s3_key')
SERVICE_KEY = r"hqG+v6Og5JXUdp0uzMb+ZSF0PuRN8jKmvz/TqToTbiEIi0Qqi68Gkp6y7Di+sz/ZVbsSgp3EYcnemak6p7nX8A=="  # 이후 Variables로 수정

# API 정보
KEY_URL = "https://infuser.odcloud.kr/oas/docs?namespace=15097972/v1"
BASE_URL = "https://api.odcloud.kr/api"

# 병렬 실행할 TASK 개수
PARALLEL_TASK_NUM = 6
PAGE_SIZE = 20

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
    dag_id='api_to_s3_with_variables',
    default_args=default_args,
    description='Fetch data from an API and save to S3 using Airflow Variables',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    # Step 1: Get last key for population api
    def fetch_last_key():
        try:
            response = requests.get(KEY_URL)
            response.raise_for_status()
            last_key = list(response.json()['paths'])[-1]
            return last_key
        except Exception as e:
            raise ValueError(f"Failed to fetch last key: {str(e)}")

    fetch_last_key_task = PythonOperator(
        task_id='fetch_last_key_task',
        python_callable=fetch_last_key,
    )

    # Step 2: calculate page ranges
    def calculate_page_range_task(last_key, **kwargs):
        url = BASE_URL + last_key
        params = {
            "page": 1,
            "perPage": 1,
            "returnType": "JSON",
            "serviceKey": SERVICE_KEY,
        }
        total_count = requests.get(url, params).json()['totalCount']
        total_pages = math.ceil(total_count / PAGE_SIZE)
        pages_per_task = math.ceil(total_pages / PARALLEL_TASK_NUM)
        
        # 페이지 범위 계산
        page_ranges = [
            (i * pages_per_task + 1, min((i + 1) * pages_per_task, total_pages))
            for i in range(PARALLEL_TASK_NUM)
        ]
        
        print(f"Page ranges: {page_ranges}")
        return page_ranges

    calculate_page_range = PythonOperator(
        task_id='calculate_page_range_task',
        python_callable=calculate_page_range_task,
        op_kwargs={
            "last_key": "{{ task_instance.xcom_pull(task_ids='fetch_last_key_task') }}"
        },
    )

    # Step 3: Get data for each page ranges
    def fetch_paginated_data_task(page_range, **kwargs):
        """특정 page range의 데이터를 s3내로 가져오는 작업"""
        start_page, end_page = page_range
        url = BASE_URL + kwargs['last_key']
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
                "page": page,
                "perPage": PAGE_SIZE,
                "returnType": "JSON",
                "serviceKey": SERVICE_KEY,
            }
            time.sleep(0.2)
            response = requests.get(url, params=params)
            response.raise_for_status()

            # JSON 데이터를 S3에 저장

            s3_file_path = f"/population_data/{date_string}/population_page_{page}.json"
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_KEY + s3_file_path,
                Body=json.dumps(response, ensure_ascii=False),  # 한글 깨짐 방지
                ContentType='application/json'
            )
        
        print(f"Fetched {PAGE_SIZE} datas per each {end_page - start_page + 1} pages from popluation apis.")

    # 병렬 작업 생성
    fetch_data_tasks = []
    for i in range(PARALLEL_TASK_NUM):
        fetch_data_tasks.append(
            PythonOperator(
                task_id=f'fetch_data_task_{i + 1}',
                python_callable=fetch_paginated_data_task,
                op_kwargs={
                    "page_range": "{{ task_instance.xcom_pull(task_ids='calculate_page_range_task')[" + str(i) + "] }}",
                    "last_key": "{{ task_instance.xcom_pull(task_ids='fetch_last_key_task') }}"
                },
            )
        )

    # 작업 간의 의존성
    fetch_last_key_task >> calculate_page_range >> fetch_data_tasks
