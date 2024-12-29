from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from utils.otherapis_dependencies import *

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
KEY_URL = r""
BASE_URL = r"https://api.musinsa.com/api2/campaign/offline/v1/shops"

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
    dag_id='musinsa_offline_shops_api_to_s3_dag',
    default_args=default_args,
    description='Fetch data from an Musinsa offline shops API and save to S3 using Airflow Variables',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    # For each of MEN and WOMAN, STYLE = ALL, til 50th rank
    # Get data for each page ranges
    def fetch_data_task():
        """page로 구분되지 않은 데이터를 적재하는 함수"""
        url = BASE_URL + KEY_URL
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "origin": "https://www.musinsa.com",
            "referer": "https://www.musinsa.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }
        print(f"Fetching pages from {url}...")
        
        now = datetime.now()
        # 문자열 형식으로 변환 (예: "YYYY-MM-DD")
        date_string = now.strftime("%Y-%m-%d")
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        params = {
            "language" : "ko"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()

        # JSON 데이터를 S3에 저장
        s3_file_path = f"/musinsa_offline_raw_data/{date_string}/musinsa_offline.json"
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_KEY + s3_file_path,
            Body=json.dumps(response, ensure_ascii=False),  # 한글 깨짐 방지
            ContentType='application/json'
        )
        
        print(f"Fetched data from Musinsa offline shops api.")

    fetch_musinsa_offline_shops_data_task = PythonOperator(
        task_id=f'fetch_musinsa_offline_shops_data_task',
        python_callable=fetch_data_task,
    )

    # 작업 간의 의존성
    fetch_musinsa_offline_shops_data_task
