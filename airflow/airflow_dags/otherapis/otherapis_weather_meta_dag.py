from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import boto3
import requests

# AWS 관련 variables
AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')
AWS_REGION = Variable.get('aws_region')
S3_BUCKET_NAME = Variable.get('s3_bucket_name')
S3_KEY = Variable.get('s3_key')

# API Key 설정
API_KEY = r"qpWkt6VHS2eVpLelRztnaw"  # 이후 Variable 등으로 수정
BASE_URL = r"https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"

def fetch_and_save_weather_meta_data():
    """날씨 관측소 메타데이터를 불러와서 바로 S3에 저장"""
    
    url = rf"{BASE_URL}?inf=SFC&help=1&authKey={API_KEY}"

    res = requests.get(url)
    
    # 에러 핸들링
    if res.status_code != 200:
        raise ValueError("API 요청에 실패했습니다.")

    # S3에 업로드할 데이터를 파일로 저장
    s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    
    s3_file_path = f"/weather_meta_data/weather_meta_data.txt"
    
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_KEY + s3_file_path,
        Body=res.text,
        ContentType='application/json'
    )

# DAG 설정
dag = DAG(
    'weather_meta_data_to_s3',
    default_args={
        'owner': 'airflow',
        'retries': 0,
    },
    description='Fetch weather meta data and upload to S3',
    schedule_interval=None,  
)

# 태스크 정의
fetch_and_save_weather_meta_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_and_save_weather_meta_data,
    dag=dag,
)

# 태스크 실행 순서 설정
fetch_and_save_weather_meta_data_task
