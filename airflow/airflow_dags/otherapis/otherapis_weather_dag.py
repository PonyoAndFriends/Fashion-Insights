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
BASE_URL = r"https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php"

def fetch_and_save_weather_data():
    """주간 날씨 데이터를 불러오고 바로 S3에 저장"""
    now = datetime.now()
    one_week_ago = now - timedelta(weeks=1)

    now_string = now.strftime("%Y%m%d%H%M")
    one_week_ago_string = one_week_ago.strftime("%Y%m%d%H%M")

    url = rf"{BASE_URL}?tm1={one_week_ago_string}&tm2={now_string}&help=1&authKey={API_KEY}"

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
    
    s3_file_path = f"/weekly_weather_data/{one_week_ago_string}_{now_string}/weather_data_from_{one_week_ago_string}_{now_string}.txt"
    
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_KEY + s3_file_path,
        Body=res.text,
        ContentType='application/json'
    )

# DAG 설정
dag = DAG(
    'weather_data_to_s3',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Fetch weather data and upload to S3',
    schedule_interval=timedelta(hours=1),  # 매 시간마다 실행
    start_date=datetime(2024, 1, 1),
)

# 태스크 정의
fetch_and_save_weather_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_and_save_weather_data,
    dag=dag,
)

# 태스크 실행 순서 설정
fetch_and_save_weather_data_task
