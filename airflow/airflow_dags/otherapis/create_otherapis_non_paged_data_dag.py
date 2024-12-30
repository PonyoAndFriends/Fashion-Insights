from dag_templates.non_paged_data_dag_template import create_fetch_non_paged_data_dag
from datetime import timedelta
from airflow.models import Variable

import datetime

# 공통 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# header가 필요한 경우 사용
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

# 오늘의 날짜 string
now = datetime.now()
date_string = now.strftime("%Y-%m-%d")
one_week_ago = now - timedelta(weeks=1)
one_week_ago_string = one_week_ago.strftime("%Y%m%d%H%M")

# DAG별 설정
dags_config = [
    {
        "dag_id": "fetch_musinsa_offline_shops_data_dag",
        "api_dict": {
            'base_url': r"https://api.musinsa.com/api2/campaign/offline/v1/shops",
            'key_url': "",
            'params': {
                "language" : "ko"
            },
            'headers': headers,
        },
        "s3_upload_dict": {
            'aws_access_key_id': Variable.get('aws_access_key_id'),
            'aws_secret_access_key': Variable.get('aws_secret_access_key'),
            'aws_region': Variable.get('aws_region'),
            's3_bucket_name': Variable.get('s3_bucket_name'),
            'file_path': Variable.get('s3_key') + "/musinsa_offline_raw_data/{date_string}/musinsa_offline.json",
            'content_type': 'application/json',
        },
        "schedule_interval": "@daily",
    },
    {
        "dag_id": "fetch_weekly_weather_data_dag",
        "api_dict": {
            'base_url': r"https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php",
            'key_url': rf"?inf=SFC&help=1&authKey={Variable.get('weather_api_key')}",
            'params': None,
            'headers': None,
        },
        "s3_upload_dict": {
            'aws_access_key_id': Variable.get('aws_access_key_id'),
            'aws_secret_access_key': Variable.get('aws_secret_access_key'),
            'aws_region': Variable.get('aws_region'),
            's3_bucket_name': Variable.get('s3_bucket_name'),
            'file_path': Variable.get('s3_key') + f"/weekly_weather_data/{one_week_ago_string}_{date_string}/weather_data_from_{one_week_ago_string}_{date_string}.txt",
            'content_type': 'text/plain',
        },
        "schedule_interval": "@daily",
    },
    {
        "dag_id": "fetch_weather_station_data_dag",
        "api_dict": {
            'base_url': r"https://apihub.kma.go.kr/api/typ01/url/stn_inf.php",
            'key_url': rf"?inf=SFC&help=1&authKey={Variable.get('weather_api_key')}",
            'params': None,
            'headers': None,
        },
        "s3_upload_dict": {
            'aws_access_key_id': Variable.get('aws_access_key_id'),
            'aws_secret_access_key': Variable.get('aws_secret_access_key'),
            'aws_region': Variable.get('aws_region'),
            's3_bucket_name': Variable.get('s3_bucket_name'),
            'file_path': Variable.get('s3_key') + f"/weather_meta_data/weather_meta_data.txt",
            'content_type': 'text/plain',
        },
        "schedule_interval": "@once",  # 협의
    },
]

# DAG 생성
for config in dags_config:
    globals()[config["dag_id"]] = create_fetch_non_paged_data_dag(dag_id=config["dag_id"],
        api_dict=config["api_dict"],
        s3_upload_dict=config["s3_upload_dict"],
        default_args=default_args,
        schedule_interval=config["schedule_interval"]
    )
