from airflow.models import Variable
from datetime import timedelta
from dag_templates.paged_data_dag_template import create_fetch_paged_data_dag

# 공통 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# request 헤더 공통 설정
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

# DAG별 설정
dags_config = [
    {
        "dag_id": "fetch_musinsa_snap_ranking_brand_data_dag",
        "context_dict": {
            "base_url": r"https://content.musinsa.com/api2/content/snap/v1",
            "key_url": r"/profile-rankings/BRAND/DAILY",
            "params": {
                'page': 1,
                'size': 36,
            },
            "page_range": [(1, 2), (3, 4)],
            "page_size": 25,
            "aws_config_dict": {
                "aws_access_key_id": Variable.get('aws_access_key_id'),
                "aws_secret_access_key": Variable.get('aws_secret_access_key'),
                "aws_region": Variable.get('aws_region'),
                "s3_bucket_name": Variable.get('s3_bucket_name'),
            },
            "file_topic": "musinsa_snap_ranking_brand",
            "content_type": "application/json",
            "headers": headers,
        },
        "schedule_interval": "@daily",
    },
    {
        "dag_id": "fetch_musinsa_snap_ranking_history_MEN_data_dag",
        "context_dict": {
            "base_url": r"https://content.musinsa.com/api2/content/snap/v1",
            "key_url": r"/rankings/DAILY",
            "params": {
                "gender": "MEN", # MEN / WOMEN
                "page": 1,
                "size": 20,
                "style" : 'ALL'
            },
            "page_range": [(1, 2)],
            "page_size": 25,
            "aws_config_dict": {
                "aws_access_key_id": Variable.get('aws_access_key_id'),
                "aws_secret_access_key": Variable.get('aws_secret_access_key'),
                "aws_region": Variable.get('aws_region'),
                "s3_bucket_name": Variable.get('s3_bucket_name'),
            },
            "file_topic": "musinsa_snap_ranking_history_men",
            "content_type": "application/json",
            "headers": headers,
        },
        "schedule_interval": "@daily",
    },
    {
        "dag_id": "fetch_musinsa_snap_ranking_history_WOMEN_data_dag",
        "context_dict": {
            "base_url": r"https://content.musinsa.com/api2/content/snap/v1",
            "key_url": r"/rankings/DAILY",
            "params": {
                "gender": "WOMEN",
                "page": 1,
                "size": 20,
                "style" : 'ALL'
            },
            "page_range": [(1, 2)],
            "page_size": 25,
            "aws_config_dict": {
                "aws_access_key_id": Variable.get('aws_access_key_id'),
                "aws_secret_access_key": Variable.get('aws_secret_access_key'),
                "aws_region": Variable.get('aws_region'),
                "s3_bucket_name": Variable.get('s3_bucket_name'),
            },
            "file_topic": "musinsa_snap_ranking_history_women",
            "content_type": "application/json",
            "headers": headers,
        },
        "schedule_interval": "@daily",
    }
]

# 템플릿을 사용하여 DAG 생성
for config in dags_config:
    globals()[config["dag_id"]] = create_fetch_paged_data_dag(
        dag_id=config["dag_id"],
        context_dict=config["context_dict"],
        default_args=default_args,
        schedule_interval=config["schedule_interval"]
    )
