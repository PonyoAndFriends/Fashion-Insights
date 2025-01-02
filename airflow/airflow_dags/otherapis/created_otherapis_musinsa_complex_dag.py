from airflow.models import Variable
from datetime import timedelta
from dag_templates.paged_data_dag_template import create_fetch_paged_data_dag
import logging

# 공통 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        "dag_id": "fetch_musinsa_magazine_news_data_dag",
        "context_dict": {
            "first_url": "https://content.musinsa.com/api2/content/magazine-content/v1/news",
            "second_url": "https://www.musinsa.com/cms/news/view/",
            "params": {
                "page": 1,
                "size": 20,
                "lookbookInclude": "true"
            },
            "headers": headers,
            "aws_config_dict": {
                'aws_access_key_id': Variable.get('aws_access_key_id'),
                'aws_secret_access_key': Variable.get('aws_secret_access_key'),
                'aws_region': Variable.get('aws_region'),
                's3_bucket_name': Variable.get('s3_bucket_name'),
                's3_key': Variable.get('s3_key'),
            },
            "content_type": "text/html",
            "file_topic": "musinsa_magazine_news",
        },
        "schedule_interval": "@daily",
    },
    {
        "dag_id": "fetch_musinsa_magazine_lookbook_data_dag",
        "context_dict": {
            "first_url": "https://content.musinsa.com/api2/content/magazine-content/v1/lookbook",
            "second_url": "https://www.musinsa.com/mz/magazine/view/",
            "params": {
                "page": 1,
                "size": 20,
                "lookbookInclude": "true",
            },
            "headers": headers,
            "aws_config_dict": {
                'aws_access_key_id': Variable.get('aws_access_key_id'),
                'aws_secret_access_key': Variable.get('aws_secret_access_key'),
                'aws_region': Variable.get('aws_region'),
                's3_bucket_name': Variable.get('s3_bucket_name'),
                's3_key': Variable.get('s3_key'),
            },
            "content_type": "text/html",
            "file_topic": "musinsa_magazine_lookbook",
        },
        "schedule_interval": "@daily",
    },
]

# 템플릿을 사용하여 DAG 생성
for config in dags_config:
    try:
        dag_id = config["dag_id"]
        context_dict = config["context_dict"]
        schedule_interval = config["schedule_interval"]

        logging.info(f"Creating DAG: {dag_id}")

        # 필수 키 검증
        required_context_keys = ["first_url", "second_url", "params", "headers", "aws_config_dict", "content_type", "file_topic"]
        for key in required_context_keys:
            if key not in context_dict:
                raise ValueError(f"Missing required context key: {key} in {dag_id}")

        required_aws_keys = ["aws_access_key_id", "aws_secret_access_key", "aws_region", "s3_bucket_name", "s3_key"]
        for key in required_aws_keys:
            if key not in context_dict["aws_config_dict"]:
                raise ValueError(f"Missing required AWS config key: {key} in {dag_id}")

        # DAG 생성
        globals()[dag_id] = create_fetch_paged_data_dag(
            dag_id=dag_id,
            context_dict=context_dict,
            default_args=default_args,
            schedule_interval=schedule_interval
        )
        logging.info(f"Successfully created DAG: {dag_id}")

    except Exception as e:
        logging.error(f"Error creating DAG {dag_id}: {e}")
