from airflow.models import Variable
from datetime import timedelta
from dag_templates.last_key_dag_template import create_last_key_dag_template

# 기본 DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dags_config = [
    {
        "dag_id": "fetch_poulation_data_dag",
        'context_dict': {
            'first_url': r"https://infuser.odcloud.kr/oas/docs?namespace=15097972/v1",
            'second_url': r"https://api.odcloud.kr/api",
            'params': {
                "page": 1,
                "perPage": 1,
                "returnType": "JSON",
                "serviceKey": Variable.get('popluation_service_key'),
            },
            'page_size': 20,
            'parallel_task_num': 6,
            'file_topic': 'population_api',
            'aws_config_dict': {
                "aws_access_key_id": Variable.get('aws_access_key_id'),
                "aws_secret_access_key": Variable.get('aws_secret_access_key'),
                "aws_region": Variable.get('aws_region'),
                "s3_bucket_name": Variable.get('s3_bucket_name'),
                "s3_key": Variable.get('s3_key'),
            }
        },
        "schedule_interval": "@daily",
    },
]

# DAG 생성
for config in dags_config:
    globals()[config["dag_id"]] = create_last_key_dag_template(
        dag_id=config["dag_id"],
        context_dict=config['context_dict'],
        default_args=default_args,
        schedule_interval=config["schedule_interval"]
    )
