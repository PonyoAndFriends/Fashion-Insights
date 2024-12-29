# sample
from datetime import datetime, timedelta
from utils.otherapis_dag_template import *

# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

# API 설정
api_configs = [
    {
        "dag_id": "fetch_users_data",
        "api_url": "https://api.example.com/users",
        "schedule": "@daily",
        "params": {"status": "active"},
    },
    {
        "dag_id": "fetch_orders_data",
        "api_url": "https://api.example.com/orders",
        "schedule": "@hourly",
        "params": {"status": "pending"},
    },
]

# DAG 인스턴스 생성
for config in api_configs:
    dag_id = config["dag_id"]
    api_url = config["api_url"]
    schedule = config["schedule"]
    params = config["params"]
    
    globals()[dag_id] = create_api_dag(
        dag_id=dag_id,
        api_url=api_url,
        default_args=default_args,
        schedule=schedule,
        params=params,
    )
