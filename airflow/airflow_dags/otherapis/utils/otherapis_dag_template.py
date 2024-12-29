# sample
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# API 호출 함수
def fetch_paginated_data(api_url, params, **kwargs):
    page = kwargs.get('page', 1)
    print(f"Fetching page {page} from {api_url}...")
    
    response = requests.get(api_url, params={**params, "page": page})
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page {page}: {response.text}")
    
    data = response.json()
    if "results" in data:
        print(f"Fetched {len(data['results'])} items on page {page}.")
    else:
        print(f"No results on page {page}.")
    
    return data

# 데이터 처리 함수
def process_page_data(data, **kwargs):
    print(f"Processing data: {data}")

# DAG 생성 템플릿
def create_api_dag(dag_id, api_url, default_args, schedule, params):
    def generate_fetch_tasks(dag):
        # 기본적으로 1~10페이지를 처리하는 작업 생성
        tasks = []
        for page in range(1, 11):
            fetch_task = PythonOperator(
                task_id=f"fetch_page_{page}",
                python_callable=fetch_paginated_data,
                op_kwargs={"api_url": api_url, "params": params, "page": page},
                provide_context=True,
                dag=dag,
            )
            process_task = PythonOperator(
                task_id=f"process_page_{page}",
                python_callable=process_page_data,
                op_kwargs={"data": f"{{{{ task_instance.xcom_pull(task_ids='fetch_page_{page}') }}}}"},
                provide_context=True,
                dag=dag,
            )
            fetch_task >> process_task
            tasks.append(fetch_task)
        return tasks

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
    ) as dag:
        fetch_tasks = generate_fetch_tasks(dag)
        return dag
