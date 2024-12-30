from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def create_fetch_non_paged_data_dag(
    dag_id,
    api_dict,
    s3_upload_dict,
    default_args,
    schedule_interval
):
    """페이지네이션 되지 않은 api로부터 데이터를 가져와 s3에 적재하는 DAG 템플릿"""
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2023, 12, 25),
        catchup=False
    ) as dag:

        def fetch_non_paged_data_and_upload_to_s3(**kwargs):
            from utils.api_utils import fetch_non_paged_data
            from utils.s3_utils import load_data_to_s3
            import json

            keys = ['base_url', 'key_url', 'params', 'headers']
            base_url, key_url, params, headers = [api_dict.get(key, None) for key in keys]
            response = fetch_non_paged_data(base_url, key_url, params, headers=headers)

            keys= ['aws_access_key_id', 'aws_secret_access_key', 'aws_region', 's3_bucket_name', 'file_path', 'content_type']
            aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name, file_path, content_type = [s3_upload_dict.get(key, None) for key in keys]

            if content_type == 'application/json':
                response = json.dumps(response, ensure_ascii=False)
            else:
                response = response.text

            load_data_to_s3(aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name, response, file_path, content_type)

        fetch_non_paged_data_and_upload_to_s3_task = PythonOperator(
            task_id='fetch_total_count_task',
            python_callable=fetch_non_paged_data_and_upload_to_s3,
        )

        fetch_non_paged_data_and_upload_to_s3_task

    return dag
