from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def create_fetch_paged_data_dag(
    dag_id,
    context_dict,
    default_args,
    schedule_interval
):
    """페이지네이션이 적용된 api로부터 데이터를 가져와 s3에 적재하는 대그를 작성하는 템플릿"""
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2023, 12, 25),
        catchup=False
    ) as dag:

        from utils.api_utils import fetch_paged_data
        
        keys = ['base_url', 'key_url', 'params', 'page_ranges', 'page_size', 'aws_config_dict', 'file_topic', 'content_type', 'headers']
        base_url, key_url, params, page_ranges, page_size, aws_config_dict, file_topic, content_type, headers = [context_dict.get(key, None) for key in keys]

        fetch_paged_data_and_upload_s3_tasks = []
        for i, page_range in enumerate(page_ranges):
            fetch_paged_data_and_upload_s3_tasks.append(
                PythonOperator(
                    task_id=f'fetch_{file_topic}_data_task_{i + 1}',
                    python_callable=fetch_paged_data,
                    op_kwargs={
                        'base_url': base_url,
                        'key_url': key_url,
                        'params': params,
                        'page_range': page_range,
                        'page_size': page_size,
                        'aws_config_dict': aws_config_dict,
                        'file_topic': file_topic,
                        'content_type': content_type,
                        'headers': headers
                    },
                    dag=dag
                )
            )

        fetch_paged_data_and_upload_s3_tasks

    return dag
