from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

def create_last_key_dag_template(
    dag_id,
    context_dict,
    schedule_interval,
    default_args
):
    """Fetch data from an API and store in S3 DAG template."""
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2023, 12, 25),
        catchup=False,
    ) as dag:

        def fetch_last_key_and_calculate_page_range(**kwargs):
            from utils.api_utils import fetch_non_paged_data
            from utils.api_utils import calculate_page_range
            import requests
            import math
            
            response = fetch_non_paged_data(context_dict['first_url'], '', context_dict['params'], headers=None)
            last_key = list(response.json()['paths'])[-1]
            
            url = context_dict['second_url'] + last_key
            params = context_dict['params']
            params['page'] = 1
            params['perpage'] = 1
            
            total_count = requests.get(url, params=params).json()['totalCount']
            total_pages = math.ceil(total_count / context_dict['page_size'])

            return calculate_page_range(total_count, total_pages, context_dict['page_size'], context_dict['parallel_task_num'])

        fetch_last_key_and_calculate_page_range_task = PythonOperator(
            task_id=f'fetch_last_key_and_calculated_page_range_of_{context_dict['file_topic']}',
            python_callable=fetch_last_key_and_calculate_page_range,
        )

        def import_and_call_fetch_paged_data_func(second_url, key_url, params, page_range, page_size, aws_config_dict, file_topic, content_type, **kwargs):
            from utils.api_utils import fetch_paged_data
            fetch_paged_data(second_url, key_url, params, page_range, page_size, aws_config_dict, file_topic, content_type)


        fetch_data_tasks = []
        for i in range(context_dict['parallel_task_num']):
            fetch_data_tasks.append(
                PythonOperator(
                    task_id=f'fetch_data_task_{i + 1}',
                    python_callable=import_and_call_fetch_paged_data_func,
                    op_kwargs={
                        "second_url": context_dict['second_url'],
                        "key_url": context_dict['key_url'],
                        "params": context_dict['params'],
                        "page_range": "{{ task_instance.xcom_pull(task_ids='fetch_last_key_and_calculated_page_range_of_" + context_dict["file_topic"] + "')['page_ranges'][" + str(i) + "] }}",
                        "aws_config_dict": context_dict['aws_config_dict'],
                        "file_topic": context_dict['file_topic'],
                        "content_type": "application/json",
                    },
                )
            )


        fetch_last_key_and_calculate_page_range_task >> fetch_data_tasks 
        return dag
