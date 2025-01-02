from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def create_last_key_dag_template(
    dag_id: str,
    context_dict: dict,
    schedule_interval: str,
    default_args: dict
):
    """
    Airflow DAG Template: 별도의 키를 불러오는 api가 있는 DAG용 템플릿.

    Args:
        dag_id (str): DAG ID.
        context_dict (dict): Context dictionary with API and S3 configuration.
        schedule_interval (str): Schedule interval for the DAG.
        default_args (dict): Default arguments for the DAG.

    Returns:
        DAG: Configured Airflow DAG.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2023, 12, 25),
        catchup=False,
    ) as dag:

        def fetch_last_key_and_calculate_page_range(**kwargs):
            """
            Fetch the last key from the API and calculate the page range.
            """
            from utils.api_utils import fetch_non_paged_data, calculate_page_range
            import math

            logging.info("Fetching the last key and calculating page range...")
            response = fetch_non_paged_data(
                context_dict['first_url'],
                '',
                context_dict['params'],
                headers=None
            )
            last_key = list(response.json().get('paths', []))[-1]

            url = f"{context_dict['second_url']}{last_key}"
            params = context_dict['params']
            params['page'] = 1
            params['perpage'] = 1

            total_count = requests.get(url, params=params).json().get('totalCount', 0)
            total_pages = math.ceil(total_count / context_dict['page_size'])

            return calculate_page_range(
                total_count, total_pages, context_dict['page_size'], context_dict['parallel_task_num']
            )

        fetch_last_key_and_calculate_page_range_task = PythonOperator(
            task_id=f"fetch_last_key_and_calculate_page_range_{context_dict['file_topic']}",
            python_callable=fetch_last_key_and_calculate_page_range,
        )

        def import_and_call_fetch_paged_data_func(**kwargs):
            """
            Import and call the fetch_paged_data function.
            """
            from utils.api_utils import fetch_paged_data
            
            page_range = kwargs['page_range']
            fetch_paged_data(
                kwargs['second_url'],
                kwargs['key_url'],
                kwargs['params'],
                page_range,
                kwargs['page_size'],
                kwargs['aws_config_dict'],
                kwargs['file_topic'],
                kwargs['content_type']
            )

        fetch_data_tasks = []
        for i in range(context_dict['parallel_task_num']):
            fetch_data_tasks.append(
                PythonOperator(
                    task_id=f"fetch_data_task_{i + 1}",
                    python_callable=import_and_call_fetch_paged_data_func,
                    op_kwargs={
                        "second_url": context_dict['second_url'],
                        "key_url": context_dict['key_url'],
                        "params": context_dict['params'],
                        "page_range": f"{{{{ task_instance.xcom_pull(task_ids='fetch_last_key_and_calculate_page_range_{context_dict['file_topic']}')['page_ranges'][{i}] }}}}",
                        "page_size": context_dict['page_size'],
                        "aws_config_dict": context_dict['aws_config_dict'],
                        "file_topic": context_dict['file_topic'],
                        "content_type": "application/json",
                    },
                )
            )

        fetch_last_key_and_calculate_page_range_task >> fetch_data_tasks
        return dag
