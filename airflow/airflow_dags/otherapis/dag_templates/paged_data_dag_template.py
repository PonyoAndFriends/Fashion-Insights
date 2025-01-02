from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def create_fetch_paged_data_dag(
    dag_id: str,
    context_dict: dict,
    default_args: dict,
    schedule_interval: str
):
    """
    페이지네이션이 적용된 api로부터 데이터를 가져오는 DAG

    Args:
        dag_id (str): The DAG ID.
        context_dict (dict): Dictionary containing API and S3 configurations.
        default_args (dict): Default arguments for the DAG.
        schedule_interval (str): Schedule interval for the DAG.

    Returns:
        DAG: Configured Airflow DAG.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2023, 12, 25),
        catchup=False
    ) as dag:

        from utils.api_utils import fetch_paged_data

        # Extract context configuration
        required_keys = ['base_url', 'key_url', 'params', 'page_ranges', 'page_size', 'aws_config_dict', 'file_topic', 'content_type']
        optional_keys = ['headers']

        config = {key: context_dict.get(key) for key in required_keys + optional_keys}

        # Validate required keys
        for key in required_keys:
            if not config[key]:
                raise ValueError(f"Missing required configuration: {key}")

        fetch_paged_data_and_upload_s3_tasks = []

        # Create tasks for each page range
        for i, page_range in enumerate(config['page_ranges']):
            task_id = f"fetch_{config['file_topic']}_data_task_{i + 1}"

            fetch_paged_data_and_upload_s3_tasks.append(
                PythonOperator(
                    task_id=task_id,
                    python_callable=fetch_paged_data,
                    op_kwargs={
                        'base_url': config['base_url'],
                        'key_url': config['key_url'],
                        'params': config['params'],
                        'page_range': page_range,
                        'page_size': config['page_size'],
                        'aws_config_dict': config['aws_config_dict'],
                        'file_topic': config['file_topic'],
                        'content_type': config['content_type'],
                        'headers': config.get('headers')
                    },
                    dag=dag
                )
            )

        # Set task dependencies (optional if sequential execution required)
        for i in range(len(fetch_paged_data_and_upload_s3_tasks) - 1):
            fetch_paged_data_and_upload_s3_tasks[i] >> fetch_paged_data_and_upload_s3_tasks[i + 1]

        return dag
