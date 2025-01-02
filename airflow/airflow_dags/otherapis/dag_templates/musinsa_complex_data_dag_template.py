from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.api_utils import fetch_ids_to_search
from utils.s3_utils import load_data_to_s3
import requests
import logging

def create_fetch_data_dag(
    dag_id: str,
    context_dict: dict,
    schedule_interval: str,
    start_date: datetime,
    default_args: dict
):
    """
    무신사 SNAP, MAGAZINE 관련 복합 api 호출 DAG

    Args:
        dag_id (str): DAG ID.
        context_dict (dict): Context dictionary with API and S3 configurations.
        schedule_interval (str): Schedule interval for the DAG.
        start_date (datetime): Start date for the DAG.
        default_args (dict): Default arguments for the DAG.

    Returns:
        DAG: Configured Airflow DAG.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
    ) as dag:

        # Task: Fetch IDs to search
        fetch_ids_task = PythonOperator(
            task_id='fetch_ids_task',
            python_callable=fetch_ids_to_search,
            op_kwargs={
                'first_url': context_dict['first_url'],
                'params': context_dict['first_params'],
                'headers': context_dict['headers'],
            },
        )

        def fetch_details_of_ids(**kwargs):
            """
            Fetch details for each ID and store in S3.
            """
            from airflow.models import TaskInstance

            ti: TaskInstance = kwargs['ti']
            ids = ti.xcom_pull(task_ids='fetch_ids_task')
            headers = context_dict['headers']

            for i, id in enumerate(ids):
                try:
                    url = f"{context_dict['second_url']}{id}"
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()

                    aws_keys = ['aws_access_key_id', 'aws_secret_access_key', 'aws_region', 's3_bucket_name', 's3_key', 'content_type']
                    aws_config = {key: context_dict[key] for key in aws_keys}

                    now = datetime.now()
                    date_string = now.strftime("%Y-%m-%d")
                    file_topic = context_dict['file_topic']
                    file_path = f"{aws_config['s3_key']}/{file_topic}_raw_data/{date_string}/{file_topic}_{i}.json"

                    load_data_to_s3(
                        aws_access_key_id=aws_config['aws_access_key_id'],
                        aws_secret_access_key=aws_config['aws_secret_access_key'],
                        aws_region=aws_config['aws_region'],
                        s3_bucket_name=aws_config['s3_bucket_name'],
                        data_file=response.text,
                        file_path=file_path,
                        content_type=aws_config['content_type']
                    )

                    logging.info(f"Successfully processed and uploaded ID: {id}")
                except requests.RequestException as e:
                    logging.error(f"Failed to fetch details for ID {id}: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error for ID {id}: {e}")

        # Task: Fetch details of IDs
        fetch_details_of_ids_task = PythonOperator(
            task_id='fetch_details_of_ids_task',
            python_callable=fetch_details_of_ids,
        )

        # Task dependencies
        fetch_ids_task >> fetch_details_of_ids_task

        return dag
