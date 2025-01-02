from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def create_fetch_non_paged_data_dag(
    dag_id: str,
    api_dict: dict,
    s3_upload_dict: dict,
    default_args: dict,
    schedule_interval: str
):
    """
    페이지네이션으로 구분되지 않은 단순 api로 부터 데이터를 가져오는 DAG

    Args:
        dag_id (str): DAG ID.
        api_dict (dict): API configuration dictionary.
        s3_upload_dict (dict): S3 upload configuration dictionary.
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
        catchup=False,
    ) as dag:

        def fetch_non_paged_data_and_upload_to_s3(**kwargs):
            """
            Fetch non-paginated data from an API and upload it to S3.
            """
            from utils.api_utils import fetch_non_paged_data
            from utils.s3_utils import load_data_to_s3
            import json

            logging.info("Starting to fetch non-paged data and upload to S3...")

            # Extract API configuration
            api_keys = ['base_url', 'key_url', 'params', 'headers']
            base_url, key_url, params, headers = [api_dict.get(key) for key in api_keys]

            # Fetch data from API
            response = fetch_non_paged_data(base_url, key_url, params, headers=headers)
            logging.info("Data fetched from API successfully.")

            # Extract S3 configuration
            s3_keys = ['aws_access_key_id', 'aws_secret_access_key', 'aws_region', 's3_bucket_name', 'file_path', 'content_type']
            aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name, file_path, content_type = [
                s3_upload_dict.get(key) for key in s3_keys
            ]

            # Prepare data for upload
            if content_type == 'application/json':
                data = json.dumps(response.json(), ensure_ascii=False)
            else:
                data = response.text

            # Upload data to S3
            load_data_to_s3(
                aws_access_key_id,
                aws_secret_access_key,
                aws_region,
                s3_bucket_name,
                data,
                file_path,
                content_type
            )
            logging.info(f"Data uploaded to S3: {file_path}")

        # Task: Fetch and upload data
        fetch_non_paged_data_and_upload_to_s3_task = PythonOperator(
            task_id='fetch_non_paged_data_and_upload_to_s3_task',
            python_callable=fetch_non_paged_data_and_upload_to_s3,
        )

        fetch_non_paged_data_and_upload_to_s3_task

    return dag
