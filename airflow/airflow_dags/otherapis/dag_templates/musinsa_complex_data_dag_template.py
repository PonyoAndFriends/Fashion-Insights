from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.api_utils import fetch_ids_to_search
from utils.s3_utils import load_data_to_s3

import boto3
import requests
import json

def create_fetch_data_dag(
    dag_id,
    context_dict,
    schedule_interval,
    start_date,
    default_args
):
    """Musinsa 데이터를 가져와 S3에 저장하는 DAG 템플릿"""
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
    ) as dag:

        fetch_ids_task = PythonOperator(
            task_id='fetch_ids_task',
            python_callable=fetch_ids_to_search,
            op_kwargs={
                'first_url': context_dict['first_url'],
                'params': context_dict['first_params'],
                'headers': context_dict['headers'],
            }
        )

        def fetch_details_of_ids(**kwargs):
            import datetime

            ids = list(kwargs['ti'])
            headers = context_dict['headers']
            for i, id in enumerate(ids):
                url = context_dict['second_url'] + str(id)
                res = requests.get(url, headers=headers)

                keys = ['aws_access_key_id', 'aws_secret_access_key', 'aws_region', 's3_bucket_name', 's3_key', 'content_type']
                aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name, s3_key, content_type = [context_dict.get(key, None) for key in keys]
                
                now = datetime.now()
                date_string = now.strftime("%Y-%m-%d")    
                file_topic = context_dict['file_topic']
                file_path = s3_key + f"/{file_topic}_raw_data/{date_string}/{file_topic}_{i}th.json"
                
                load_data_to_s3(aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name, res.text, file_path, content_type)
        
        fetch_details_of_ids_task = PythonOperator(
            task_id='fetch_details_of_ids_task',
            python_callable=fetch_details_of_ids,
        )

        fetch_ids_task >> fetch_details_of_ids_task
        
        return dag

