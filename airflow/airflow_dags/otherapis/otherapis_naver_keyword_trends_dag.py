from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from custom_operators.custom_modules.otherapis_categories import MALE_CATEGORY_LIST, FEMALE_CATEGORY_LIST
from custom_operators.custom_modules.otherapis_dependencies import OTHERAPI_DEFAULT_ARGS
from custom_operators.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from custom_operators.keyword_dictionary_process_oprerator import KeywordDictionaryMergeAndExtractOperator

# Pod내에서 실행할 스레드의 최대 개수
MAX_THREAD = 15

# API 설정값
url = "https://openapi.naver.com/v1/datalab/shopping/category/keywords"
headers = {
    "X-Naver-Client-Id": Variable.get("x-naver-client-id"),
    "X-Naver-Client-secret": Variable.get("x-naver-client-secret"),
    "Content-Type": "application/json",
}

# 기본 DAG 설정
default_args = OTHERAPI_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="keyword_trends_for_categories_dag",
    default_args=default_args,
    description="Fetch and save keywords trend data from naver api",
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    tags=["otherapi", "keyword", "trends", "daily"],
    catchup=False,
) as dag:

    tasks_config = [
        {"gender": "female", "category_list": FEMALE_CATEGORY_LIST},
        {"gender": "male", "category_list": MALE_CATEGORY_LIST},
    ]

    keyword_list_tasks = []
    fetch_keyword_data_tasks = []

    for task in tasks_config:
        gender_keywords_list_task = KeywordDictionaryMergeAndExtractOperator(
            task_id=f"making_{task['gender']}_keywords_list_task",
            dict_list=task["category_list"],
        )
        keyword_list_tasks.append(gender_keywords_list_task)

        gender_fetch_keyword_data_task = CustomKubernetesPodOperator(
            task_id=f"fetch_{task['gender']}_keyword_data_task",
            name=f"{task['gender']}_keyword_data_task",
            script_path="/app/python_script/fetch_keyword_trend_data.py",
            required_args={
                "url": url,
                "headers": headers,
                "category_list": f"{{{{ task_instance.xcom_pull(task_ids='making_{task['gender']}_keywords_list_task') }}}}",
                "max_threads": MAX_THREAD,
                "s3_dict": {
                    "aws_access_key_id": Variable.get("aws_access_key_id"),
                    "aws_secret_access_key": Variable.get("aws_secret_access_key"),
                    "aws_region": Variable.get("aws_region"),
                    "s3_bucket_name": Variable.get("s3_bucket_name"),
                    "data_file": None,
                    "file_path": None,
                    "content_type": "application/json",
                },
            },
            cpu_limit="1000m",
            memory_limit="1Gi",
            cpu_request="500m",
            memory_request="512Mi",
            delete_operator_pod=True,
            get_logs=True,
        )
        fetch_keyword_data_tasks.append(gender_fetch_keyword_data_task)

    for list_task, fetch_task in zip(keyword_list_tasks, fetch_keyword_data_tasks):
        list_task >> fetch_task
