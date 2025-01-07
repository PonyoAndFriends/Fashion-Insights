from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from custom_operators.custom_modules.otherapis_categories import (
    MALE_CATEGORY_LIST,
    FEMALE_CATEGORY_LIST,
)
from custom_operators.custom_modules.otherapis_dependencies import (
    OTHERAPI_DEFAULT_ARGS,
    DEFAULT_S3_DICT,
    NAVER_HEADER,
)
from custom_operators.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from custom_operators.keyword_dictionary_process_oprerator import (
    CategoryDictionaryMergeAndExplodeOperator,
)
from custom_operators.k8s_spark_job_submit_operator import SparkApplicationOperator
from custom_operators.custom_modules.s3_upload import make_s3_url

# Pod내에서 실행할 스레드의 최대 개수
MAX_THREAD = 10

# API 설정값
url = "https://openapi.naver.com/v1/datalab/shopping/category/keywords"
headers = NAVER_HEADER

# 기본 DAG 설정
default_args = OTHERAPI_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="keyword_trends_for_categories_dag",
    default_args=default_args,
    description="Fetch and save keywords trend data from naver api",
    schedule_interval="@daily",
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
    spark_submit_tasks = []

    for task in tasks_config:
        gender_keywords_list_task = CategoryDictionaryMergeAndExplodeOperator(
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
                "all_keywords": f"{{{{ task_instance.xcom_pull(task_ids='making_{task['gender']}_keywords_list_task') }}}}",
                "s3_dict": DEFAULT_S3_DICT,
            },
            cpu_limit="1000m",
            memory_limit="1Gi",
            cpu_request="500m",
            memory_request="512Mi",
            delete_operator_pod=True,
            get_logs=True,
        )
        fetch_keyword_data_tasks.append(gender_fetch_keyword_data_task)

        file_path = f"/{datetime.now().strftime("%Y-%m-%d")}/otherapis/{task['gender']}_keyword_trends/"
        spark_job_submit_task = SparkApplicationOperator(
            task_id=f"naver_keywords_trend_{task['gender']}_submit_spark_job_task",
            name=f"naveer_keywords_trend_{task['gender']}_from_bronze_to_silver_data",
            main_application_file=r"otherapis\bronze_to_silver\naver_keyword_trend_to_silver.py",
            application_args=[make_s3_url(Variable.get("bronze_bucket"), file_path), make_s3_url(Variable.get("silver_bucket"), file_path)],
        )
        spark_job_submit_task.append(spark_job_submit_task)

    for list_task, fetch_task, spark_task in zip(keyword_list_tasks, fetch_keyword_data_tasks, spark_submit_tasks):
        list_task >> fetch_task >> spark_task
