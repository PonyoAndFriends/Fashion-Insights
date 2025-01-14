from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from otherapis.custom_operators.custom_modules.otherapis_categories import (
    MALE_CATEGORY_LIST,
    FEMALE_CATEGORY_LIST,
)
from otherapis.custom_operators.custom_modules.otherapis_dependencies import (
    OTHERAPI_DEFAULT_ARGS,
    DEFAULT_S3_DICT,
    NAVER_HEADER,
    NAVER_HAEDER_2,
    OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH,
)
from otherapis.custom_operators.k8s_custom_python_pod_operator import (
    CustomKubernetesPodOperator,
)
from otherapis.custom_operators.k8s_spark_job_submit_operator import (
    submit_spark_application,
)
from airflow.operators.python import PythonOperator
from otherapis.custom_operators.custom_modules.s3_upload import (
    make_s3_url,
)

# Pod내에서 실행할 스레드의 최대 개수
MAX_THREAD = 10

# API 설정값
url = "https://openapi.naver.com/v1/datalab/shopping/category/keywords"

# 기본 DAG 설정
default_args = OTHERAPI_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="hs_keyword_trends_for_categories_dag",
    default_args=default_args,
    description="Fetch and save keywords trend data from naver api",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1) + timedelta(hours=9),
    tags=["otherapi", "keyword", "trends", "daily"],
    catchup=False,
) as dag:

    tasks_config = [
        {
            "gender": "여성",
            "category_list": FEMALE_CATEGORY_LIST,
            "task_gender": "female",
            "header": NAVER_HAEDER_2,
        },
        {
            "gender": "남성",
            "category_list": MALE_CATEGORY_LIST,
            "task_gender": "male",
            "header": NAVER_HEADER,
        },
    ]

    fetch_keyword_data_tasks = []
    spark_submit_tasks = []

    for task in tasks_config:
        gender_fetch_keyword_data_task = CustomKubernetesPodOperator(
            task_id=f"fetch_{task['task_gender']}_keyword_data_task",
            name=f"{task['task_gender']}_keyword_data_task",
            script_path=f"{OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH}/fetch_keyword_trend_data.py",
            required_args={
                "url": url,
                "headers": task["header"],
                "gender": task["gender"],
                "s3_dict": DEFAULT_S3_DICT,
            },
            cpu_limit="1000m",
            memory_limit="1Gi",
        )
        fetch_keyword_data_tasks.append(gender_fetch_keyword_data_task)

        now_string = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")
        bronze_file_path = (
            f"bronze/{now_string}/otherapis/{task['gender']}_keyword_trends/"
        )
        silver_file_path = (
            f"silver/{now_string}/otherapis/{task['gender']}_keyword_trends/"
        )
        spark_args = [
            make_s3_url(Variable.get("s3_bucket"), bronze_file_path),
            make_s3_url(Variable.get("s3_bucket"), silver_file_path),
            task["task_gender"],
        ]
        spark_job_submit_task = PythonOperator(
            task_id=f"naver_keywords_trend_{task['task_gender']}_submit_spark_job_task",
            python_callable=submit_spark_application,
            op_args=[
                f"naveer-keywords-trend-{task['task_gender']}-from-bronze-to-silver-data",
                r"otherapis/bronze_to_silver/naver_keyword_trend_to_silver.py",
                spark_args,
            ],
        )
        spark_submit_tasks.append(spark_job_submit_task)

    for fetch_task, spark_task in zip(fetch_keyword_data_tasks, spark_submit_tasks):
        fetch_task >> spark_task
