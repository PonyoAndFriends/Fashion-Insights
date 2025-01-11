from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from otherapis.custom_operators.custom_modules.otherapis_categories import (
    MALE_CATEGORY_LIST,
    FEMALE_CATEGORY_LIST,
)
from otherapis.custom_operators.custom_modules.otherapis_dependencies import (
    OTHERAPI_DEFAULT_ARGS,
    OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH,
    DEFAULT_S3_DICT,
)
from otherapis.custom_operators.k8s_spark_job_submit_operator import (
    SparkApplicationOperator,
)
from otherapis.custom_operators.k8s_custom_python_pod_operator import (
    CustomKubernetesPodOperator,
)
from otherapis.custom_operators.keyword_dictionary_process_oprerator import (
    CategoryDictionaryMergeAndExplodeOperator,
)
from otherapis.custom_operators.custom_modules.s3_upload import (
    make_s3_url,
)

# Pod내에서 실행할 스레드의 최대 개수
MAX_THREAD = 10

# API 설정값
youtube_api_key = Variable.get("youtube_api_key")

# 기본 DAG 설정
default_args = OTHERAPI_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="fetch_fashion_categories_yotubue_video_dag",
    default_args=default_args,
    description="Fetch and save Youtube videos and data from youtube api",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1) + timedelta(hours=9),
    tags=["otherapi", "youtube", "category", "daily"],
    catchup=False,
) as dag:

    tasks_config = [
        {
            "gender": "여성",
            "category_list": FEMALE_CATEGORY_LIST,
            "task_gender": "female",
        },
        {"gender": "남성", "category_list": MALE_CATEGORY_LIST, "task_gender": "male"},
    ]

    category_list_tasks = []
    fetch_keyword_data_tasks = []
    spark_submit_tasks = []

    for task in tasks_config:
        gender_category_list_task = CategoryDictionaryMergeAndExplodeOperator(
            task_id=f"making_{task['task_gender']}_category_list_task",
            dict_list=task["category_list"],
        )
        category_list_tasks.append(gender_category_list_task)

        gender_fetch_youtube_data_task = CustomKubernetesPodOperator(
            task_id=f"fetch_{task['task_gender']}_yotubue_data_task",
            name=f"pod_for_{task['task_gender']}_category_yotubue_data_task",
            script_path=f"{OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH}/fetch_and_load_youtube_data_to_s3.py",
            required_args={
                "youtube_api_key": youtube_api_key,
                "category_list": "{{ task_instance.xcom_pull(task_ids='making_"
                + task["task_gender"]
                + "_category_list_task') | tojson }}",
                "max_threads": MAX_THREAD,
                "s3_dict": DEFAULT_S3_DICT,
                "file_topic": "youtoube_videos_by_categories",
            },
            cpu_limit="1000m",
            memory_limit="1Gi",
        )
        fetch_keyword_data_tasks.append(gender_fetch_youtube_data_task)

        file_topic = "youtoube_videos_by_categories"
        now_string = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")
        bronze_file_path = f"bronze/{now_string}/otherapis/{task['task_gender']}_{file_topic}_raw_data/"
        silver_file_path = f"silver/{now_string}/otherapis/{task['task_gender']}_{file_topic}_raw_data/"
        spark_job_submit_task = SparkApplicationOperator(
            task_id=f"youtube_category_videos_{task['task_gender']}_submit_spark_job_task",
            name=f"youtube_category_videos_{task['task_gender']}_from_bronze_to_silver_data",
            main_application_file=r"otherapis/bronze_to_silver/youtube_data_to_silver.py",
            application_args=[
                make_s3_url(Variable.get("s3_bucket"), bronze_file_path),
                make_s3_url(Variable.get("s3_bucket"), silver_file_path),
                task["task_gender"],
            ],
        )
        spark_submit_tasks.append(spark_job_submit_task)

    for list_task, fetch_task, spark_submit_task in zip(
        category_list_tasks, fetch_keyword_data_tasks, spark_submit_tasks
    ):
        list_task >> fetch_task >> spark_submit_task
