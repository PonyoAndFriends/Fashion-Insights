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
)
from custom_operators.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from custom_operators.keyword_dictionary_process_oprerator import (
    CategoryDictionaryMergeAndExtractOperator,
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
    start_date=datetime(2024, 1, 1),
    tags=["otherapi", "youtube", "category", "daily"],
    catchup=False,
) as dag:

    tasks_config = [
        {"gender": "female", "category_list": FEMALE_CATEGORY_LIST},
        {"gender": "male", "category_list": MALE_CATEGORY_LIST},
    ]

    category_list_tasks = []
    fetch_keyword_data_tasks = []

    for task in tasks_config:
        gender_category_list_task = CategoryDictionaryMergeAndExtractOperator(
            task_id=f"making_{task['gender']}_category_list_task",
            dict_list=task["category_list"],
        )
        category_list_tasks.append(gender_category_list_task)

        gender_fetch_youtube_data_task = CustomKubernetesPodOperator(
            task_id=f"fetch_{task['gender']}_yotubue_data_task",
            name=f"pod_for_{task['gender']}_category_yotubue_data_task",
            script_path="/app/python_script/fetch_and_load_youtube_data_to_s3.py",
            required_args={
                "youtube_api_key": youtube_api_key,
                "category_list": f"{{{{ task_instance.xcom_pull(task_ids='making_{task['gender']}_category_list_task') }}}}",
                "max_threads": MAX_THREAD,
                "s3_dict": DEFAULT_S3_DICT,
                "file_topic": "youtoube_videos_by_categories",
            },
            cpu_limit="1000m",
            memory_limit="1Gi",
            cpu_request="500m",
            memory_request="512Mi",
            delete_operator_pod=True,
            get_logs=True,
        )
        fetch_keyword_data_tasks.append(gender_fetch_youtube_data_task)

    for list_task, fetch_task in zip(category_list_tasks, fetch_keyword_data_tasks):
        list_task >> fetch_task
