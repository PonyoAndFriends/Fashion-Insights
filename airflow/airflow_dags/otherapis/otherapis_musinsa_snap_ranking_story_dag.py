from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from custom_operators.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from custom_operators.calculate_page_range_operator import CalculatePageRangeOperator
from custom_operators.custom_modules.otherapis_dependencies import (
    MUSINSA_HEADERS,
    OTHERAPI_DEFAULT_ARGS,
)
import math

# API 정보
url = r"https://content.musinsa.com/api2/content/snap/v1/rankings/DAILY"
headers = MUSINSA_HEADERS

# 대그 동작을 위한 기초 상수 정의
TOTAL_DATA_COUNT = 100
PARALLEL_POD_NUM = 2
PARALLEL_THREAD_NUM = 10
PAGE_SIZE = math.ceil(TOTAL_DATA_COUNT / (PARALLEL_POD_NUM * PARALLEL_THREAD_NUM))

# other api 대그들
default_args = OTHERAPI_DEFAULT_ARGS

with DAG(
    dag_id="musinsa_snap_api_ranking_story_to_s3_pod_dag",
    default_args=default_args,
    description="Fetch snap ranking story data from Musinsa SNAP API using KubernetesPodOperator",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    tags=["otherapi", "musinsa", "SNAP", "Daily"],
    catchup=False,
) as dag:

    # 페이지 범위 계산
    calculate_page_range_task = CalculatePageRangeOperator(
        task_id="calculate_page_ranges_for_snap_ranking_story",
        total_count=TOTAL_DATA_COUNT,
        page_size=PAGE_SIZE,
        parallel_process_num=PARALLEL_POD_NUM * PARALLEL_THREAD_NUM,
    )

    # KubernetesPodOperator 기반 데이터 처리
    fetch_snap_ranking_story_data_tasks = []
    for gender in ["MEN", "WOMEN"]:
        for i in range(0, PARALLEL_POD_NUM * PARALLEL_THREAD_NUM, PARALLEL_THREAD_NUM):
            fetch_snap_ranking_story_data_task = CustomKubernetesPodOperator(
                task_id=f"fetch_{gender}_snap_ranking_story_task_group_{i // PARALLEL_THREAD_NUM + 1}",
                script_path="/app/python_script/fetch_and_load_paged_data_to_s3.py",
                required_args={
                    "url": url,
                    "page_ranges": "{{ task_instance.xcom_pull(task_ids='calculate_page_ranges_for_snap_ranking_story')[%d:%d] }}"
                    % (
                        i,
                        min(
                            i + PARALLEL_THREAD_NUM,
                            PARALLEL_POD_NUM * PARALLEL_THREAD_NUM,
                        ),
                    ),
                    "file_topic": f"musinsa_{gender}_ranking_story_group",
                    "s3_dict": {
                        "aws_access_key_id": Variable.get("aws_access_key_id"),
                        "aws_secret_access_key": Variable.get("aws_secret_access_key"),
                        "aws_region": Variable.get("aws_region"),
                        "s3_bucket_name": Variable.get("s3_bucket_name"),
                        "data_file": None,  # 동작 과정에서 생성
                        "file_path": None,  # 동작 과정에서 생성
                        "content_type": "application/json",
                    },
                    "pagenation_keyword": "page",
                },
                optional_args={
                    "headers": headers,
                    "params": {
                        "gender": gender,
                        "page": None,  # 동작 과정에서 생성
                        "size": PAGE_SIZE,
                        "style": "ALL",
                    },
                },
                cpu_limit="1000m",
                memory_limit="1Gi",
                cpu_request="500m",
                memory_request="512Mi",
            )
            fetch_snap_ranking_story_data_tasks.append(
                fetch_snap_ranking_story_data_task
            )

    calculate_page_range_task >> fetch_snap_ranking_story_data_tasks
