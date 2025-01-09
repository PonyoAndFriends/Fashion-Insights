from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from custom_operators.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from custom_operators.calculate_page_range_operator import CalculatePageRangeOperator
from custom_operators.custom_modules.otherapis_dependencies import (
    MUSINSA_HEADERS,
    OTHERAPI_DEFAULT_ARGS,
    DEFAULT_S3_DICT,
)
from custom_operators.k8s_spark_job_submit_operator import SparkApplicationOperator
from custom_operators.custom_modules.s3_upload import make_s3_url
import math

# API 정보
url = r"https://content.musinsa.com/api2/content/snap/v1/rankings/DAILY"
headers = MUSINSA_HEADERS

# 대그 동작을 위한 기초 상수 정의
TOTAL_DATA_COUNT = 100
PARALLEL_POD_NUM = 2
PARALLEL_THREAD_NUM = 10
PAGE_SIZE = math.ceil(TOTAL_DATA_COUNT / (PARALLEL_POD_NUM * PARALLEL_THREAD_NUM))

# 파일 경로 설정
FILE_TOPIC = "musinsa_snap_story_ranking"
FILE_PATH = f"/{datetime.now().strftime('%Y-%m-%d')}/otherapis/{FILE_TOPIC}_raw_data/"

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
                    "s3_dict": DEFAULT_S3_DICT,
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

    fetch_snap_ranking_story_data_spark_submit_tasks = []
    for gender in ["MEN", "WOMEN"]:
        file_topic = f"musinsa_{gender}_ranking_story_group"
        file_path = (
            f"/{datetime.now().strftime('%Y-%m-%d')}/otherapis/{file_topic}_raw_data/"
        )
        spark_job_submit_task = SparkApplicationOperator(
            task_id=f"musinsa_snap_ranking_story_{gender}_submit_spark_job_task",
            name=f"musinsa_snap_ranking_stroy_{gender}_from_bronze_to_silver_data",
            main_application_file=r"otherapis/bronze_to_silver/musinsa_snap_ranking_story_to_silver.py",
            application_args=[
                make_s3_url(Variable.get("bronze_bucket"), file_path),
                make_s3_url(Variable.get("silver_bucket"), file_path),
                gender,
            ],
        )

    (
        calculate_page_range_task
        >> fetch_snap_ranking_story_data_tasks
        >> spark_job_submit_task
    )
