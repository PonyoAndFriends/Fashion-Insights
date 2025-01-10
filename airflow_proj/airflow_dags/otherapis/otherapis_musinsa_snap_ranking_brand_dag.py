from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow_proj.airflow_dags.otherapis.custom_operators.k8s_spark_job_submit_operator import SparkApplicationOperator
from airflow_proj.airflow_dags.otherapis.custom_operators.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from airflow_proj.airflow_dags.otherapis.custom_operators.calculate_page_range_operator import CalculatePageRangeOperator
from airflow_proj.airflow_dags.otherapis.custom_operators.custom_modules.otherapis_dependencies import (
    MUSINSA_HEADERS,
    OTHERAPI_DEFAULT_ARGS,
    DEFAULT_S3_DICT,
    OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH,
)
from zoneinfo import ZoneInfo
from airflow_proj.airflow_dags.otherapis.custom_operators.custom_modules.s3_upload import make_s3_url

import math

# API 정보
url = r"https://content.musinsa.com/api2/content/snap/v1/profile-rankings/BRAND/DAILY"
headers = MUSINSA_HEADERS

# 대그 동작을 위한 기초 상수 정의
TOTAL_DATA_COUNT = 100
PARALLEL_POD_NUM = 2
PARALLEL_THREAD_NUM = 10
PAGE_SIZE = math.ceil(TOTAL_DATA_COUNT // (PARALLEL_POD_NUM * PARALLEL_THREAD_NUM))

# 파일 경로 설정
FILE_TOPIC = "musinsa_snap_brand_ranking"
BRONZE_FILE_PATH = f"bronze/{datetime.now().astimezone(ZoneInfo('Asia/Seoul')).strftime('%Y-%m-%d')}/otherapis/{FILE_TOPIC}_raw_data/"
SILVER_FILE_PATH = f"silver/{datetime.now().astimezone(ZoneInfo('Asia/Seoul')).strftime('%Y-%m-%d')}/otherapis/{FILE_TOPIC}_raw_data/"

# DAG의 기본 args 정의
default_args = OTHERAPI_DEFAULT_ARGS

with DAG(
    dag_id="musinsa_snap_api_brand_ranking_to_s3_pod_dag",
    default_args=default_args,
    description="Fetch snap brand ranking data from Musinsa SNAP API and save to S3",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1).astimezone(ZoneInfo("Asia/Seoul")),
    tags=["otherapi", "musinsa", "SNAP", "Daily"],
    catchup=False,
) as dag:

    # 100개의 데이터를 정해진 수의 pod의 각 스레드에 할당하기 위해 page_range 리스트를 계산
    calculate_page_range_task = CalculatePageRangeOperator(
        task_id="calculate_page_ranges_for_snap_brand_ranking",
        total_count=100,
        page_size=PAGE_SIZE,
        parallel_process_num=PARALLEL_POD_NUM * PARALLEL_THREAD_NUM,
    )

    # 슬라이싱된 page_ranges를 처리할 KubernetesPodFetchDataOperator 생성
    fetch_snap_ranking_brand_data_tasks = []

    # 파드 수 * 스레드 수 = 총 나누어 돌아갈 api 호출 및 s3 적재 함수의 실제 실행의 수(=반환될 page_ranges의 길이)
    # 스레드의 수 만큼 하나의 파드 내로 분배
    for i in range(0, PARALLEL_POD_NUM * PARALLEL_THREAD_NUM, PARALLEL_THREAD_NUM):
        fetch_snap_ranking_brand_data_task = CustomKubernetesPodOperator(
            task_id=f"fetch_musinsa_snap_brand_ranking_task_group_{i // PARALLEL_THREAD_NUM + 1}",
            script_path=f"{OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH}/fetch_and_load_paged_data_to_s3.py",
            required_args={
                "url": url,
                "page_ranges": "{{ task_instance.xcom_pull(task_ids='calculate_page_ranges_for_snap_brand_ranking')[%d:%d] }}"
                % (
                    i,
                    min(
                        i + PARALLEL_THREAD_NUM, PARALLEL_POD_NUM * PARALLEL_THREAD_NUM
                    ),
                ),
                "file_topic": FILE_TOPIC,
                "s3_dict": DEFAULT_S3_DICT,
                "pagination_keyword": "page",
            },
            optional_args={
                "headers": headers,
                "params": {"page": None, "size": PAGE_SIZE},
            },
            cpu_limit="1000m",
            memory_limit="1Gi",
            cpu_request="500m",
            memory_request="512Mi",
        )
        fetch_snap_ranking_brand_data_tasks.append(fetch_snap_ranking_brand_data_task)

    spark_job_submit_task = SparkApplicationOperator(
        task_id="musinsa_snap_ranking_brand_submit_spark_job_task",
        name="musinsa_snap_ranking_brand_from_bronze_to_silver_data",
        main_application_file=r"otherapis/bronze_to_silver/musinsa_snap_brand_ranking_to_silver.py",
        application_args=[
            make_s3_url(Variable("s3_bucket"), BRONZE_FILE_PATH),
            make_s3_url(Variable("s3_bucket"), SILVER_FILE_PATH),
        ],
    )

    (
        calculate_page_range_task
        >> fetch_snap_ranking_brand_data_tasks
        >> spark_job_submit_task
    )
