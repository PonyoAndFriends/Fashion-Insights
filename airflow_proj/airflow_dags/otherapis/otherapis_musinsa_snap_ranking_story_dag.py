from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from otherapis.custom_operators.k8s_custom_python_pod_operator import (
    CustomKubernetesPodOperator,
)
from otherapis.custom_operators.calculate_page_range_operator import (
    CalculatePageRangeOperator,
)
from otherapis.custom_operators.custom_modules.otherapis_dependencies import (
    MUSINSA_HEADERS,
    OTHERAPI_DEFAULT_ARGS,
    DEFAULT_S3_DICT,
    OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH,
)
from otherapis.custom_operators.k8s_spark_job_submit_operator import (
    submit_spark_application,
)
from otherapis.custom_operators.custom_modules.s3_upload import (
    make_s3_url,
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

# 파일 경로 설정
FILE_TOPIC = "musinsa_snap_story_ranking"
BRONZE_FILE_PATH = f"bronze/{(datetime.now() + timedelta(hours=9)).strftime('%Y-%m-%d')}/otherapis/{FILE_TOPIC}_raw_data/"
SILVER_FILE_PATH = f"silver/{(datetime.now() + timedelta(hours=9)).strftime('%Y-%m-%d')}/otherapis/{FILE_TOPIC}_raw_data/"

# other api 대그들
default_args = OTHERAPI_DEFAULT_ARGS

with DAG(
    dag_id="musinsa_snap_api_ranking_story_to_s3_pod_dag",
    default_args=default_args,
    description="Fetch snap ranking story data from Musinsa SNAP API using KubernetesPodOperator",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1) + timedelta(hours=9),
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
                script_path=f"{OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH}/fetch_and_load_paged_data_to_s3.py",
                required_args={
                    "url": url,
                    "page_ranges": "{{ task_instance.xcom_pull(task_ids='calculate_page_ranges_for_snap_ranking_story')[%d:%d] | tojson }}"
                    % (
                        i,
                        min(
                            i + PARALLEL_THREAD_NUM,
                            PARALLEL_POD_NUM * PARALLEL_THREAD_NUM,
                        ),
                    ),
                    "file_topic": f"musinsa_{gender}_ranking_story_group",
                    "s3_dict": DEFAULT_S3_DICT,
                    "pagination_keyword": "page",
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
            )
            fetch_snap_ranking_story_data_tasks.append(
                fetch_snap_ranking_story_data_task
            )

    fetch_complete_task = DummyOperator(
        task_id="fetch_complete_task",
    )

    fetch_snap_ranking_story_data_spark_submit_tasks = []
    gender_dict = {
        "남성": "MEN",
        "여성": "WOMEN",
    }
    for gender in ["남성", "여성"]:
        file_topic = f"musinsa_{gender_dict[gender]}_ranking_story_group"
        file_path = f"{(datetime.now() + timedelta(hours=9)).strftime('%Y-%m-%d')}/otherapis/{file_topic}_raw_data/"
        spark_args = [
            make_s3_url(Variable.get("s3_bucket"), "bronze/" + file_path),
            make_s3_url(Variable.get("s3_bucket"), "silver/" + file_path),
            gender,
        ]

        spark_job_submit_task = PythonOperator(
            task_id=f"musinsa_snap_ranking_story_{gender_dict[gender]}_submit_spark_job_task",
            python_callable=submit_spark_application,
            op_args=[
                f"snap-ranking-stroy-{gender_dict[gender].lower()}-silver-data",
                r"otherapis/bronze_to_silver/musinsa_snap_ranking_story_to_silver.py",
                spark_args,
            ],
        )
        fetch_snap_ranking_story_data_spark_submit_tasks.append(spark_job_submit_task)

    (
        calculate_page_range_task
        >> fetch_snap_ranking_story_data_tasks
        >> fetch_complete_task
        >> fetch_snap_ranking_story_data_spark_submit_tasks
    )
