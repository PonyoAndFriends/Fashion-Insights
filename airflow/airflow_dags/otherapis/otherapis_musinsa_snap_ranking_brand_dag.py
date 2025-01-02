from airflow import DAG
from datetime import datetime, timedelta
from custom_operators.fetch_paged_data_operator import FetchPagedDataOperator
from custom_operators.calculate_page_range_operator import CalculatePageRangeOperator

# API 정보
url = r"https://content.musinsa.com/api2/content/snap/v1/profile-rankings/BRAND/DAILY"
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

# 병렬 실행할 TASK 개수
PARALLEL_TASK_NUM = 4
PAGE_SIZE = 25

# Airflow 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='musinsa_snap_api_brand_ranking_to_s3_dag',
    default_args=default_args,
    description='Fetch snap brand ranking data from Musinsa SNAP API and save to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    calculate_page_range_task = CalculatePageRangeOperator(
        task_id="calculate_page_ranges_snap_brand_ranking",
        total_count=100,
        page_size=PAGE_SIZE,
        parallel_task_num=PARALLEL_TASK_NUM,
    )

    fetch_snap_ranking_brand_data_tasks = []
    for i in range(PARALLEL_TASK_NUM):
        fetch_task = FetchPagedDataOperator(
            task_id=f"fetch_musinsa_snap_brand_ranking_task_{i}",
            url=url,
            params={
                "page": 0,
                "size": PAGE_SIZE,
            },
            headers=headers,
            file_topic=f"musinsa_snap_brand_ranking",
            content_type="application/json",
            page_range="{{ task_instance.xcom_pull(task_ids='calculate_page_ranges_snap_brand_ranking')[%d] }}" % i,
        )
        fetch_snap_ranking_brand_data_tasks.append(fetch_task)

    calculate_page_range_task >> fetch_snap_ranking_brand_data_tasks
