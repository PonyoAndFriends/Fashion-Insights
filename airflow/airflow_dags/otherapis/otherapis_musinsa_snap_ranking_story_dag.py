from airflow import DAG
from datetime import datetime, timedelta
from custom_operators.fetch_paged_data_operator import FetchPagedDataOperator
from custom_operators.calculate_page_range_operator import CalculatePageRangeOperator

# API 정보
url = r"https://content.musinsa.com/api2/content/snap/v1/rankings/DAILY"
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

# 이후 시연 때 email 설정을 True로 변경
default_args = {
    'owner': 'gjstjd9509@gmail.com',
    'start_date': datetime(2023, 1, 1),
    'email': ['gjstjd9509@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

with DAG(
    dag_id='musinsa_snap_api_ranking_story_to_s3_dag',
    default_args=default_args,
    description='Fetch snap ranking story data from Musinsa SNAP API and save to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    # 총 100개의 데이터를 병렬 태스크로 나누어 가져오기 위해 page_range 리스트를 계산
    calculate_page_range_task = CalculatePageRangeOperator(
        task_id="calculate_page_ranges_for_snap_ranking_story",
        total_count=100,
        page_size=PAGE_SIZE,
        parallel_task_num=PARALLEL_TASK_NUM,
    )

    # 남성, 여성에 대해 데이터를 가져올 태스크를 병렬로 구성
    fetch_tasks = []
    for gender in ['MEN', 'WOMEN']:
        for i in range(PARALLEL_TASK_NUM):
            fetch_task = FetchPagedDataOperator(
                task_id=f"fetch_{gender}_ranking_task_{i + 1}",
                url=url,
                params={
                    "gender": gender,
                    "page": None,
                    "size": PAGE_SIZE,
                    "style": "ALL"
                },
                headers=headers,
                file_topic=f"musinsa_{gender}_ranking",
                content_type="application/json",
                page_range="{{ task_instance.xcom_pull(task_ids='calculate_page_ranges_snap_ranking_story')[%d] }}" % i,
            )
            fetch_tasks.append(fetch_task)

    calculate_page_range_task >> fetch_tasks
