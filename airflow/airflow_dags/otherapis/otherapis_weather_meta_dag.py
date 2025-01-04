from airflow import DAG
from airflow.models import Variable
from custom_operators.fetch_non_paged_data_operator import FetchNonPagedDataOperator
from datetime import datetime

# 이후 시연 때 email 설정을 True로 변경
default_args = {
    "owner": "gjstjd9509@gmail.com",
    "start_date": datetime(2023, 1, 1),
    "email": ["gjstjd9509@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
}

# DAG 정의 - 기상 관측소 메타 데이터는 오랜 기간 변경이 없을 것이므로 수동으로 트리거
with DAG(
    dag_id="fetch_weather_meta_data_dag",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:

    # API Key 설정
    API_KEY = Variable.get("weather_api_key")
    BASE_URL = r"https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"

    url = rf"{BASE_URL}?inf=SFC&help=1&authKey={API_KEY}"

    fetch_weather_station_data_task = FetchNonPagedDataOperator(
        task_id="fetch_weather_meta_data_task",
        url=url,
        file_topic="weather_station_data",
        content_type="plain/text",
    )

    fetch_weather_station_data_task
