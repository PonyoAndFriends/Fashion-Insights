from airflow import DAG
from airflow.models import Variable
from custom_operators.fetch_non_paged_data_operator import FetchNonPagedDataOperator
from custom_operators.custom_modules.otherapis_dependencies import OTHERAPI_DEFAULT_ARGS
from datetime import datetime

default_args = OTHERAPI_DEFAULT_ARGS

# DAG 정의 - 기상 관측소 메타 데이터는 오랜 기간 변경이 없을 것이므로 수동으로 트리거
with DAG(
    dag_id="fetch_weather_meta_data_dag",
    default_args=default_args,
    description="Fetch korean weather station metadata from open api and load to s3 bucket",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    tags=["otherapi", "weather", "station", "openAPI", "Daily"],
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
