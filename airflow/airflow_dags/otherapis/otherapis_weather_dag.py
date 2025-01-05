from airflow import DAG
from airflow.models import Variable
from custom_operators.fetch_non_paged_data_operator import FetchNonPagedDataOperator
from datetime import datetime, timedelta
from custom_operators.custom_modules.otherapis_dependencies import OTHERAPI_DEFAULT_ARGS

default_args = OTHERAPI_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="fetch_weekly_weather_data_dag",
    default_args=default_args,
    description="Fetch weekly weather data from open api and load to s3 bucket",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    tags=["otherapi", "weather", "openAPI", "Daily"],
    catchup=False,
) as dag:

    # API 관련 기본 설정
    api_key = Variable.get("weather_api_key")
    url = r"https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd.php"

    now = datetime.now()
    one_day = timedelta(days=1)

    # 일별 최저 기온, 최대 기온, 날씨 개황(흐림, 맑음 등)을 포함한 날씨 데이터를 오늘로부터 과거 1주일 날짜만큼 적재
    weather_fetch_task_list = []
    for i in range(7):
        now_string = now.strftime("%Y%m%d")
        url = rf"{url}?tm={now_string}&help=1&authKey={api_key}"

        fetch_weather_data_task = FetchNonPagedDataOperator(
            task_id=f"fetch_weather_data_task_{i + 1}",
            url=url,
            file_topic=f"weekly_weather_data_{now_string}",
            content_type="plain/text",
        )
        weather_fetch_task_list.append(fetch_weather_data_task)
        now -= one_day

    weather_fetch_task_list
