from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from otherapis.custom_operators.fetch_non_paged_data_operator import (
    FetchNonPagedDataOperator,
)
from otherapis.custom_operators.custom_modules.otherapis_dependencies import (
    OTHERAPI_DEFAULT_ARGS,
)
from datetime import datetime, timedelta
from otherapis.custom_operators.k8s_spark_job_submit_operator import (
    submit_spark_application,
)
from otherapis.custom_operators.custom_modules.s3_upload import (
    make_s3_url,
)

default_args = OTHERAPI_DEFAULT_ARGS

FILE_TOPIC = "weather_station_data"
now = datetime.now() + timedelta(hours=9)
now_string = now.strftime("%Y-%m-%d")
BRONZE_FILE_PATH = f"bronze/{now_string}/otherapis/{FILE_TOPIC}_raw_data/"
SILVER_FILE_PATH = f"silver/{now_string}/otherapis/{FILE_TOPIC}_raw_data/"

# DAG 정의 - 기상 관측소 메타 데이터는 오랜 기간 변경이 없을 것이므로 수동으로 트리거
with DAG(
    dag_id="fetch_weather_meta_data_dag",
    default_args=default_args,
    description="Fetch korean weather station metadata from open api and load to s3 bucket",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1) + timedelta(hours=9),
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
        file_topic=FILE_TOPIC,
        content_type="plain/text",
    )

    spark_args = [
        make_s3_url(Variable.get("s3_bucket"), BRONZE_FILE_PATH),
        make_s3_url(Variable.get("s3_bucket"), SILVER_FILE_PATH),
    ]

    spark_job_submit_task = PythonOperator(
        task_id="weekly-weather-submit-spark-job-task",
        python_callable=submit_spark_application,
        op_args=[
            "weekly-weather-data-from-bronze-to-silver-task",
            r"otherapis/bronze_to_silver/weekly_weather_meta_data_to_silver.py",
            spark_args,
        ],
    )

    fetch_weather_station_data_task >> spark_job_submit_task
