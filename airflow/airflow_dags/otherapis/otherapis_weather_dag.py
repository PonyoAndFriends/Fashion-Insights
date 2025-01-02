from airflow import DAG
from airflow.models import Variable
from custom_operators.fetch_non_paged_data_operator import FetchNonPagedDataOperator
from datetime import datetime, timedelta

# 이후 시연 때 email 설정을 True로 변경
default_args = {
    'owner': 'gjstjd9509@gmail.com',
    'start_date': datetime(2023, 1, 1),
    'email': ['gjstjd9509@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

# DAG 정의
with DAG(
    dag_id='fetch_weekly_weather_data_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # API Key 설정
    api_key = Variable.get("weather_api_key")
    url = r"https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd.php"
    
    now = datetime.now()
    one_day = timedelta(weeks=1)

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
