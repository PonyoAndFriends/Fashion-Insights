from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from redshift_dags.custom_sql_operators.custom_query_operator import (
    RedshiftQueryOperator,
)
from redshift_dags.custom_sql_operators.custom_refresh_table_operator import (
    RefreshTableOperator,
)
from redshift_dags.custom_sql_modules.query_dag_dependencies import (
    SILVER_LOAD_DEFAULT_ARGS,
    DEFAULT_SILVER_SHCEMA,
    NOW_STRING,
    DEFULAT_SILVER_BUCKET_URL,
)

# DAG 기본 설정
default_args = SILVER_LOAD_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="weather_station_silver_data_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    now_string = NOW_STRING
    silver_bucket_url = DEFULAT_SILVER_BUCKET_URL
    table = "weather_center_location_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXISTS {DEFAULT_SILVER_SHCEMA}.{table};
    """
    create_sql = f"""
    CREATE TABLE {DEFAULT_SILVER_SHCEMA}.{table} (
        STN_ID INT PRIMARY KEY, -- 관측소 구분자
        STN_KO VARCHAR(100) NOT NULL -- 지역명
    );
    """
    refresh_task = RefreshTableOperator(
        task_id="weather_station_refresh_table_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    copy_query = f"""
    COPY {DEFAULT_SILVER_SHCEMA}.{table}
    FROM '{silver_bucket_url}/{now_string}/otherapis/weather_station_data_raw_data/'
    IAM_ROLE '{redshift_iam_role}'
    FORMAT AS PARQUET;
    """

    copy_task = RedshiftQueryOperator(
        task_id="copy_weather_station_data_task",
        sql=copy_query,
    )

    refresh_task >> copy_task
