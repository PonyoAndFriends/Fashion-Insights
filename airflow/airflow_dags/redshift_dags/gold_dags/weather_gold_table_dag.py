from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from custom_sql_operators.custom_query_operator import RedshiftQueryOperator
from custom_sql_operators.custom_refresh_table_operator import RefreshTableOperator
from custom_sql_modules.query_dag_dependencies import (
    SILVER_LOAD_DEFAULT_ARGS,
    DEFAULT_SILVER_SHCEMA,
    DEFAULT_GOLD_SHCEMA,
    NOW_STRING,
    DEFULAT_SILVER_BUCKET_URL,
)

# DAG 기본 설정
default_args = SILVER_LOAD_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="weather_gold_table_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    table = "weather_gold_tb"

    drop_sql = f"""
    DROP TABLE IF EXIST {DEFAULT_GOLD_SHCEMA}.{table};
    """

    create_sql = f"""
    CREATE TABLE weather_summary AS
    SELECT
        wo.STN,
        wo.TM,
        wo.TA_AVG,
        wo.TA_MAX,
        wo.TA_MIN,
        wo.RN_DAY
    FROM
        {DEFAULT_SILVER_SHCEMA}.weather_daily_tb wd
    JOIN
        {DEFAULT_SILVER_SHCEMA}.weather_center_location_tb st 
    ON wo.STN = st.STN_ID;
    """

    full_refresh_task = RefreshTableOperator(
        drop_sql,
        create_sql,
        task_id="weather_gold_table_task",
    )

    full_refresh_task
