from airflow import DAG
from datetime import datetime
from redshift_dags.custom_sql_operators.custom_refresh_table_operator import (
    RefreshTableOperator,
)
from redshift_dags.custom_sql_modules.query_dag_dependencies import (
    SILVER_LOAD_DEFAULT_ARGS,
    DEFAULT_SILVER_SHCEMA,
    DEFAULT_GOLD_SHCEMA,
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
    DROP TABLE IF EXISTS {DEFAULT_GOLD_SHCEMA}.{table};
    """

    create_sql = f"""
    CREATE TABLE {DEFAULT_GOLD_SHCEMA}.{table} AS
    SELECT
        st.stn_ko AS 지역명,  -- weather_center_location_tb의 지역명
        wd.TM AS 측정시간,   -- 측정 시간
        wd.TA_AVG AS 평균온도, -- 평균 온도
        wd.TA_MAX AS 최고온도, -- 최고 온도
        wd.TA_MIN AS 최저온도, -- 최저 온도
        wd.RN_DAY AS 일강수량  -- 하루 강수량
    FROM
        {DEFAULT_SILVER_SHCEMA}.weather_daily_tb wd
    JOIN
        {DEFAULT_SILVER_SHCEMA}.weather_center_location_tb st
    ON wd.STN = st.stn_id;
    """

    full_refresh_task = RefreshTableOperator(
        task_id="weather_gold_table_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    full_refresh_task
