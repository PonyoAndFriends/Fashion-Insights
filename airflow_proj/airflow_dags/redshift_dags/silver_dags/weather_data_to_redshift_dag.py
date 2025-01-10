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
    dag_id="weekly_weather_data_silver_data_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    now_string = NOW_STRING
    silver_bucket_url = DEFULAT_SILVER_BUCKET_URL
    table = "weather_daily_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXIST {DEFAULT_SILVER_SHCEMA}.{table};
    """
    create_sql = f"""
    CREATE TABLE {DEFAULT_SILVER_SHCEMA}.{table} (
        STN INT NOT NULL,
        TM DATE NOT NULL,
        WS_AVG FLOAT,
        WS_MAX FLOAT,
        TA_AVG FLOAT,
        TA_MAX FLOAT,
        TA_MIN FLOAT,
        HM_AVG FLOAT,
        HM_MIN FLOAT,
        FG_DUR FLOAT,
        CA_TOT FLOAT,
        RN_DAY FLOAT,
        RN_DUR FLOAT,
        RN_60M_MAX FLOAT,
        RN_POW_MAX FLOAT,
        PRIMARY KEY (STN, TM)
    );
    """
    refresh_task = RefreshTableOperator(
        task_id="refresh_table_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    copy_query = f"""
    COPY INTO {DEFAULT_SILVER_SHCEMA}.{table}
    FROM '{silver_bucket_url}/{now_string}/otherapis/weekly_weather_data_raw_data/'
    IAM_ROLE {redshift_iam_role}
    FORMAT AS PARQUET;
    """

    copy_task = RedshiftQueryOperator(
        task_id="weekly_weather_data_copy_task",
        sql=copy_query,
    )

    refresh_task >> copy_task
