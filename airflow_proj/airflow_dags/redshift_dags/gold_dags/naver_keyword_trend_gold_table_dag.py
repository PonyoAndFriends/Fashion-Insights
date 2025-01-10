from airflow import DAG
from airflow.models import Variable
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
    dag_id="naver_keyword_trend_gold_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    table = "naver_shopping_kwd_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXIST {DEFAULT_GOLD_SHCEMA}.{table};
    """

    create_sql = f"""
    CREATE TABLE {DEFAULT_GOLD_SHCEMA}.{table} AS
    SELECT
        keyword_name,
        ratio,
        TO_CHAR(period, 'MM-DD') AS period,
        created_at
    FROM
        {DEFAULT_SILVER_SHCEMA}.{table};
    """

    full_refresh_task = RefreshTableOperator(
        task_id="naver_keyword_trend_gold_table_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    full_refresh_task
