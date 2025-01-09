from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from gold_dags.custom_sql_operators.custom_refresh_table_operator import RefreshTableOperator
from gold_dags.custom_sql_operators.custom_sql_modules.query_dag_dependencies import (
    SILVER_LOAD_DEFAULT_ARGS,
    DEFAULT_SILVER_SHCEMA,
    DEFAULT_GOLD_SHCEMA,
)

# DAG 기본 설정
default_args = SILVER_LOAD_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="snap_brand_ranking_gold_table_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    table = "musinsa_snap_brand_ranking_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXIST {DEFAULT_GOLD_SHCEMA}.{table};
    """
    create_sql = f"""
    CREATE TABLE {DEFAULT_GOLD_SHCEMA}.{table} AS
    SELECT
        brand_name,
        rank AS curr_rank,
        (rank - previous_rank) AS prev_diff_rank,
        follower_count,
        CASE
            WHEN JSON_LENGTH(labels) >= 2 THEN
                JSON_EXTRACT_ARRAY_ELEMENT_TEXT(labels, 0) || ', ' ||
                JSON_EXTRACT_ARRAY_ELEMENT_TEXT(labels, 1)
            WHEN JSON_LENGTH(labels) = 1 THEN
                JSON_EXTRACT_ARRAY_ELEMENT_TEXT(labels, 0)
            ELSE
                NULL
        END AS labels,
        created_at,
        img_url
    FROM
        {DEFAULT_SILVER_SHCEMA}.{table};
    """

    full_refresh_task = RefreshTableOperator(
        drop_sql,
        create_sql,
        task_id="snap_brand_ranking_gold_task",
    )

    full_refresh_task
