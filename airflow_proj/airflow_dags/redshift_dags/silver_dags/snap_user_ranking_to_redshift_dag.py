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
    dag_id="snap_user_ranking_silver_table_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    now_string = NOW_STRING
    silver_bucket_url = DEFULAT_SILVER_BUCKET_URL
    genders = ["여성", "남성"]
    table = "musinsa_snap_user_ranking_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXISTS {DEFAULT_SILVER_SHCEMA}.{table};
    """
    create_sql = f"""
    CREATE TABLE {DEFAULT_SILVER_SHCEMA}.{table} (
        story_id INT PRIMARY KEY,
        aggregation_like_count INT,
        tags VARCHAR(256),
        created_at DATE NOT NULL,
        gender VARCHAR(8)
    );
    """
    refresh_task = RefreshTableOperator(
        task_id="snap_user_ranking_refresh_table_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    MEN_copy_query = f"""
    COPY {DEFAULT_SILVER_SHCEMA}.{table}
    FROM '{silver_bucket_url}/{now_string}/otherapis/musinsa_WOMEN_ranking_story_group_raw_data/'
    IAM_ROLE '{redshift_iam_role}'
    FORMAT AS PARQUET;
    """

    copy_task_MEN = RedshiftQueryOperator(
        task_id="snap_user_MEN_story_ranking_copy_task",
        sql=MEN_copy_query,
    )

    WOMEN_copy_query = f"""
    COPY {DEFAULT_SILVER_SHCEMA}.{table}
    FROM '{silver_bucket_url}/{now_string}/otherapis/musinsa_MEN_ranking_story_group_raw_data/'
    IAM_ROLE '{redshift_iam_role}'
    FORMAT AS PARQUET;
    """

    copy_task_WOMEN = RedshiftQueryOperator(
        task_id="snap_user_WOMEN_story_ranking_copy_task",
        sql=WOMEN_copy_query,
    )

    refresh_task >> copy_task_MEN >> copy_task_WOMEN
