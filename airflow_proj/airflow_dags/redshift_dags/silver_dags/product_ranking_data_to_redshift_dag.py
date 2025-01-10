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
    PLATFORMS,
)

# DAG 기본 설정
default_args = SILVER_LOAD_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="product_ranking_silver_data_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    now_string = NOW_STRING
    silver_bucket_url = DEFULAT_SILVER_BUCKET_URL
    platforms = PLATFORMS
    table = "ranking_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXIST {DEFAULT_SILVER_SHCEMA}.{table};
    """
    create_sql = f"""
    CREATE TABLE {DEFAULT_SILVER_SHCEMA}.{table} (
        platform VARCHAR(100) NOT NULL,
        master_category_name VARCHAR(12) NOT NULL,
        product_id INT NOT NULL,
        ranking INT NOT NULL,
        created_at DATETIME NOT NULL
    );
    """
    refresh_task = RefreshTableOperator(
        drop_sql,
        create_sql,
        task_id="product_ranking_table_refresh_task",
    )

    copy_queries = [
        f"""
        COPY INTO {DEFAULT_SILVER_SHCEMA}.{table}
        FROM '{silver_bucket_url}/{now_string}/{platform}/ranking_data'
        IAM_ROLE {redshift_iam_role}
        FORMAT AS PARQUET;
        """
        for platform in platforms
    ]

    copy_tasks = []

    for i, query in enumerate(copy_queries):
        copy_task = RedshiftQueryOperator(
            task_id=f"{platforms[i]}_product_ranking_copy_task",
            sql=query,
        )
        copy_tasks.append(copy_task)

    # 태스크 실행 순서 - COPY의 특성 상 순서대로 실행
    previous_task = refresh_task
    for copy_task in copy_tasks:
        previous_task >> copy_task
        previous_task = copy_task
