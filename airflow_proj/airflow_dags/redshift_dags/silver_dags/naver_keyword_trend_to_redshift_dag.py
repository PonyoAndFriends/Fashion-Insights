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
    dag_id="naver_keyword_trend_silver_data_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    now_string = NOW_STRING
    silver_bucket_url = DEFULAT_SILVER_BUCKET_URL
    table = "naver_naver_shopping_kwd_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXIST {DEFAULT_SILVER_SHCEMA}.{table};
    """
    create_sql = f"""
    CREATE TABLE {DEFAULT_SILVER_SHCEMA}.{table} (
        trend_id INT IDENTITY(1,1) PRIMARY KEY,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        time_unit VARCHAR(10) NOT NULL,
        category_name VARCHAR(100) NOT NULL,
        category_code VARCHAR(20) NOT NULL,
        keyword_name VARCHAR(100) NOT NULL,
        gender VARCHAR(5),
        period DATE NOT NULL,
        ratio FLOAT NOT NULL,
        created_at TIMESTAMP NOT NULL
    );
    """
    refresh_task = RefreshTableOperator(
        drop_sql,
        create_sql,
        task_id="naver_keyword_trend_table_refresh_task",
    )

    copy_tasks = []
    for gender in ["남성", "여성"]:
        copy_query = f"""
        COPY INTO {DEFAULT_SILVER_SHCEMA}.{table}
        FROM '{silver_bucket_url}/{now_string}/otherapis/{gender}_keyword_trend_raw_data/'
        IAM_ROLE {redshift_iam_role}
        FORMAT AS PARQUET;
        """

        copy_task = RedshiftQueryOperator(
            task_id="copy_naver_keyword_trend_task",
            op_args=copy_query,
        )
        copy_tasks.append(copy_task)

    refresh_task >> copy_tasks
