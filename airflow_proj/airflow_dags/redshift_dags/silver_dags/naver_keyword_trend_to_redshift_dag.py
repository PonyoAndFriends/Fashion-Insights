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
    table = "naver_shopping_kwd_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXISTS {DEFAULT_SILVER_SHCEMA}.{table};
    """
    create_sql = f"""
    CREATE TABLE {DEFAULT_SILVER_SHCEMA}.{table} (
        trend_id INT PRIMARY KEY,
        start_date DATE,
        end_date DATE,
        time_unit VARCHAR(10),
        category_name VARCHAR(100),
        category_code VARCHAR(50),
        keyword_name VARCHAR(100),
        gender VARCHAR(8),
        period DATE,
        ratio FLOAT,
        created_at DATE
    );
    """
    refresh_task = RefreshTableOperator(
        task_id="refresh_keywords_table_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    copy_tasks = []
    gender_dict = {
        "남성": "MEN",
        "여성": "WOMAN"
    }
    for gender in ["남성", "여성"]:
        copy_query = f"""
        COPY {DEFAULT_SILVER_SHCEMA}.{table}
        FROM '{silver_bucket_url}/{now_string}/otherapis/{gender}_keyword_trend_raw_data/'
        IAM_ROLE '{redshift_iam_role}'
        FORMAT AS PARQUET;
        """

        copy_task = RedshiftQueryOperator(
            task_id=f"{gender_dict[gender]}_copy_naver_keyword_trend_task",
            sql=copy_query,
        )
        copy_tasks.append(copy_task)

    refresh_task >> copy_tasks
