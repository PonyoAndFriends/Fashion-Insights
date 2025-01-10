from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow_proj.airflow_dags.redshift_dags.custom_sql_operators.custom_query_operator import RedshiftQueryOperator
from airflow_proj.airflow_dags.redshift_dags.custom_sql_operators.custom_refresh_table_operator import RefreshTableOperator
from airflow_proj.airflow_dags.redshift_dags.custom_sql_modules.query_dag_dependencies import (
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
    table = "youtube_video_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXIST {DEFAULT_SILVER_SHCEMA}.{table};
    """
    create_sql = """
    CREATE TABLE youtube_videos (
        video_id VARCHAR(20) PRIMARY KEY,
        gender VARCHAR(2),
        category_name VARCHAR(100) NOT NULL,
        channel_title VARCHAR(200) NOT NULL,
        title VARCHAR(200) NOT NULL,
        img_url VARCHAR(1000) NOT NULL,
        duration_seconds INT NOT NULL,
        published_at TIMESTAMP NOT NULL,
        view_count INT NOT NULL,
        like_count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    refresh_task = RefreshTableOperator(drop_sql, create_sql)

    copy_queries = [
        f"""
        COPY INTO {DEFAULT_SILVER_SHCEMA}.{table}
        FROM '{silver_bucket_url}/{now_string}/otherapis/{gender}_youtoube_videos_by_categories_raw_data/'
        IAM_ROLE {redshift_iam_role}
        FORMAT AS PARQUET;
        """
        for gender in ["남성", "여성"]
    ]

    copy_tasks = []
    for copy_query in copy_queries:
        copy_task = RedshiftQueryOperator(
            task_id="youtube_data_copy_task",
            op_args=[copy_query],
        )
        copy_tasks.append(copy_task)

    refresh_task >> copy_tasks
