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

mapping_list = [
    "기타",
    "니트",
    "셔츠",
    "스커트",
    "원피스",
    "재킷",
    "전체",
    "코트",
    "티셔츠",
    "패딩",
    "팬츠",
    "폴리스",
]

default_args = SILVER_LOAD_DEFAULT_ARGS

with DAG(
    dag_id="musinsa_review_detail_silver_data_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
) as dag:

    # 기본적인 설정 정의
    now_string = NOW_STRING
    silver_bucket_url = DEFULAT_SILVER_BUCKET_URL
    platforms = ["29cm", "musinsa"]
    redshift_iam_role = Variable.get("redshift_iam_role")
    platform = "musinsa"

    table = f"{platform}_product_review_detail_tb" if platform != "29cm" else "cm29"

    drop_sql = f"""
    DROP TABLE IF EXISTS {DEFAULT_SILVER_SHCEMA}.{table};
    """

    create_sql = f"""
    CREATE TABLE {DEFAULT_SILVER_SHCEMA}.{table} (
        product_id INT NOT NULL,
        review_content VARCHAR(8000),
        review_rating INT,
        review_date DATE,
        reviewer_height FLOAT,
        reviewer_weight FLOAT,
        selected_options VARCHAR(200),
        created_at DATE NOT NULL
    );
    """

    refresh_task = RefreshTableOperator(
        task_id=f"{platform}_review_detail_refresh_table_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    for task_number, depth3code in enumerate(mapping_list):
        copy_query = f"""
        COPY {DEFAULT_SILVER_SHCEMA}.{table}
        FROM '{silver_bucket_url}/{now_string}/{platform}/{platform}_product_review_detail_tb/{depth3code}/'
        IAM_ROLE '{redshift_iam_role}'
        FORMAT AS PARQUET;
        """

        copy_task = RedshiftQueryOperator(
            task_id=f"{platform}_{task_number}_review_detail_copy_task",
            sql=copy_query,
        )

        refresh_task >> copy_task
