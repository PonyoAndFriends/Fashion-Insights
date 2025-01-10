from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow_proj.airflow_dags.redshift_dags.custom_sql_operators.custom_query_operator import (
    RedshiftQueryOperator,
)
from airflow_proj.airflow_dags.redshift_dags.custom_sql_operators.custom_refresh_table_operator import (
    RefreshTableOperator,
)
from airflow_proj.airflow_dags.redshift_dags.custom_sql_modules.query_dag_dependencies import (
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
    dag_id="review_detail_silver_data_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    now_string = NOW_STRING
    silver_bucket_url = DEFULAT_SILVER_BUCKET_URL
    platforms = PLATFORMS
    redshift_iam_role = Variable.get("redshift_iam_role")

    for platform in platforms:
        table = f"{platform}_product_review_detail_tb"
        drop_sql = f"""
        DROP TABLE IF EXIST {DEFAULT_SILVER_SHCEMA}.{table};
        """

        create_sql = f"""
        CREATE TABLE {DEFAULT_SILVER_SHCEMA}.{table} (
            platform VARCHAR(100) NOT NULL,
            master_category_name VARCHAR(12),
            small_category_name VARCHAR(30),
            product_id INT NOT NULL,
            img_url VARCHAR(1000),
            product_name VARCHAR(100) NOT NULL,
            brand_name_kr VARCHAR(100) NOT NULL,
            brand_name_en VARCHAR(100),
            original_price INT,
            final_price INT,
            discount_ratio INT,
            review_counting INT,
            review_avg_rating FLOAT,
            like_counting INT,
            selected_options VARCHAR(100),
            created_at TIMESTAMP NOT NULL
        );
        """
        refresh_task = RefreshTableOperator(
            drop_sql,
            create_sql,
            task_id=f"{platform}_review_detail_table_refresh_task",
        )

        copy_query = f"""
            COPY INTO {DEFAULT_SILVER_SHCEMA}.{table}
            FROM '{silver_bucket_url}/{now_string}/{platform}/product_review_data/'
            IAM_ROLE {redshift_iam_role}
            FORMAT AS PARQUET;
            """

        copy_task = RedshiftQueryOperator(
            task_id=f"{platform}_review_detail_copy_task",
            op_args=[copy_query],
        )

        refresh_task >> copy_task
