from airflow import DAG
from datetime import datetime, timedelta
from operators.s3_to_redshift_operator import S3ToRedshiftCustomOperator

# DAG 기본 설정
default_args = {
    "owner": "team3-2@retaeil.com",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

# 컬럼 정보 (테이블 스키마)
table_schema = {
    "id": "VARCHAR(50)",
    "brand_name": "VARCHAR(255)",
    "product_name": "VARCHAR(255)",
    "discount_ratio": "FLOAT",
    "final_price": "FLOAT",
    "ranking": "INT",
    "category_code": "VARCHAR(50)",
    "gender": "VARCHAR(10)",
    "age_band": "VARCHAR(50)",
    "platform": "VARCHAR(50)",
    "date": "DATE",
}

# S3 경로 및 테이블 정보
schema = "SilverLayer"
table = "Musinsa_Ranking_silver"
s3_bucket = "destination-bucket-hs"
s3_key = "2025-01-02/Musinsa/RankingData/"  # 폴더로 지정 시 해당 폴더 내 모든 파일 COPY

with DAG(
    dag_id="custom_example_s3_to_redshift_dag",
    default_args=default_args,
    description="Load S3 data into Redshift with dynamic table creation",
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["S3", "Redshift", "CustomOperator"],
) as dag:
    load_to_redshift = S3ToRedshiftCustomOperator(
        task_id="load_s3_to_redshift",
        schema=schema,
        table=table,
        columns=table_schema,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        copy_options=["FORMAT AS PARQUET"],  # 추가 COPY 옵션 가능
        aws_conn_id="aws_conn",
        redshift_conn_id="redshift_default",
    )

    load_to_redshift
